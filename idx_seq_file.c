#include <idx_seq_file.h>
#include <index.h>
#include <errno.h>
#include <assert.h>
#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

static size_t get_file_size(const char *filename)
{
    assert(filename != NULL);

    FILE *fp = fopen(filename, "rb");
    if (fp == NULL) {
        fprintf(stderr, "Couldn't open the file: %s\n", filename);
        return 0;
    }

    fseek(fp, 0, SEEK_END);
    size_t size = ftell(fp);
    fseek(fp, 0, SEEK_SET);
    fclose(fp);

    return size;
}

static bool is_file_empty(const char *filename)
{
    assert(filename != NULL);

    size_t size = get_file_size(filename);
    return (size == 0);
}

static int add_first_record(struct idx_seq_file *file, struct record *r)
{
    assert(file != NULL);
    assert(file->data_file_path != NULL);
    assert(r != NULL);
    assert(r->key >= 1);

    // allocate whole page
    struct record page[RECORDS_PER_PAGE];
    memset(page, 0x0, PAGESIZE);

    r->overflow_pointer = OVERFLOW_PTR_NULL;
    memcpy(&page[0], r, RECORD_SIZE);

    struct index_entry idx = {
        .key = r->key,
        .page_number = 1
    };

    FILE *index_file = fopen(file->index_file_path, "w+b");
    if (index_file == NULL) {
        fprintf(stderr, "Couldn't open the file: %s\n", file->index_file_path);
        return -1;
    }

    FILE *data_file = fopen(file->data_file_path, "w+b");
    if (data_file == NULL) {
        fprintf(stderr, "Couldn't open the file: %s\n", file->data_file_path);
        fclose(index_file);
        return -1;
    }

    // set position at 0 in both files just to be sure
    fseek(index_file, 0, SEEK_SET);
    fseek(data_file, 0, SEEK_SET);

    size_t written = fwrite(&idx, sizeof(struct index_entry), 1, index_file);
    assert(written == 1);

    written = fwrite(page, RECORD_SIZE, RECORDS_PER_PAGE, data_file);
    assert(written == RECORDS_PER_PAGE);

    fclose(data_file);
    fclose(index_file);

    file->primary_area_size += PAGESIZE;

    return 0;
}

static uint16_t get_page_number_from_index_file(struct idx_seq_file *file, int32_t key)
{
    assert(file != NULL);
    assert(file->index_file_path != NULL);

    // allocate buffer for all entries in the index file
    size_t index_file_size = get_file_size(file->index_file_path);

    assert(index_file_size > 0);
    assert(index_file_size % sizeof(struct index_entry) == 0);

    size_t buff_len = index_file_size / sizeof(struct index_entry);
    struct index_entry *buff = calloc(buff_len, sizeof(struct index_entry));
    if (buff == NULL) {
        return 0;
    }

    FILE *fp = fopen(file->data_file_path, "rb");
    if (fp == NULL) {
        fprintf(stderr, "Couldn't open file: %s\n", file->index_file_path);
        free(buff);
        return 0;
    }

    // set position at 0
    fseek(fp, 0, SEEK_SET);

    // read whole file, 4 entries at a time
    for (size_t i = 0; i < buff_len;) {
        i += fread(buff, sizeof(struct index_entry), 4, fp);
    }

    fseek(fp, 0, SEEK_SET);
    fclose(fp);

    uint16_t page = 0;
    for (size_t i = 0; i < buff_len; i++) {
        if (buff[i].key > key) {
            page = buff[i].page_number - 1;
            break;
        }
    }

    // TODO:
    // if the page number hasn't been found yet, we gotta return
    // page number of the last entry in the index file
    if (page == 0) {
        page = buff[buff_len - 1].page_number;
    }

    printf("Found page number: %hu, for key: %d\n", page, key);
    free(buff);
    return page;
}

static int read_page_from_data_file(struct idx_seq_file *file, uint16_t page_number, struct record *buffer)
{
    assert(file != NULL);
    assert(file->data_file_path != NULL);
    assert(page_number > 0); // page number isn't zero-based
    assert(buffer != NULL);

    FILE *fp = fopen(file->data_file_path, "rb");
    if (fp == NULL) {
        fprintf(stderr, "Couldn't open file: %s\n", file->index_file_path);
        return -1;
    }

    // set position at 0 just to be sure
    fseek(fp, 0, SEEK_SET);

    size_t offset = (page_number - 1) * PAGESIZE;
    assert(offset < file->primary_area_size);

    fseek(fp, offset, SEEK_SET);

    int records_read = fread(buffer, RECORD_SIZE, RECORDS_PER_PAGE, fp);
    // there's always whole page allocated so this should never happen
    assert(records_read == RECORDS_PER_PAGE);

    fseek(fp, 0, SEEK_SET);
    fclose(fp);

    return 0;
}


int get_record(struct idx_seq_file *file, int32_t key, struct record *r)
{
    if (file == NULL) {
        fprintf(stderr, "file is NULL\n");
        return -EINVAL;
    }

    if (r == NULL) {
        fprintf(stderr, "record is NULL\n");
        return -EINVAL;
    }

    uint16_t page_number = get_page_number_from_index_file(file, key);
    struct record *records_buff = calloc(RECORDS_PER_PAGE, RECORD_SIZE);
    if (records_buff == NULL) {
        fprintf(stderr, "Couldn't allocate buffer\n");
        return -ENOMEM;
    }

    int records_read = read_page_from_data_file(file, page_number, records_buff);
    bool found = false;

    for (size_t i = 0; i < records_read; i++) {
        if (records_buff[i].key == key) {
            found = true;
            memcpy(r, &records_buff[i], RECORD_SIZE);
        } else if (records_buff[i].overflow_pointer != OVERFLOW_PTR_NULL) {
            uint32_t ovf_ptr = records_buff[i].overflow_pointer;
            struct record tmp = {};
            while (ovf_ptr != OVERFLOW_PTR_NULL) {
                if (get_record_from_overflow_area(file, ovf_ptr, &tmp) <= 0) {
                    break;
                } else if (tmp.key == key) {
                    found = true;
                    memcpy(r, &tmp, RECORD_SIZE);
                    break;
                } else {
                    ovf_ptr = tmp.overflow_pointer;
                }
            }
        }

        if (found) {
            break;
        }
    }

    free(records_buff);

    int rc = (found) ? 0 : -1;
    return rc;
}

static bool is_page_free(struct record *page_buffer)
{
    assert(page_buffer != NULL);

    // if a key is greater than or equal to 1, then the entry associated with it is valid 
    for (size_t i = 0; i < RECORDS_PER_PAGE; i++) {
        if (page_buffer[i].key < 1) {
            return true;
        }
    }

    return false;
}

static int add_record_to_primary_area(struct idx_seq_file *file, uint16_t page_number,
                                    struct record *page_buffer, struct record *rec)
{
    assert(file != NULL);
    assert(page_buffer != NULL);
    assert(rec != NULL);
    assert(page_number > 0);

    // assumption is that the page has extra space - no overflow pointers have to be updated
    size_t idx = 0;
    while (page_buffer[idx].key < rec->key) {
        if (idx == RECORDS_PER_PAGE - 1) {
            break;
        } else {
            idx++;
        }
    }

    rec->overflow_pointer = OVERFLOW_PTR_NULL;

    if (idx == RECORDS_PER_PAGE - 1) {
        memcpy(&page_buffer[idx], rec, RECORD_SIZE);
    } else {
        memmove(&page_buffer[idx+1], &page_buffer[idx], RECORD_SIZE);
        memcpy(&page_buffer[idx], rec, RECORD_SIZE);
    }

    FILE *fp = fopen(file->data_file_path, "wb");
    if (fp == NULL) {
        fprintf(stderr, "Couldn't open file: %s\n", file->index_file_path);
        return -1;
    }

    // set position at 0 just to be sure
    fseek(fp, 0, SEEK_SET);

    size_t offset = (page_number - 1) * PAGESIZE;
    assert(offset < file->primary_area_size);

    fseek(fp, offset, SEEK_SET);
    int rc = 0;
    size_t record_written = fwrite(page_buffer, RECORDS_PER_PAGE, RECORD_SIZE, fp);
    if (record_written != RECORDS_PER_PAGE) {
        fprintf(stderr, "Couldn't write all records (written:%lu, expected:%u\n", record_written, RECORDS_PER_PAGE);
        rc = -1;
    }

    fseek(fp, 0, SEEK_SET);
    fclose(fp);

    return rc;
}

// TODO:
static int add_record_to_overflow_area(struct idx_seq_file *file, uint16_t page_number,
                                    struct record *page_buffer, struct record *rec)
{
    assert(file != NULL);
    assert(page_buffer != NULL);
    assert(rec != NULL);
    assert(page_number > 0);

    return 0;

}

int add_record(struct idx_seq_file *file, struct record *r)
{
    if (file == NULL) {
        fprintf(stderr, "file is NULL\n");
        return -EINVAL;
    }

    if (r == NULL) {
        fprintf(stderr, "record is NULL\n");
        return -EINVAL;
    }

    int rc = 0;
    if (is_file_empty(file->index_file_path)) {
        rc = add_first_record(file, r);
        return rc;
    }

    uint16_t page_number = get_page_number_from_index_file(file, r->key);
    if (page_number == 0) {
        fprintf(stderr, "Getting page number from the index file failed\n");
        return -1;
    }

    struct record *page_buffer = calloc(RECORDS_PER_PAGE, RECORD_SIZE);
    if (page_buffer == NULL) {
        fprintf(stderr, "Couldn't allocate buffer\n");
        return -ENOMEM;
    }

    rc = read_page_from_data_file(file, page_number, page_buffer);
    if (rc != 0) {
        fprintf(stderr, "Couldn't read page: %hu\n", page_number);
        free(page_buffer);
        return -1;
    }

    if (is_page_free(page_buffer)) {
        rc = add_record_to_primary_area(file, page_number, page_buffer, r);
    } else {
        rc = add_record_to_overflow_area(file, page_number, page_buffer, r);
    }

    return rc;
/*
    struct record *records_buff = calloc(RECORDS_PER_PAGE, PAGESIZE);
    if (records_buff == NULL) {
        fprintf(stderr, "Couldn't allocate buffer\n");
        return -ENOMEM;
    }

    read_data_file_page_primary_area(file, page_number, records_buff); */

    // check if 
}

int idx_seq_file_init(struct idx_seq_file *file, const char *index_file, const char *data_file)
{
    if (file == NULL) {
        fprintf(stderr, "file is NULL\n");
        return -EINVAL;
        
    }

    if (index_file == NULL) {
        fprintf(stderr, "index_file is NULL\n");
        return -EINVAL;
        
    }

    if (data_file == NULL) {
        fprintf(stderr, "data_file is NULL\n");
        return -EINVAL;
        
    }

    file->index_file_path = index_file;
    file->data_file_path = data_file;
    file->overflow_area_size = 0;
    file->primary_area_size = 0;
    return 0;
}