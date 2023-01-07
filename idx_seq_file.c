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

static uint16_t get_page_number_from_index_file(struct idx_seq_file *file, int32_t key)
{
    assert(file != NULL);
    assert(key > 1);

    size_t index_file_size = get_file_size(file->index_file_path);
    assert(index_file_size % sizeof(struct index_entry) == 0);

    size_t number_of_entries = index_file_size / sizeof(struct index_entry);

    struct index_entry *buffer = calloc(number_of_entries, sizeof(struct index_entry));
    if (buffer == NULL) {
        return 0;
    }

    FILE *fp = fopen(file->index_file_path, "rb");
    if (fp == NULL) {
        fprintf(stderr, "Couldn't open file: %s\n", file->index_file_path);
        free(buffer);
        return 0;
    }

    // set position at 0
    fseek(fp, 0, SEEK_SET);
    fread(buffer, sizeof(struct index_entry), number_of_entries, fp);

    fclose(fp);

    uint16_t page_no = 0;

    // find first key greater than ours and return previous page number
    for (size_t i = 0; i < number_of_entries; i++) {
        if (buffer[i].key > key) {
            page_no = buffer[i-1].page_number;
        }
    }

    // if page number still hasn't been find return page number of the last entry
    if (page_no == 0) {
        page_no = buffer[number_of_entries-1].page_number;
    }

    free(buffer);
    return page_no;
}

static void read_page_from_data_file(struct idx_seq_file *file, struct record *page, uint16_t page_number)
{
    assert(file != NULL);
    assert(page != NULL);
    assert(page_number > 0);

    FILE *fp = fopen(file->data_file_path, "rb");

    size_t offset = (page_number - 1) * PAGESIZE;
    fseek(fp, offset, SEEK_SET);
    size_t read = fread(page, RECORD_SIZE, RECORDS_PER_PAGE, fp);
    assert(read == RECORDS_PER_PAGE);

    fflush(fp);
    fclose(fp);
}

static void save_page_to_data_file(struct idx_seq_file *file, struct record *page, uint16_t page_number)
{
    assert(file != NULL);
    assert(page != NULL);
    assert(page_number > 0);

    FILE *fp = fopen(file->data_file_path, "r+b");
    size_t offset = (page_number - 1) * PAGESIZE;
    fseek(fp, offset, SEEK_SET);
    assert(ftell(fp) < file->primary_area_size);
    size_t written = fwrite(page, RECORD_SIZE, RECORDS_PER_PAGE, fp);
    assert(written == RECORDS_PER_PAGE);

    fflush(fp);
    fclose(fp);
}

static bool is_page_free(struct record *page)
{
    assert(page != NULL);

    for (size_t i = 0; i < RECORDS_PER_PAGE; i++) {
        if (page[i].key == 0) {
            return true;
        }
    }

    return false;
}

static void read_record_overflow_area(struct idx_seq_file *file, uint32_t ovf_ptr, struct record *buff)
{
    assert(file != NULL);
    assert(buff != NULL);
    assert(ovf_ptr != OVERFLOW_PTR_NULL);

    FILE *fp = fopen(file->data_file_path, "rb");
    fseek(fp, ovf_ptr, SEEK_SET);

    size_t read = fread(buff, RECORD_SIZE, 1, fp);
    assert(read == 1);

    fflush(fp);
    fclose(fp);
}

static void save_record_overflow_area(struct idx_seq_file *file, uint32_t ovf_ptr, struct record *r)
{
    assert(file != NULL);
    assert(r != NULL);
    assert(ovf_ptr != OVERFLOW_PTR_NULL);
    assert(file->data_file_path != NULL);

    FILE *fp = fopen(file->data_file_path, "r+b");
    assert(fp != NULL);
    fseek(fp, ovf_ptr, SEEK_SET);

    size_t written = fwrite(r, RECORD_SIZE, 1, fp);
    assert(written == 1);

    fflush(fp);
    fclose(fp);
}

/**
 * Returns true if overflow pointer from data file has to be updated
*/
static bool add_record_overflow_area(struct idx_seq_file *file, struct record *r, uint32_t ovf_ptr)
{
    assert(file != NULL);
    assert(r != NULL);

    uint32_t ptr = file->primary_area_size + file->overflow_area_size;

    if (ovf_ptr == OVERFLOW_PTR_NULL) {
        r->overflow_pointer = OVERFLOW_PTR_NULL;
        save_record_overflow_area(file, ptr, r);
        file->overflow_area_size += RECORD_SIZE;
        
        return true;
    }

    uint32_t prev_ptr = OVERFLOW_PTR_NULL;
    uint32_t curr_ptr = ovf_ptr;
    struct record curr = {};
    read_record_overflow_area(file, ovf_ptr, &curr);

    if (curr.key == r->key) {
        fprintf(stderr, "Record with a key %d already exists. Aborting\n", r->key);
        return false;
    }

    bool ret;

    if (curr.key < r->key) {
        while (curr.overflow_pointer != OVERFLOW_PTR_NULL) {
            prev_ptr = curr_ptr;
            curr_ptr = curr.overflow_pointer;
            read_record_overflow_area(file, curr_ptr, &curr);

            if (curr.key == r->key) {
                fprintf(stderr, "Record with a key %d already exists. Aborting\n", r->key);
                return false;
            }

            if (curr.key > r->key) {
                break;
            }
        }
    }

    if (curr.key > r->key) { // znalazlem wiekszy
        if (prev_ptr == OVERFLOW_PTR_NULL) { // poprzedni w main
            r->overflow_pointer = curr_ptr;
            ret = true;
        }
        else { // poprzedni w overflow
            struct record prev = {};
            read_record_overflow_area(file, prev_ptr, &prev);

            r->overflow_pointer = prev.overflow_pointer;
            prev.overflow_pointer = ptr;
            save_record_overflow_area(file, prev_ptr, &prev);
            ret = false;
        }
    } else { // nie znalazlem wiekszego
        curr.overflow_pointer = ptr;
        save_record_overflow_area(file, curr_ptr, &curr);
        r->overflow_pointer = OVERFLOW_PTR_NULL;
        ret = false;
    }

    save_record_overflow_area(file, ptr, r);
    file->overflow_area_size += RECORD_SIZE;
    return ret;
}

int add_record(struct idx_seq_file *file, struct record *r)
{
    if (file == NULL) {
        fprintf(stderr, "file is NULL\n");
        return -EINVAL;
    }

    if (file->index_file_path == NULL) {
        fprintf(stderr, "index_file is NULL\n");
        return -EINVAL;
    }

    if (file->data_file_path == NULL) {
        fprintf(stderr, "data_file is NULL\n");
        return -EINVAL;
    }

    if (r == NULL) {
        fprintf(stderr, "record is NULL\n");
        return -EINVAL;
    }

    if (is_file_empty(file->data_file_path) || is_file_empty(file->index_file_path)) {
        return -EINVAL;
    }

    if (r->key <= 1) {
        fprintf(stderr, "Key has to be greater than 1\n");
        return -EINVAL;
    }

    uint16_t page_number = get_page_number_from_index_file(file, r->key);
    if (page_number == 0) {
        fprintf(stderr, "Failed to get page number for key: %d\n", r->key);
        return -1;
    }

    struct record page[RECORDS_PER_PAGE] = {};
    r->overflow_pointer = OVERFLOW_PTR_NULL;
    read_page_from_data_file(file, page, page_number);

    /* find position for the new record */
    size_t idx = 0;
    bool found = false;
    for (size_t i = 0; i < RECORDS_PER_PAGE; i++) {
        if (page[i].key == r->key) {
            fprintf(stderr, "Record with a key %d already exists. Aborting\n", r->key);
            return -1;
        }
        if (page[i].key > r->key) {
            idx = i;
            found = true;
            break;
        }
    }

    int last_rec_idx;
    for (last_rec_idx = RECORDS_PER_PAGE-1; last_rec_idx >= 0; last_rec_idx--) {
        if (page[last_rec_idx].key != 0) {
            break;
        }
    }

    if (is_page_free(page)) {
        if (found) { /* Add at (idx), move all the greater */
            memmove(&page[idx+1], &page[idx], RECORD_SIZE * (last_rec_idx - idx + 1));
            memcpy(&page[idx], r, RECORD_SIZE);

        } else { /* Add at the end */
            memcpy(&page[last_rec_idx+1], r, RECORD_SIZE);
        }
        save_page_to_data_file(file, page, page_number);

    } else { 
        if (found == false) {
            idx = last_rec_idx;
        }
        bool ret = add_record_overflow_area(file, r, page[idx].overflow_pointer);
        if (ret) {
            page[idx].overflow_pointer = file->overflow_area_size + file->primary_area_size - RECORD_SIZE;
            save_page_to_data_file(file, page, page_number);
        }
    }

    return 0;
}

static int add_first_record(struct idx_seq_file *file, struct record *r)
{
    assert(file != NULL);
    assert(r != NULL);

    FILE *data_file = fopen(file->data_file_path, "wb");
    if (data_file == NULL) {
        fprintf(stderr, "Couldn't open file: %s\n", file->data_file_path);
        return -1;
    }

    FILE *index_file = fopen(file->index_file_path, "wb");
    if (index_file == NULL) {
        fprintf(stderr, "Couldn't open file: %s\n", file->index_file_path);
        fclose(data_file);
        return -1;
    }

    // allocate whole page
    struct record page[RECORDS_PER_PAGE] = {};
    r->overflow_pointer = OVERFLOW_PTR_NULL;
    memcpy(&page[0], r, RECORD_SIZE);

    size_t written = fwrite(page, RECORD_SIZE, RECORDS_PER_PAGE, data_file);
    assert(written == RECORDS_PER_PAGE);

    struct index_entry idx_ent = {
        .key = 1,
        .page_number = 1
    };

    written = fwrite(&idx_ent, sizeof(struct index_entry), 1, index_file);
    assert(written == 1);

    fclose(index_file);
    fclose(data_file);

    file->primary_area_size = PAGESIZE;
    return 0;
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

    if (!is_file_empty(index_file)) {
        fprintf(stderr, "index_file isn't empty as expected\n");
        return -EINVAL;
    }

    if (!is_file_empty(data_file)) {
        fprintf(stderr, "index_file isn't empty as expected\n");
        return -EINVAL;
    }

    file->index_file_path = index_file;
    file->data_file_path = data_file;
    file->overflow_area_size = 0;
    file->primary_area_size = 0;

    struct record dummy_record;
    dummy_record.key = 1;
    dummy_record.overflow_pointer = OVERFLOW_PTR_NULL;
    memset(&dummy_record.numbers, 0, RECORD_LEN);

    int rc = add_first_record(file, &dummy_record);

    return rc;
}

static inline uint32_t _ovf_ptr_translate(struct idx_seq_file *file, uint32_t ovf_ptr)
{
    assert(file != NULL);
    uint32_t ret = (ovf_ptr - file->primary_area_size) / RECORD_SIZE;
}

void print_data_file(struct idx_seq_file *file)
{
    if (file == NULL) {
        fprintf(stderr, "file is NULL\n");
        return;
    }

    struct record rec = {};
    FILE *fp = fopen(file->data_file_path, "rb");
    if (fp == NULL) {
        return;
    }

    bool ovf_info = false;
    printf("\n*** MAIN AREA ***\n");
    while (fread(&rec, RECORD_SIZE, 1, fp) > 0) {
        printf("%d   |", rec.key);
        for (size_t i = 0; i < RECORD_LEN; i++) {
            printf("%hu ", rec.numbers[i]);
        }
        if (rec.overflow_pointer == OVERFLOW_PTR_NULL || rec.overflow_pointer == 0) {
            printf("| %x\n", rec.overflow_pointer);
        } else {
            printf("| %x (ovf_idx:%u)\n", rec.overflow_pointer, _ovf_ptr_translate(file, rec.overflow_pointer));
        }

        if (ovf_info == false && ftell(fp) >= file->primary_area_size) {
            printf("*** OVERFLOW AREA ***\n");
            ovf_info = true;
        }
    }

    fclose(fp);
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

    if (key <= 1) {
        fprintf(stderr, "Invalid key: %d\n", key);
        return -EINVAL;
    }

    uint16_t page_number = get_page_number_from_index_file(file, key);
    
    struct record page[RECORDS_PER_PAGE];
    read_page_from_data_file(file, page, page_number);

    size_t idx = 0;
    bool found = false;
    for (size_t i = 0; i < RECORDS_PER_PAGE; i++) {
        if (page[i].key == key) {
            memcpy(r, &page[i], RECORD_SIZE);
            return 0;
        }
        if (page[i].key > key) {
            idx = i-1;
            found = true;
            break;
        }
    }

    if (!found) {
        for (int last_rec_idx = RECORDS_PER_PAGE-1; last_rec_idx >= 0; last_rec_idx--) {
            if (page[last_rec_idx].key != 0) {
                idx = last_rec_idx;
                break;
            }
        }
    }


    uint32_t overflow_ptr = page[idx].overflow_pointer;
    while (overflow_ptr != OVERFLOW_PTR_NULL) {
        struct record tmp = {};
        read_record_overflow_area(file, overflow_ptr, &tmp);
        if (tmp.key == key) {
            memcpy(r, &tmp, RECORD_SIZE);
            return 0;
        }
        if (tmp.key > key) {
            return -1;
        }
        overflow_ptr = tmp.overflow_pointer;
    }

    return -1;

}