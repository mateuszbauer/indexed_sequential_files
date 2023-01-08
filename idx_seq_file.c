#include <idx_seq_file.h>
#include <index.h>
#include <errno.h>
#include <assert.h>
#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

static int number_of_disk_operations = 0;

//#define LOG_ENTRY(msg) (printf("%s:%d\t%s\n", __FILE__, __LINE__, msg));
#define LOG_ENTRY(msg)

static size_t get_file_size(const char *filename)
{
    LOG_ENTRY("get_file_size");
    
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
    LOG_ENTRY("get_page_number_from_index_file");
    assert(file != NULL);
    assert(key >= 1);

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
    number_of_disk_operations++;

    fclose(fp);

    uint16_t page_no = 0;

    // find first key greater than ours and return previous page number
    for (size_t i = 0; i < number_of_entries; i++) {
        if (buffer[i].key > key) {
            page_no = buffer[i-1].page_number;
            break;
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
    LOG_ENTRY("read_page_from_data_file");
    assert(file != NULL);
    assert(page != NULL);
    assert(page_number > 0);

    FILE *fp = fopen(file->data_file_path, "rb");

    size_t offset = (page_number - 1) * PAGESIZE;
    fseek(fp, offset, SEEK_SET);
    size_t read = fread(page, RECORD_SIZE, RECORDS_PER_PAGE, fp);
    assert(read == RECORDS_PER_PAGE);

    number_of_disk_operations++;

    fflush(fp);
    fclose(fp);
}

static void save_page_to_data_file(struct idx_seq_file *file, struct record *page, uint16_t page_number)
{
    LOG_ENTRY("save_page_to_data_file");
    assert(file != NULL);
    assert(page != NULL);
    assert(page_number > 0);

    FILE *fp = fopen(file->data_file_path, "r+b");
    size_t offset = (page_number - 1) * PAGESIZE;
    fseek(fp, offset, SEEK_SET);
    assert(ftell(fp) < file->primary_area_size);
    size_t written = fwrite(page, RECORD_SIZE, RECORDS_PER_PAGE, fp);
    assert(written == RECORDS_PER_PAGE);

    number_of_disk_operations += 1;

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
    LOG_ENTRY("read_record_overflow_area");
    assert(file != NULL);
    assert(buff != NULL);
    assert(ovf_ptr != OVERFLOW_PTR_NULL);

    FILE *fp = fopen(file->data_file_path, "rb");
    fseek(fp, ovf_ptr, SEEK_SET);

    size_t read = fread(buff, RECORD_SIZE, 1, fp);
    assert(read == 1);
    number_of_disk_operations++;

    fflush(fp);
    fclose(fp);
}

static void save_record_overflow_area(struct idx_seq_file *file, uint32_t ovf_ptr, struct record *r)
{
    LOG_ENTRY("save_record_overflow_area");
    assert(file != NULL);
    assert(r != NULL);
    assert(ovf_ptr != OVERFLOW_PTR_NULL);
    assert(file->data_file_path != NULL);

    FILE *fp = fopen(file->data_file_path, "r+b");
    assert(fp != NULL);
    fseek(fp, ovf_ptr, SEEK_SET);

    size_t written = fwrite(r, RECORD_SIZE, 1, fp);
    assert(written == 1);
    number_of_disk_operations++;

    fflush(fp);
    fclose(fp);
}

/**
 * Returns true if overflow pointer from data file has to be updated
*/
static bool add_record_overflow_area(struct idx_seq_file *file, struct record *r, uint32_t ovf_ptr)
{
    LOG_ENTRY("add_record_overflow_area");
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
    LOG_ENTRY("add_record");
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

    number_of_disk_operations = 0;

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
        size_t i = 0;
        if (found == false) {
            i = last_rec_idx;
        } else {
            i = idx-1;
        }
        bool ret = add_record_overflow_area(file, r, page[i].overflow_pointer);
        if (ret) {
            page[i].overflow_pointer = file->overflow_area_size + file->primary_area_size - RECORD_SIZE;
            save_page_to_data_file(file, page, page_number);
        }
    }

    double a = (double)file->overflow_area_size;
    double b = (double)file->primary_area_size;
    double overflow_ratio = a / (a+b);
    if (overflow_ratio > BETA) {
        reorganize(file);
    }

    return number_of_disk_operations;
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
    number_of_disk_operations += 1;

    struct index_entry idx_ent = {
        .key = 1,
        .page_number = 1
    };

    written = fwrite(&idx_ent, sizeof(struct index_entry), 1, index_file);
    number_of_disk_operations += 1;
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
    return (ovf_ptr - file->primary_area_size) / RECORD_SIZE;
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
    uint16_t page_no = 1;
    size_t records_read = 0;
    size_t rc = 0;

    printf("\n*** MAIN AREA ***\n");

    while ((rc = fread(&rec, RECORD_SIZE, 1, fp)) > 0) {
        records_read += rc;
        if (ovf_info == false && (records_read-1) % RECORDS_PER_PAGE == 0) {
            printf("Page: %hu\n", page_no);
            page_no++;
        }


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
    number_of_disk_operations += records_read;

    fclose(fp);
}

int get_record(struct idx_seq_file *file, int32_t key, struct record *r)
{
    LOG_ENTRY("get_record");
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

static uint16_t get_number_of_pages(struct idx_seq_file *file)
{
    assert(file != NULL);
    assert (file->primary_area_size % PAGESIZE == 0);

    return (file->primary_area_size / PAGESIZE);
}

static int32_t get_next_record_page(struct idx_seq_file *file, uint16_t page_no, struct record *next, size_t index)
{
    assert(file != NULL);
    assert(next != NULL);

    struct record page[RECORDS_PER_PAGE] = {};
    uint16_t pages_total = get_number_of_pages(file);

    while (page_no <= pages_total) {
        memset(page, 0x0, RECORD_SIZE * RECORDS_PER_PAGE);
        read_page_from_data_file(file, page, page_no);

        for (; index < RECORDS_PER_PAGE; index++) {
            if (page[index].key != 0) {
                memcpy(next, &page[index], RECORD_SIZE);
                return page[index].key;
            }
        }
        index = 0;
        page_no++;
    }

    return 0;

}

// return 0 if not found
static int32_t get_next_record(struct idx_seq_file *file, int32_t key, struct record *next)
{
    LOG_ENTRY("get_next_record");
    assert(file != NULL);
    assert(key > 0);
    assert(next != NULL);

    uint16_t page_number = get_page_number_from_index_file(file, key); 
    struct record page[RECORDS_PER_PAGE] = {};
    read_page_from_data_file(file, page, page_number);

    bool found_current = false;
    size_t idx = 0;

    for (size_t i = 0; i < RECORDS_PER_PAGE; i++) {
        if (page[i].key == key) {
            idx = i;
            found_current = true;
            break;
        }

        if (page[i].key > key) { // if key is greater, then current is in the overflow area
            found_current = false;
            idx = i - 1;
            break;
        }

        if (i == RECORDS_PER_PAGE-1) {
            found_current = false;
            idx = i;
        }
    }


    if (found_current) { // current in the main area
        if (page[idx].overflow_pointer != OVERFLOW_PTR_NULL) {
            read_record_overflow_area(file, page[idx].overflow_pointer, next);
            return next->key;
        } else { // the next is page[idx+1] // TODO: jak zero to nie dziala
            return get_next_record_page(file, page_number, next, idx+1);
        }
    } else { // current in the overflow area
        struct record current = {.key = 0};
        uint32_t ovf_ptr = page[idx].overflow_pointer;
        while (true) {
            read_record_overflow_area(file, ovf_ptr, &current);
            if (current.key == key) {
                break;
            } else {
                ovf_ptr = current.overflow_pointer;
                assert(ovf_ptr != OVERFLOW_PTR_NULL);
            }
        }

        if (current.overflow_pointer != OVERFLOW_PTR_NULL) { // next is the next one in the overflow area
            read_record_overflow_area(file, current.overflow_pointer, next);
            return next->key;
        } else { // next is on the next page (if the page exists) // TODO: jak zero to nie dziala
            // two cases
            // 1. next is on the 
            for (size_t i = 0; i < RECORDS_PER_PAGE; i++) {
                if (page[i].key > current.key) {
                    memcpy(next, &page[i], RECORD_SIZE);
                    return next->key;
                }
            }            


            if (page_number + 1 > get_number_of_pages(file)) {
                return 0;
            }
            return get_next_record_page(file, page_number+1, next, 0);
        }
    }

    return 0;
}

int update_record(struct idx_seq_file *file, struct record *r)
{
    if (file == NULL) {
        fprintf(stderr, "File is NULL\n");
        return -EINVAL;
    }

    int ret = delete_record(file, r->key);

    ret += add_record(file, r);

    return ret;
}

int delete_record(struct idx_seq_file *file, int32_t key)
{
    if (file == NULL) {
        fprintf(stderr, "File is NULL\n");
        return -EINVAL;
    }

    if (key <= 1) {
        fprintf(stderr, "Invalid key\n");
        return -EINVAL;
    }

    number_of_disk_operations = 0;

    uint16_t page_number = get_page_number_from_index_file(file, key);
    struct record page[RECORDS_PER_PAGE] = {};
    read_page_from_data_file(file, page, page_number);

    size_t idx = 0;
    bool found_in_main_area = false;

    // find record
    for (size_t i = 0; i < RECORDS_PER_PAGE; i++) {
        if (page[i].key == key) {
            idx = i;
            found_in_main_area = true;
            break;
        }
        else if (page[i].key > key) {
            idx = i - 1;
            break;
        }
    }

    // first record on page
    if (found_in_main_area && idx == 0) {
        FILE *fp = fopen(file->index_file_path, "rb");
        struct index_entry tmp = {};
        size_t read = 0;
        size_t rc = 0;
        while ((rc = fread(&tmp, sizeof(struct index_entry), 1, fp)) > 0) {
            read += rc;
            if (tmp.key == key) {

                if (page[idx].overflow_pointer != OVERFLOW_PTR_NULL) {
                    struct record r = {};
                    read_record_overflow_area(file, page[idx].overflow_pointer, &r);
                    tmp.key = r.key;
                } else {
                    tmp.key = page[idx+1].key;
                }

                fclose(fp);
                fopen(file->index_file_path, "ab");
                fseek(fp, (read-1)*(sizeof(struct index_entry)), SEEK_SET);
                fwrite(&tmp, sizeof(struct index_entry), 1, fp);
                number_of_disk_operations++;
                break;
            }
        }
        number_of_disk_operations += read;
    }

    // if the record is in the main area we gotta move records[idx+1:last]
    if (found_in_main_area) {
        if (page[idx].overflow_pointer != OVERFLOW_PTR_NULL) { // simply replace
            uint32_t ovf_ptr = page[idx].overflow_pointer;
            struct record tmp;
            read_record_overflow_area(file, ovf_ptr, &tmp);

            memcpy(&page[idx], &tmp, RECORD_SIZE);
            memset(&tmp, 0x0, RECORD_SIZE);

            save_record_overflow_area(file, ovf_ptr, &tmp);
            save_page_to_data_file(file, page, page_number);
            return number_of_disk_operations;
        }

        if (idx == RECORDS_PER_PAGE-1) { // last 
            memset(&page[idx], 0x0, RECORD_SIZE);
        } else {
            for (size_t i = idx; i+1 < RECORDS_PER_PAGE; i++) {
                memcpy(&page[i], &page[i+1], RECORD_SIZE);
            }
            memset(&page[RECORDS_PER_PAGE-1], 0x0, RECORD_SIZE);
        }

        save_page_to_data_file(file, page, page_number);
        return number_of_disk_operations;
    }

    // record is in the overflow area
    struct record prev = {};
    struct record current = {};
    memcpy(&prev, &page[idx], RECORD_SIZE);
    uint32_t ovf_ptr = prev.overflow_pointer;

    read_record_overflow_area(file, ovf_ptr, &current);

    if (current.key == key) { // prev is on page in the main area, current in ovf
        // delete from overflow
        struct record tmp = {};
        read_record_overflow_area(file, ovf_ptr, &tmp);
        memset(&tmp, 0x0, RECORD_SIZE);
        save_record_overflow_area(file, ovf_ptr, &tmp);

        // update pointer
        page[idx].overflow_pointer = current.overflow_pointer;
        save_page_to_data_file(file, page, page_number);

    } else {
        uint32_t prev_ovf_ptr;
        while (true) {
            prev_ovf_ptr = prev.overflow_pointer;
            memcpy(&prev, &current, RECORD_SIZE);
            read_record_overflow_area(file, current.overflow_pointer, &current);
            if (current.key == key) {
                break;
            }
            assert(current.overflow_pointer != OVERFLOW_PTR_NULL);
        }

        uint32_t curr_ovf_ptr = prev.overflow_pointer;
        prev.overflow_pointer = current.overflow_pointer;
        save_record_overflow_area(file, prev_ovf_ptr, &prev);

        memset(&current, 0x0, RECORD_SIZE);
        save_record_overflow_area(file, curr_ovf_ptr, &current);
    }

    return number_of_disk_operations;
}

void reorganize_save_page(const char *data_tmp_path, const char *index_tmp_path, struct record *page, uint16_t page_no)
{
    LOG_ENTRY("reorganize_save_page");
    assert(index_tmp_path != NULL);
    assert(data_tmp_path != NULL);
    assert(page != NULL);

    FILE *data = fopen(data_tmp_path, "a+b");
    FILE *index = fopen(index_tmp_path, "a+b");

    fseek(data, 0, SEEK_END);
    fseek(index, 0, SEEK_END);

    struct index_entry idx_entry = {
        .key = page[0].key,
        .page_number = page_no
    };

    fwrite(page, RECORD_SIZE, RECORDS_PER_PAGE, data);
    fwrite(&idx_entry, sizeof(struct index_entry), 1, index);

    number_of_disk_operations += 2;

    fclose(index);
    fclose(data);
}

void reorganize(struct idx_seq_file *file)
{
    LOG_ENTRY("reorganize");
    if (file == NULL) {
        fprintf(stderr, "File is NULL\n");
        return;
    }

    const char *data_tmp = "data_tmp.bin";
    const char *index_tmp = "index_tmp.bin";

    struct record new_page[RECORDS_PER_PAGE] = {};
    uint16_t new_page_number = 1;

    // set up a dummy record
    struct record tmp = {.key = 1, .overflow_pointer = OVERFLOW_PTR_NULL};
    memset(&tmp.numbers, 0x0, RECORD_LEN);

    memcpy(&new_page[0], &tmp, RECORD_SIZE);
    size_t new_page_idx = 1;
    int32_t key = 1;

    while (get_next_record(file, key, &tmp) > 0) {
        tmp.overflow_pointer = OVERFLOW_PTR_NULL;
        key = tmp.key;

        memcpy(&new_page[new_page_idx], &tmp, RECORD_SIZE);
        new_page_idx++;

        if (new_page_idx == ALPHA * RECORDS_PER_PAGE) {
            reorganize_save_page(data_tmp, index_tmp, new_page, new_page_number);
            memset(new_page, 0x0, PAGESIZE);
            new_page_idx = 0;
            new_page_number++;
        }
    }

    if (new_page_idx > 0) {
        reorganize_save_page(data_tmp, index_tmp, new_page, new_page_number);
    }

    remove(file->data_file_path);
    remove(file->index_file_path);

    rename(data_tmp, file->data_file_path);
    rename(index_tmp, file->index_file_path);

    file->primary_area_size = get_file_size(file->data_file_path);
    file->overflow_area_size = 0;
}