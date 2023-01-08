#ifndef _INDEXED_SEQUENTIAL_FILE_H_
#define _INDEXED_SEQUENTIAL_FILE_H_

#include <stdint.h>
#include <record.h>

#define ALPHA 0.5
#define BETA 0.2
#define RECORDS_PER_PAGE 10
#define PAGESIZE (RECORDS_PER_PAGE * RECORD_SIZE)

struct idx_seq_file {
    const char *index_file_path;
    const char *data_file_path;
    uint32_t primary_area_size;
    uint32_t overflow_area_size;
};

void reorganize(struct idx_seq_file *file);

int delete_record(struct idx_seq_file *file, int32_t key);

int update_record(struct idx_seq_file *file, struct record *r);

int idx_seq_file_init(struct idx_seq_file *file, const char *index_file, const char *data_file);

int add_record(struct idx_seq_file *file, struct record *r);

int get_record(struct idx_seq_file *file, int32_t key, struct record *r);

void print_data_file(struct idx_seq_file *file);

#endif // _INDEXED_SEQUENTIAL_FILE_H_