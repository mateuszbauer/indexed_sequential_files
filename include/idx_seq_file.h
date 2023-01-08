#ifndef _INDEXED_SEQUENTIAL_FILE_H_
#define _INDEXED_SEQUENTIAL_FILE_H_

#include <stdint.h>
#include <record.h>

#define ALPHA 0.5
#define BETA 0.2
#define RECORDS_PER_PAGE 4
#define PAGESIZE (RECORDS_PER_PAGE * RECORD_SIZE)

struct idx_seq_file {
    const char *index_file_path;
    const char *data_file_path;
    uint32_t primary_area_size;
    uint32_t overflow_area_size;
};

void reorganize(struct idx_seq_file *file);

int delete_record(struct idx_seq_file *file, int32_t key);

/**
 * Initializes indexed sequential file struct
 * 
 * Both index and data file should be empty
 * 
 * \param[in,out] file pointer to \ref struct idx_seq_file
 * \param[in] index_file path to a file with the indexes 
 * \param[in] data_file path to a file with the data
 * 
 * \return 0 on success
*/
int idx_seq_file_init(struct idx_seq_file *file, const char *index_file, const char *data_file);

/**
 * Adds given record to the indexed sequential file
 * 
 * \param[in] file pointer to \ref struct idx_seq_file
 * \param[in] r pointer to the record
 * 
 * \return 0 on success
*/
int add_record(struct idx_seq_file *file, struct record *r);

/**
 * Gets a record associated with a given key
 * 
 * \param[in] file pointer to \ref struct idx_seq_file
 * \param[in] key key 
 * \param[out] r pointer to the record struct, in which the result will be stored
 * 
 * \return 0 on success
*/
int get_record(struct idx_seq_file *file, int32_t key, struct record *r);

void print_data_file(struct idx_seq_file *file);

/*
int delete_record(struct idx_seq_file *file, int32_t key);

int update_record(struct idx_seq_file *file, int32_t key, struct record *r);

void print_index_file(struct idx_seq_file *file); */

#endif // _INDEXED_SEQUENTIAL_FILE_H_