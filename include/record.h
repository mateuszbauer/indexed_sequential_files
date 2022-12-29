#ifndef _RECORD_H_
#define _RECORD_H_

#include <stdint.h>

#define RECORD_LEN 15
#define RECORD_SIZE (sizeof(struct record))
#define OVERFLOW_PTR_NULL 0xdeaddead

struct record {
    uint8_t numbers[RECORD_LEN];
    int32_t key;
    uint32_t overflow_pointer;
} __attribute__((packed));

// Prints a record in a human-readable format
void record_print(struct record *r);

#endif // _RECORD_H_
