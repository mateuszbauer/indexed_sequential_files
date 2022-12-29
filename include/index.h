#ifndef _INDEX_H_
#define _INDEX_H_

#include <stdint.h>

struct index_entry {
    int32_t key;
    uint16_t page_number;
} __attribute__((packed));

#endif // _INDEX_H_