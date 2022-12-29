#include <record.h>
#include <stdio.h>
#include <assert.h>

void record_print(struct record *r)
{
    assert(r != NULL);
    printf("Record:\n\tKey: %d\n\tData: ", r->key);
    for (size_t i = 0; i < RECORD_LEN; i++) {
        printf("%hhu ", r->numbers[i]);
    }
    printf("\n\tPointer: %u\n", r->overflow_pointer);
}