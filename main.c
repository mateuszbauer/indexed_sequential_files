#include <stdio.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <record.h>
#include <idx_seq_file.h>

#define NUM_REC 7

int main () {

	struct idx_seq_file file;

	idx_seq_file_init(&file, "index.bin", "data.bin");

	struct record tmp = {};
	memset(&tmp, 0x0, RECORD_SIZE);

	for (size_t i = 0; i < 14; i++) {
		tmp.key = i+2;
		add_record(&file, &tmp);
	}


	print_data_file(&file);

	return 0;
}
