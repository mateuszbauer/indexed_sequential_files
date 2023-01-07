#include <stdio.h>
#include <stdlib.h>
#include <record.h>
#include <idx_seq_file.h>
#include <assert.h>

#define NUM_REC 7

int main () {

	struct idx_seq_file file;

	idx_seq_file_init(&file, "index.bin", "data.bin");

	struct record records[NUM_REC] = {};

	for (size_t i = 0; i < NUM_REC; i++) {
		records[i].key = i+2;
	}

	for (size_t i = 0; i < NUM_REC; i++) {
		if (i == 3) continue;
		add_record(&file, &records[i]);
		print_data_file(&file);
	}

	add_record(&file, &records[3]);
	print_data_file(&file);


	struct record ret = {};
	int32_t klucz = 8;

	assert (get_record(&file, klucz, &ret) == 0);
	record_print(&ret);


	return 0;
}
