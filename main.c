#include <stdio.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <record.h>
#include <idx_seq_file.h>

void print_menu(void) {
	printf("1.\tAdd record\n");
	printf("2.\tAdd record (zeroed)\n");
	printf("3.\tDelete record\n");
	printf("4.\tQuit\n");
}

int main () {

	struct idx_seq_file file;

	struct record zeroed = {};
	memset(&zeroed, 0x0, RECORD_SIZE);

	idx_seq_file_init(&file, "index.bin", "data.bin");

	bool running = true;
	while (running) {
		print_data_file(&file);
		printf("\n\n");
		print_menu();

		char opt;
		scanf("%c", &opt);
		switch (opt) {
		case '1': {
			break;
		}

		case '2': {
			printf("Enter key: ");
			int32_t key;
			scanf("%d", &key);
			zeroed.key = key;
			add_record(&file, &zeroed);
			zeroed.key = 0;
			break;
		}

		case '3': {
			printf("Enter key: ");
			int32_t key;
			scanf("%d", &key);
			delete_record(&file, key);
			break;
		}

		case '4': {
			running = false;
			break;
		}
		
		default: {
			break;
		}

		}
		printf("\n\n");
	}

	return 0;
}
