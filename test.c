/**
 * File              : test.c
 * Author            : Igor V. Sementsov <ig.kuzm@gmail.com>
 * Date              : 14.03.2023
 * Last Modified Date: 27.03.2023
 * Last Modified By  : Igor V. Sementsov <ig.kuzm@gmail.com>
 */

#include "kdata2.h"
#include <stdio.h>
#include <stdlib.h>

int callback(
		void *user_data, int ncols, 
		enum KDATA2_TYPE types[], const char *columns[], void *values[], size_t sizes[])
{
	int i;
	for (i = 0; i < ncols; i++) {
		switch (types[i]) {
			case KDATA2_TYPE_NUMBER:
				{
					long *number = values[i];	
					printf("%s = %ld\n", columns[i], *number); break;
				}
			case KDATA2_TYPE_TEXT:
				{
					char *text = values[i];	
					printf("%s: %s\n", columns[i], text); break;
				}			
			case KDATA2_TYPE_DATA:
				{
					printf("%s: data (%ld bytes)\n", columns[i], sizes[i]); break;
				}			
		}
	}

	return 0;
}

void on_log(void *data, const char *msg){
	if (msg)
		printf("%s\n", msg);
}

int main(int argc, char *argv[])
{
	printf("kdata2 test start...\n");
	
	struct kdata2_table *t;
	kdata2_table_init(&t, "pers", KDATA2_TYPE_TEXT,   "name", KDATA2_TYPE_NUMBER, "date", KDATA2_TYPE_DATA,   "photo", NULL); 

	printf("kdata2 init database...\t");
	kdata2_t *database;
	kdata2_init(&database, "database.db", "",  NULL, on_log, NULL, on_log, 10, t, NULL);
	printf("OK\n");

	char *uuid = "80ff0830-9160-467c-897b-722f03e802bd";
	printf("kdata2 add text...\t");
	kdata2_set_text_for_uuid(database, "pers", "name", "Igor V.", uuid);
	printf("OK\n");
	
	printf("kdata2 add number...\t");
	kdata2_set_number_for_uuid(database, "pers", "date", time(NULL), uuid);
	printf("OK\n");

	printf("kdata2 add data...\t");
	// get test.jpg
	size_t size = 220526; 
	FILE *fp = fopen("test.jpg", "r");
	if (!fp){
		perror("no such file test.jpg\n");
		return 1;
	}
	void *data = malloc(size);
	if (!data){
		perror("data malloc\n");
		return 1;
	}
	if (fread(data, size, 1, fp) != 1){
		perror("error to read data from test.jpg\n");
		return 1;
	}	
	fclose(fp);
	
	kdata2_set_data_for_uuid(database, "pers", "photo", data, size, uuid);
	printf("OK\n");

	printf("GET DATA:\n");
	kdata2_get(database, "select * from pers;", NULL, callback);


	printf("press any key...\n");
	getchar();

	kdata2_close(database);
	
	return 0;
}

