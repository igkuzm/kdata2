/**
 * File              : test.c
 * Author            : Igor V. Sementsov <ig.kuzm@gmail.com>
 * Date              : 14.03.2023
 * Last Modified Date: 17.03.2023
 * Last Modified By  : Igor V. Sementsov <ig.kuzm@gmail.com>
 */

#include "kdata2.h"
#include <stdio.h>
#include <stdlib.h>

int callback(void *user_data, enum KDATA2_TYPE type, const char *column, void *data, size_t size){
	switch (type) {
		case KDATA2_TYPE_NUMBER:
			{
				long *number = data;	
				printf("%s = %ld\n", column, *number); break;
			}
		case KDATA2_TYPE_TEXT:
			{
				char *text = data;	
				printf("%s: %s\n", column, text); break;
			}			
		case KDATA2_TYPE_DATA:
			{
				char *text = data;	
				printf("%s: data (%ld bytes)\n", column, size); break;
			}			
	}

	return 0;
}

int main(int argc, char *argv[])
{
	printf("kdata2 test start...\n");
	
	kdata2_col_t name      = {KDATA2_TYPE_TEXT,   "name" };
	kdata2_col_t date      = {KDATA2_TYPE_NUMBER, "date" };
	kdata2_col_t photo     = {KDATA2_TYPE_DATA,   "photo"};

	kdata2_col_t *cols[]   = {&name, &date, &photo, NULL };

	kdata2_tab_t pers      = {"pers", cols};

	kdata2_tab_t *tables[] = {&pers, NULL};

	printf("kdata2 init database...\t");
	kdata2_t *database;
	kdata2_init(&database, "database.db", "", tables, 300);
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
	kdata2_get(database, "pers", NULL, NULL, callback);


	printf("press any key...\n");
	getchar();

	kdata2_close(database);
	
	return 0;
}

