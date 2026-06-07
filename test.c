/**
 * File              : test.c
 * Author            : Igor V. Sementsov <ig.kuzm@gmail.com>
 * Date              : 14.03.2023
 * Last Modified Date: 07.06.2026
 * Last Modified By  : Igor V. Sementsov <ig.kuzm@gmail.com>
 */

#include "kdata2.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "modules/yandexdisk/yandexdisk.h"
#include "modules/yclients/cYclients/cYclients.h"


int progress(
			void *progressp, pphase phase, int current, int total)
{
	if (phase == PPHASE_COUNTING)
		printf("\x1B[32mCOUNTING: %d of %d (%0.2lf%%)\x1B[0m\n", 
				current, total,
				(double)current/(double)total*100);
	if (phase == PPHASE_UPLOADING)
		printf("\x1B[32mUPLOADING: %d of %d (%0.2lf%%)\x1B[0m\n", 
				current, total,
				(double)current/(double)total*100);
	if (phase == PPHASE_DOWNLOADING)
		printf("\x1B[32mDOWNLOADING: %d of %d (%0.2lf%%)\x1B[0m\n", 
				current, total,
				(double)current/(double)total*100);
	if (phase == PPHASE_DELETING)
		printf("\x1B[32mDELETING: %d of %d (%0.2lf%%)\x1B[0m\n", 
				current, total,
				(double)current/(double)total*100);
	return 0;
}

int file_progress(void *d, double dlt, double dln, double ut, double un)
{
	printf("FILE TRANSFER: U: %lf%%, D: %lf%%\n",
			un/ut*100, dln/dlt*100);

	return 0;
}

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

			default:
				break;
		}
	}

	return 0;
}

void on_log(void *data, const char *msg){
	if (msg)
		printf("%s\n", msg);
}

void on_err(void *data, const char *msg){
	if (msg)
		printf("\x1B[31m%s\x1B[0m\n", msg);
}


int companies_cb(void *userdata, const CYCCompany *company)
{
	printf("COMPANY: %s\n", company->title);
	return 0;
}


int main(int argc, char *argv[])
{
	printf("kdata2 test start...\n");

	char secret[16], login[32], password[32];
	int company_id;
	
	if (argc < 2){
		//printf("usage: %s login password\n", argv[0]);
		//return 0;
		printf("enter login\n");
		scanf("%31s", login);
		printf("enter password\n");
		scanf("%31s", password);
	} else {
		strncat(login,argv[1],31);
		strncat(password,argv[2],31);
	}
	
	const CYCUser *user = NULL;
	const CYC2fa  *user2fa = NULL;
	CYCLIENTS_AUTH auth = CYCLIENTS_AUTH_ERROR;

	auth = cyclients_login(login, password,
		 	&user, &user2fa);
	
	cyclients_companies(user->user_token,
			NULL,
		 	NULL, companies_cb);

	return 0;


	
	struct kdata2_table *t;
	kdata2_table_init(&t, "pers", 
			KDATA2_TYPE_TEXT,   "name", 
			KDATA2_TYPE_NUMBER, "date", 
			KDATA2_TYPE_DATA,   "photo", NULL); 

	printf("kdata2 init database...\t");
	kdata2_t *database;
	kdata2_init(&database, "database.db", NULL, on_err, NULL, on_log, t, NULL);
	printf("OK\n");
	
	/* YANDEX DISK */
	printf("init yandexdisk...\t");
	char token[128] = {0};
	FILE *fp = fopen("token", "r");
	if (fp){
		fread(token, 1, 58, fp);
		fclose(fp);
	}
	printf("TOKEN: '%s'\t", token);
	kdydm_t *module = yandex_disk_module_load(
			database, token, NULL, progress);
	yandex_disk_set_file_download_progress(
			module, 
			NULL, 
			file_progress);
	printf("OK\n");
	/* */
	
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
	fp = fopen("test.jpg", "r");
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

