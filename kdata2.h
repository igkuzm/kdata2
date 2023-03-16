/**
 * File              : kdata2.h
 * Author            : Igor V. Sementsov <ig.kuzm@gmail.com>
 * Date              : 10.03.2023
 * Last Modified Date: 16.03.2023
 * Last Modified By  : Igor V. Sementsov <ig.kuzm@gmail.com>
 */

/*
 * this is data library to use with SQLite and YandexDisk sync
 */

#ifndef KDATA2_H
#define KDATA2_H

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>  //for sleep
#include <pthread.h>

#include "SQLiteConnect/SQLiteConnect.h"
#include "SQLiteConnect/sqlite3.h"
#include "cYandexDisk/cYandexDisk.h"
#include "cYandexDisk/cJSON.h"
#include "cYandexDisk/uuid4/uuid4.h"

#define DATABASE "kdata_database"
#define DATAFILES "kdata_data"
#define DELETED "kdata_deleted"

enum KDATA2_TYPE {
	KDATA2_TYPE_NUMBER,        // SQLite INT 
	KDATA2_TYPE_TEXT,		   // SQLite TEXT
	KDATA2_TYPE_DATA		   // SQLite BLOB - to store data in base64 encode
};

/* this is data column */
typedef struct kdata2_col {
	enum KDATA2_TYPE type;     // data type
	char name[128];            // name of column
} kdata2_col_t;

/* add column to columns array - set columns to NULL to create new array */
static  kdata2_col_t ** kdata2_column_add(
		kdata2_col_t **columns, 
		enum KDATA2_TYPE type, 
		const char *name
);

/* this is data table */
typedef struct kdata2_tab {
	char name[128];            // name of table
	kdata2_col_t ** columns;   // NULL-terminated array of columns
} kdata2_tab_t;

/* add table to tables array - set tables to NULL to create new array */
static  kdata2_tab_t ** kdata2_table_add(
		kdata2_tab_t **tables, 
		kdata2_col_t **columns, 
		const char *name
);


/* this is dataset */
typedef struct kdata2 {
	sqlite3 *db;               // sqlite3 database pointer
	char filepath[BUFSIZ];     // file path to where store SQLite data 	
	char access_token[64];     // Yandex Disk access token
	kdata2_tab_t ** tables;    // NULL-terminated array of tables
	int sec;				   // number of seconds of delay to sinc data with Yandex Disk
	void * user_data;          // data to transfer trough callback
	int (*callback)(void * user_data, char * msg); // yandex disk daemon
												   // callback	
} kdata2_t;

/* init function */
int kdata2_init(
		kdata2_t ** dataset,
		const char * filepath,
		const char * access_token,
		kdata2_tab_t ** tables,
		int sec,
		void * user_data,
		int (*callback)(void * user_data, char * msg)
);

int kdata2_set_access_token(kdata2_t * dataset, const char *access_token);

/* close database and free memory */
int kdata2_close(kdata2_t *dataset);

/* set number for row with uuid; set uuid to NULL to create new */
int kdata2_set_number_for_uuid(
		kdata2_t * dataset, 
		const char *tablename, 
		const char *column, 
		long number, 
		const char *uuid);

/* set text for row with uuid; set uuid to NULL to create new */
int kdata2_set_text_for_uuid(
		kdata2_t * dataset, 
		const char *tablename, 
		const char *column, 
		const char *text, 
		const char *uuid);

/* set data for row with uuid; set uuid to NULL to create new */
int kdata2_set_data_for_uuid(
		kdata2_t * dataset, 
		const char *tablename, 
		const char *column, 
		void *data, 
		int len,
		const char *uuid);

/* remove row with uuid */
int kdata2_remove_for_uuid(
		kdata2_t * dataset, 
		const char *tablename, 
		const char *uuid);

/* get rows for table; set predicate to "WHERE uuid = 'uuid' to get row with uuid" */
void kdata2_get(
		kdata2_t * dataset, 
		const char *tablename, 
		const char *predicate,
		void *user_data,
		int (*callback)(
			void *user_data,
			enum KDATA2_TYPE type,
			const char *column, 
			void *value,
			size_t size
			)
		);

#endif /* ifndef KDATA2_H */
