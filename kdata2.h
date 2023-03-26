/**
 * File              : kdata2.h
 * Author            : Igor V. Sementsov <ig.kuzm@gmail.com>
 * Date              : 10.03.2023
 * Last Modified Date: 27.03.2023
 * Last Modified By  : Igor V. Sementsov <ig.kuzm@gmail.com>
 */

/*
 * this is data library to use with SQLite and YandexDisk sync
 */

#ifndef KDATA2_H
#define KDATA2_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdio.h>
#include <time.h>
#include <pthread.h>
#include "sqlite3.h"

#define DATABASE  "kdata_database"
#define DELETED   "kdata_deleted"
#define DATAFILES "kdata_data"

enum KDATA2_TYPE {
	KDATA2_TYPE_NULL,         
	KDATA2_TYPE_NUMBER,        // SQLite INT 
	KDATA2_TYPE_TEXT,		   // SQLite TEXT
	KDATA2_TYPE_DATA		   // SQLite BLOB - to store binary data
};

/* this is data column */
struct kdata2_column {
	enum KDATA2_TYPE type;     // data type
	char columnname[128];      // name of column
};

/* this is data table */
struct kdata2_table {
	char tablename[128];			   // name of table
	struct kdata2_column ** columns;   // NULL-terminated array of column pointers
};

/* allocate table structure with allocated columns; va_args: type, columnname, ... NULL */
void kdata2_table_init(struct kdata2_table **t, const char * tablename, ...); 

/* this is kdata2 database */
typedef struct kdata2 {
	sqlite3 *db;                   // sqlite3 database pointer
	char filepath[BUFSIZ];         // file path to where store SQLite data 	
	char access_token[64];         // Yandex Disk access token
	struct kdata2_table ** tables; // NULL-terminated array of tables pointers
	int sec;				       // number of seconds of delay to sinc data with Yandex Disk
	pthread_t tid;                 // Yandex Disk daemon thread id
} kdata2_t;

/* init function */
int kdata2_init(
		kdata2_t     ** database,     // pointer to kdata2_t
		const char    * filepath,     // file path to where store SQLite data
		const char    * access_token, // Yandex Disk access token (may be NULL)
		int sec,                      // number of seconds of delay to sinc data with Yandex Disk
		...							  // kdata2_table, NULL
);
/* set access_token */
int kdata2_set_access_token(kdata2_t * database, const char *access_token);

/* close database and free memory */
int kdata2_close(kdata2_t *dataset);

/* set number for data entity with uuid; set uuid to NULL to create new */
int kdata2_set_number_for_uuid(
		kdata2_t * database, 
		const char *tablename, 
		const char *column, 
		long number, 
		const char *uuid);

/* set text for data entity with uuid; set uuid to NULL to create new */
int kdata2_set_text_for_uuid(
		kdata2_t * database, 
		const char *tablename, 
		const char *column, 
		const char *text, 
		const char *uuid);

/* set data for data entity with uuid; set uuid to NULL to create new */
int kdata2_set_data_for_uuid(
		kdata2_t * database, 
		const char *tablename, 
		const char *column, 
		void *data, 
		int len,
		const char *uuid);

/* remove data entity with uuid */
int kdata2_remove_for_uuid(
		kdata2_t * database, 
		const char *tablename, 
		const char *uuid);

/* get entities for table; set predicate to "WHERE uuid = 'uuid'" to get with uuid */
void kdata2_get(
		kdata2_t * database, 
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

#ifdef __cplusplus
}  /* end of the 'extern "C"' block */
#endif

#endif /* ifndef KDATA2_H */
