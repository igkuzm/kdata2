/**
 * File              : kdata2.h
 * Author            : Igor V. Sementsov <ig.kuzm@gmail.com>
 * Date              : 10.03.2023
 * Last Modified Date: 25.04.2026
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

#ifdef _MSC_VER
#undef EXPORTDLL
#define EXPORTDLL __declspec(dllexport)
#else
#undef EXPORTDLL
#define EXPORTDLL
#endif
	
#include <stdio.h>
#include <time.h>
#include <pthread.h>
#include <sqlite3.h>

#ifndef UUIDCOLUMN
#define UUIDCOLUMN "ZRECORDNAME"
#endif /* ifndef UUIDCOLUMN */

int uuid_new(char uuid[37]);

enum KDATA2_TYPE {
	KDATA2_TYPE_NULL,         
	KDATA2_TYPE_NUMBER,        // SQLite INTEGER 
	KDATA2_TYPE_TEXT,		   // SQLite TEXT
	KDATA2_TYPE_DATA,		   // SQLite BLOB - to store binary data
	KDATA2_TYPE_FLOAT          // SQLite REAL 
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
int EXPORTDLL kdata2_table_init(struct kdata2_table **t, const char * tablename, ...); 

/* this is kdata2 database */
typedef struct kdata2 {
	sqlite3 *db;                   // sqlite3 database pointer
	char filepath[BUFSIZ];         // file path to where store SQLite data 	
	struct kdata2_table ** tables; // NULL-terminated array of tables pointers
	void *on_error_data;           // pointer to transfer through on_error callback
	void (*on_error)(              // callback on error
			void *on_error_data, 
			const char *error);    // error message
	void *on_log_data;             // pointer to transfer through on_log callback
	void (*on_log)(                // callback on log message
			void *on_log_data,
			const char *message    // log message
			);
} kdata2_t;

/* init function */
int EXPORTDLL
kdata2_init(
		kdata2_t     ** database,     // pointer to kdata2_t
		const char    * filepath,     // file path to where store SQLite data
		void          *on_error_data,
		void         (*on_error)      (void *on_error_data, const char *error),
		void          *on_log_data,
		void         (*on_log)        (void *on_log_data, const char *message),
		...							  // kdata2_table, NULL
);

/* close database and free memory */
int EXPORTDLL kdata2_close(kdata2_t *dataset);

/* set number for data entity with uuid; set uuid to NULL to create new */
/* return UUID (allocated if uuid is NULL) */
char EXPORTDLL * 
kdata2_set_number_for_uuid(
		kdata2_t * database, 
		const char *tablename, 
		const char *column, 
		long number, 
		const char *uuid);

/* set number for data entity with uuid; set uuid to NULL to create new */
/* return UUID (allocated if uuid is NULL) */
char EXPORTDLL * 
kdata2_set_float_for_uuid(
		kdata2_t * database, 
		const char *tablename, 
		const char *column, 
		double number, 
		const char *uuid);

/* set text for data entity with uuid; set uuid to NULL to create new */
/* return UUID (allocated if uuid is NULL) */
char EXPORTDLL * 
kdata2_set_text_for_uuid(
		kdata2_t * database, 
		const char *tablename, 
		const char *column, 
		const char *text, 
		const char *uuid);

/* set data for data entity with uuid; set uuid to NULL to create new */
/* return UUID (allocated if uuid is NULL) */
char EXPORTDLL * 
kdata2_set_data_for_uuid(
		kdata2_t * database, 
		const char *tablename, 
		const char *column, 
		void *data, 
		int len,
		const char *uuid);

/* remove data entity with uuid */
int EXPORTDLL
kdata2_remove_for_uuid(
		kdata2_t * database, 
		const char *tablename, 
		const char *uuid);

/* get entities for table; set predicate to "WHERE uuid = 'uuid'" to get with uuid */
void EXPORTDLL
kdata2_get(
		kdata2_t * database, 
		const char *SQL,
		void *user_data,
		int (*callback)(
			void *user_data,
			int	num_cols,
			enum KDATA2_TYPE types[],
			const char *columns[], 
			void *values[],
			size_t sizes[]
			)
		);

// return string value of SQL request
char EXPORTDLL *
kdata2_get_string(
		kdata2_t * database, 
		const char *SQL);
	
/* Helpers */
int EXPORTDLL 
kdata2_sqlite3_exec(kdata2_t *d, const char *sql);

int EXPORTDLL 
kdata2_sqlite3_prepare(kdata2_t *d, const char *sql, sqlite3_stmt **stmt);

char * EXPORTDLL 
kdata2_sql_select_table_request(
		kdata2_t *d, const char *tablename);

#define kdata2_sqlite3_for_each(d, sql, stmt) \
sqlite3_stmt *stmt;\
int sqlite_step;\
if (kdata2_sqlite3_prepare(d, sql, &stmt) == 0)\
for (sqlite_step = sqlite3_step(stmt);\
	 sqlite_step	!= SQLITE_DONE || ({sqlite3_finalize(stmt); 0;});\
	 sqlite_step = sqlite3_step(stmt))\

#define kdata2_do_in_database_lock(d) \
sqlite3_mutex *mutex;\
for(mutex = sqlite3_db_mutex(d->db), sqlite3_mutex_enter(mutex); \
	mutex; \
	sqlite3_mutex_leave(mutex), mutex = NULL)

#define kdata2_table_for_each(d) \
struct kdata2_table **tables = NULL; \
struct kdata2_table *table = NULL; \
tables = d->tables; \
for (table = *tables++; table; table = *tables++)\

#define kdata2_column_for_each(table) \
struct kdata2_column **columns = NULL; \
struct kdata2_column *column = NULL; \
columns = table->columns; \
for (column = *columns++; column; column = *columns++)\

#ifdef __cplusplus
}  /* end of the 'extern "C"' block */
#endif

#endif /* ifndef KDATA2_H */
