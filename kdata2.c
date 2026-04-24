/**
 * File              : kdata2.c
 * Author            : Igor V. Sementsov <ig.kuzm@gmail.com>
 * Date              : 10.03.2023
 * Last Modified Date: 25.04.2026
 * Last Modified By  : Igor V. Sementsov <ig.kuzm@gmail.com>
 */

/*
 * this is data library to use with SQLite and YandexDisk sync
 */

#include "kdata2.h"
#include "uuid4.h"
#include "log.h"
#include "str.h"
#include "alloc.h"
#include <assert.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#ifdef _WIN32
#include <windows.h>
#ifndef bool
#define bool char
#define true 1
#define false 0
#endif
#else // not WIN32
#include <unistd.h>
#include <stdbool.h>
#endif // WIN32

#define ON_ERR(ptr, msg) \
	if (ptr->on_error) ptr->on_error(ptr->on_error_data, msg);
#define ON_LOG(ptr, msg) \
	if (ptr->on_log) ptr->on_log(ptr->on_log_data, msg);

struct kdata2_update {
	char table[128];
	char uuid[37];
	time_t timestamp;
	bool local;
	bool deleted;
	kdata2_t *d;
	void *data_to_free;
	char column[128];
};

int uuid_new(char *uuid){
	uuid4_init();
	uuid4_generate(uuid);

	return 0;
}

int kdata2_sqlite3_exec(
		kdata2_t *d, const char *sql)
{
	int ret = 0;
	char *errmsg = NULL;

	kdata2_do_in_database_lock(d)
	{
		ON_LOG(d, sql);
		sqlite3_exec(d->db, sql, NULL, NULL, &errmsg);
		if (errmsg){
			ON_ERR(d, 
				   STR("sqlite3_exec: %s: %s", sql, errmsg));		
			sqlite3_free(errmsg);	
			ret = -1;
		}
	}

	return ret;
}

int kdata2_sqlite3_prepare(
		kdata2_t *d, const char *sql, sqlite3_stmt **stmt)
{
	int res;
	char *errmsg = NULL;
	ON_LOG(d, sql);

	res = sqlite3_prepare_v2(d->db, sql, -1, stmt, NULL);
	if (res != SQLITE_OK) {
		ON_ERR(d,
				STR("sqlite3_prepare: %s: %s", 
					sql, sqlite3_errmsg(d->db)));		
		return -1;
	}	
	return 0;
}

char * kdata2_sql_select_table_request(
		kdata2_t *d, const char *tablename)
{
	struct str s;
	if (str_init(&s))
		return NULL;
	do {
		kdata2_table_for_each(d) {
			if (strcmp(tablename, table->tablename) == 0)
			{
				str_appendf(&s, "SELECT %s, ", UUIDCOLUMN);
				do {
					kdata2_column_for_each(table) {
						str_appendf(&s, "%s, ", column->columnname);
					}
				} while(0);
				str_appendf(&s, "timestamp FROM '%s' ", 
						table->tablename);
				return 0;
			}
	 }
	} while(0);

	ON_ERR(d, STR("No table with name: %s", tablename));		
	return s.str;
}

int kdata2_init(
		kdata2_t ** database,
		const char * filepath,
		void          *on_error_data,
		void         (*on_error)      (void *on_error_data, const char *error),
		void          *on_log_data,
		void         (*on_log)        (void *on_log_data, const char *message),		
		...
		)
{
	int err = 0, tcount = 0;
	char *errmsg = NULL;
	kdata2_t *d;
	va_list args;
	struct kdata2_table * table, **tables;

	char SQL_updates[] = 
		"CREATE TABLE IF NOT EXISTS "
		"_kdata2_updates "
		"( "
		"tablename TEXT, "
		"uuid TEXT, "
		"timestamp INT, "
		"local INT, "
		"deleted INT "
		");"
		;	

	if (on_log)
		on_log(on_log_data, "init...");	

	/* For init:
	 * 1. Create (if not exists) loacal database 
	 * 2. Create (if not exists) tables - add uuid and timestamp columns
	 * 3. Create (if not exists) database in Yandex Disk
	 * 4. Allocate data to transfer trough new tread
	 * 5. Start Yandex Disk daemon in thread */

	
	/* check filepath */
	if (!filepath){
		if (on_error)
			on_error(on_error_data, "filepath is NULL");	
		return -1;
	}

	/* check database pointer */
	if (!database){
		if (on_error)
			on_error(on_error_data, "database pointer is NULL");	
		return -1;
	}
	/* allocate kdata2_t */
	d = NEW(kdata2_t);
	if (d == NULL){
		if (on_error)
			on_error(on_error_data, "can't allocate kdata2_t");			
		return -1;
	};
	*database = d;
	
	/* set callbacks to NULL */
	d->on_error_data = on_error_data;
	d->on_error      = on_error;
	d->on_log_data   = on_log_data;
	d->on_log        = on_log;

	strncpy(d->filepath, filepath, BUFSIZ-1);
	d->filepath[BUFSIZ-1] = 0;
	
	/* init SQLIte database */
	/* create database if needed */
	ON_LOG(d, STR("sqlite3_open: %s", d->filepath));	
	/*err = sqlite3_open_v2(*/
			/*d->filepath, */
			/*&(d->db), */
			/*SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, */
			/*NULL);*/
	err = sqlite3_open(
			d->filepath, 
			&(d->db));
	if (err){
		ON_ERR(d,  
			STR("failed to open/create database at path: '%s': %s", 
						d->filepath, sqlite3_errmsg(d->db)));		
		return err;
	} 

	/* allocate and fill tables array */
	d->tables = MALLOC(sizeof(char*));
	if (d->tables == NULL){
		ON_ERR(d, "can't allocate kdata2_t_table");		
		return -1;
	}

	//init va_args
	va_start(args, on_log);

	table = va_arg(args, struct kdata2_table *);
	if (!table)
		return -1;

	//iterate va_args
	while (table){
		void *p;

		// add table to tables
		d->tables[tcount++] = table;
		
		// realloc tables
		p = realloc(d->tables, tcount * 8 + 8);
		if (!p) {
			ON_ERR(d,		
				STR("can't realloc tables with size: %d", 
					tcount * 8 + 8));			
			break;
		}
		d->tables = p;

		// iterate
		table = va_arg(args, struct kdata2_table *);
	}	
	/* NULL-terminate tables array */
	d->tables[tcount] = NULL;

	/* fill SQL string with data and update tables in memory*/
	tables = d->tables; // pointer to iterate
	while (*tables) {

		/* create SQL string */
		char SQL[BUFSIZ] = "";

		/* for each table in dataset */
		struct kdata2_table *table = *tables++;

		struct kdata2_column ** col_ptr;
		
		/* check if columns exists */
		if (!table->columns)
			continue;

		/* check if name exists */
		if (table->tablename[0] == 0)
			continue;

		/* create SQL string */
		sprintf(SQL, 
				"CREATE TABLE IF NOT EXISTS '%s' (id INT);"
								, table->tablename);
		kdata2_sqlite3_exec(d, SQL);

		sprintf(SQL, 
							"ALTER TABLE '%s' "
											"ADD COLUMN '%s' TEXT;", 
							table->tablename, UUIDCOLUMN);
		kdata2_sqlite3_exec(d, SQL);

		sprintf(SQL, 
							"ALTER TABLE '%s' "
											"ADD COLUMN 'timestamp' INT;", 
							table->tablename);
		kdata2_sqlite3_exec(d, SQL);

		col_ptr = table->columns; // pointer to iterate
		while (*col_ptr) {
			/* for each column in table */
			struct kdata2_column *col = *col_ptr++;

			/* check if name exists */
			if (col->columnname[0] == 0)
				continue;

			/* each table should have uuid column and timestamp column */
			/* check if column name is uuid or timestamp */
			if (strcmp(col->columnname, UUIDCOLUMN) == 0 
					|| strcmp(col->columnname, "timestamp") == 0)
				continue;

			/* create SQL string */
			switch (col->type) {
				case KDATA2_TYPE_NULL:
					break;
				case KDATA2_TYPE_NUMBER:
					sprintf(SQL, 
							"ALTER TABLE '%s' "
											"ADD COLUMN '%s' INT;", 
							table->tablename, col->columnname);
					kdata2_sqlite3_exec(d, SQL);
					//sqlite3_exec(d->db, SQL, NULL, NULL, NULL);
					break;
				case KDATA2_TYPE_TEXT:
					sprintf(SQL, 
							"ALTER TABLE '%s' "
											"ADD COLUMN '%s' TEXT;", 
							table->tablename, col->columnname);
					kdata2_sqlite3_exec(d, SQL);
					//sqlite3_exec(d->db, SQL, NULL, NULL, NULL);
					break;
				case KDATA2_TYPE_DATA:
					sprintf(SQL, 
							"ALTER TABLE '%s' "
											"ADD COLUMN '%s' BLOB;", 
							table->tablename, col->columnname);
					kdata2_sqlite3_exec(d, SQL);
					//sqlite3_exec(d->db, SQL, NULL, NULL, NULL);
					break;
				case KDATA2_TYPE_FLOAT:
					sprintf(SQL, 
							"ALTER TABLE '%s' "
											"ADD COLUMN '%s' REAL;", 
							table->tablename, col->columnname);
					kdata2_sqlite3_exec(d, SQL);
					//sqlite3_exec(d->db, SQL, NULL, NULL, NULL);
					break;
			}
		}
	}

	/* create table to store updates */
	/* run SQL command */
	ON_LOG(d, SQL_updates);
	kdata2_sqlite3_exec(d, SQL_updates);

	return 0;
}

char *
kdata2_set_number_for_uuid(
		kdata2_t *d, 
		const char *tablename, 
		const char *column, 
		long number, 
		const char *uuid)
{
	time_t timestamp = time(NULL);
	char SQL[BUFSIZ];

	if (!d)
		return NULL;

	if (!uuid){
		char *_uuid = malloc(37);
		if (!_uuid) return NULL;
		if (uuid_new(_uuid)){
			ON_ERR(d, "can't generate uuid");			
			return NULL;
		}
		uuid = _uuid;
	}


	/* update database */
	snprintf(SQL, BUFSIZ-1,
			"INSERT INTO '%s' (%s) "
			"SELECT '%s' "
			"WHERE NOT EXISTS (SELECT 1 FROM '%s' WHERE %s = '%s'); "
			"UPDATE '%s' SET timestamp = %ld, '%s' = %ld WHERE %s = '%s'"
			,
			tablename, UUIDCOLUMN,
			uuid,
			tablename, UUIDCOLUMN, uuid,
			tablename, timestamp, column, number, UUIDCOLUMN, uuid		
	);
	ON_LOG(d, SQL);
	if (kdata2_sqlite3_exec(d, SQL))
		return NULL;

	/* update update table */
	snprintf(SQL, BUFSIZ-1,
			"INSERT INTO _kdata2_updates (uuid) "
			"SELECT '%s' "
			"WHERE NOT EXISTS (SELECT 1 FROM _kdata2_updates WHERE uuid = '%s'); "
			"UPDATE _kdata2_updates SET timestamp = %ld, tablename = '%s', deleted = 0 WHERE uuid = '%s'"
			,
			uuid,
			uuid,
			timestamp, tablename, uuid		
	);
	ON_LOG(d, SQL);
	if (kdata2_sqlite3_exec(d, SQL))
		return NULL;

	return (char *)uuid;
}

char * kdata2_set_float_for_uuid(
		kdata2_t * d, 
		const char *tablename, 
		const char *column, 
		double number, 
		const char *uuid)
{
	time_t timestamp = time(NULL);
	char SQL[BUFSIZ];

	if (!d)
		return NULL;

	if (!uuid){
		char *_uuid = malloc(37);
		if (!_uuid) return NULL;
		if (uuid_new(_uuid)){
			ON_ERR(d, "can't generate uuid");			
			return NULL;
		}
		uuid = _uuid;
	}


	/* update database */
	snprintf(SQL, BUFSIZ-1,
			"INSERT INTO '%s' (%s) "
			"SELECT '%s' "
			"WHERE NOT EXISTS (SELECT 1 FROM '%s' WHERE %s = '%s'); "
			"UPDATE '%s' SET timestamp = %ld, '%s' = %lf WHERE %s = '%s'"
			,
			tablename, UUIDCOLUMN,
			uuid,
			tablename, UUIDCOLUMN, uuid,
			tablename, timestamp, column, number, UUIDCOLUMN, uuid		
	);
	ON_LOG(d, SQL);
	if (kdata2_sqlite3_exec(d, SQL))
		return NULL;

	/* update update table */
	snprintf(SQL, BUFSIZ-1,
			"INSERT INTO _kdata2_updates (uuid) "
			"SELECT '%s' "
			"WHERE NOT EXISTS (SELECT 1 FROM _kdata2_updates WHERE uuid = '%s'); "
			"UPDATE _kdata2_updates SET timestamp = %ld, tablename = '%s', deleted = 0 WHERE uuid = '%s'"
			,
			uuid,
			uuid,
			timestamp, tablename, uuid		
	);
	ON_LOG(d, SQL);
	if (kdata2_sqlite3_exec(d, SQL))
		return NULL;

	return (char *)uuid;
}

char * kdata2_set_text_for_uuid(
		kdata2_t *d, 
		const char *tablename, 
		const char *column, 
		const char *text, 
		const char *uuid)
{
	size_t text_len;
	time_t timestamp = time(NULL);
	char *SQL;

	if (!d)
		return NULL;

	if (!uuid){
		char *_uuid = malloc(37);
		if (!_uuid) return NULL;
		if (uuid_new(_uuid)){
			ON_ERR(d, "can't generate uuid");			
			return NULL;
		}
		uuid = _uuid;
	}

	text_len = strlen(text);
	SQL = malloc(text_len + BUFSIZ);
	if (SQL == NULL){
		ON_ERR(d, "malloc");
		return NULL;
	}

	/* update database */
	sprintf(SQL,
			"INSERT INTO '%s' (%s) "
			"SELECT '%s' "
			"WHERE NOT EXISTS (SELECT 1 FROM '%s' WHERE %s = '%s'); "
			"UPDATE '%s' SET timestamp = %ld, '%s' = '%s' WHERE %s = '%s'"
			,
			tablename, UUIDCOLUMN,
			uuid,
			tablename, UUIDCOLUMN, uuid,
			tablename, timestamp, column, text, UUIDCOLUMN, uuid		
	);
	ON_LOG(d, SQL);
	if (kdata2_sqlite3_exec(d, SQL))
		return NULL;

	/* update update table */
	snprintf(SQL, BUFSIZ-1,
			"INSERT INTO _kdata2_updates (uuid) "
			"SELECT '%s' "
			"WHERE NOT EXISTS (SELECT 1 FROM _kdata2_updates WHERE uuid = '%s'); "
			"UPDATE _kdata2_updates SET timestamp = %ld, tablename = '%s', deleted = 0 WHERE uuid = '%s'"
			,
			uuid,
			uuid,
			timestamp, tablename, uuid		
	);
	ON_LOG(d, SQL);
	if (kdata2_sqlite3_exec(d, SQL))
		return NULL;
	
	return (char *)uuid;
}

char * kdata2_set_data_for_uuid(
		kdata2_t *d, 
		const char *tablename, 
		const char *column, 
		void *data, 
		int len,
		const char *uuid)
{
	time_t timestamp = time(NULL);
	char SQL[BUFSIZ];
	sqlite3_stmt *stmt;
	int res;

	if (!d)
		return NULL;

	if (!uuid){
		char *_uuid = malloc(37);
		if (!_uuid) return NULL;
		if (uuid_new(_uuid)){
			ON_ERR(d, "can't generate uuid");			
			return NULL;
		}
		uuid = _uuid;
	}
	if (!data || !len){
		ON_ERR(d, "no data");			
		return NULL;
	}	


	/* start SQLite request */
	snprintf(SQL, BUFSIZ-1,
			"INSERT INTO '%s' (%s) "
			"SELECT '%s' "
			"WHERE NOT EXISTS (SELECT 1 FROM '%s' WHERE %s = '%s'); ",
			tablename, UUIDCOLUMN,
			uuid,
			tablename, UUIDCOLUMN, uuid
	);	
	ON_LOG(d, SQL);
	if (kdata2_sqlite3_exec(d, SQL))
		return NULL;
	
	sprintf(SQL, "UPDATE '%s' SET '%s' = (?) WHERE %s = '%s'", tablename, column, UUIDCOLUMN, uuid);
	
	/*if (kdata2_sqlite3_prepare_v2(d, SQL, &stmt))*/
	if (kdata2_sqlite3_prepare(d, SQL, &stmt))
		return NULL;

	res = sqlite3_bind_blob(stmt, 1, data, len, SQLITE_TRANSIENT);
	if (res != SQLITE_OK) {
		ON_ERR(d, STR("sqlite3_bind_blob: %s", 
					sqlite3_errmsg(d->db)));		
	}	

	if((res = sqlite3_step(stmt)) != SQLITE_DONE){
		ON_ERR(d, STR("sqlite3_step: %s", 
					sqlite3_errmsg(d->db)));		
		sqlite3_reset(stmt);
		// ??
	} 

	res = sqlite3_reset(stmt);	
	
	/* update update table */
	snprintf(SQL, BUFSIZ-1,
			"INSERT INTO _kdata2_updates (uuid) "
			"SELECT '%s' "
			"WHERE NOT EXISTS (SELECT 1 FROM _kdata2_updates WHERE uuid = '%s'); "
			"UPDATE _kdata2_updates SET timestamp = %ld, tablename = '%s', deleted = 0 WHERE uuid = '%s'"
			,
			uuid,
			uuid,
			timestamp, tablename, uuid		
	);
	ON_LOG(d, SQL);
	if (kdata2_sqlite3_exec(d, SQL))
		return NULL;

	return (char *)uuid;
}

int kdata2_remove_for_uuid(
		kdata2_t *d, 
		const char *tablename, 
		const char *uuid)
{
	char SQL[BUFSIZ];

	if (!d)
		return -1;

	if (!uuid){
		ON_ERR(d, "no uuid");
		return -1;
	}	
	
	snprintf(SQL, BUFSIZ-1,
			"DELETE FROM '%s' WHERE %s = '%s'", 
			tablename, UUIDCOLUMN, uuid);	
	ON_LOG(d, SQL);
	if (kdata2_sqlite3_exec(d, SQL))
		return -1;

	/* update update table */
	snprintf(SQL, BUFSIZ-1,
			"INSERT INTO _kdata2_updates (uuid) "
			"SELECT '%s' "
			"WHERE NOT EXISTS (SELECT 1 FROM _kdata2_updates WHERE uuid = '%s'); "
			"UPDATE _kdata2_updates SET timestamp = %ld, tablename = '%s', deleted = 1 WHERE uuid = '%s'"
			,
			uuid,
			uuid,
			time(NULL), tablename, uuid		
	);
	ON_LOG(d, SQL);
	if (kdata2_sqlite3_exec(d, SQL))
		return -1;
	
	return 0;
}

char * kdata2_get_string(
		kdata2_t *d, 
		const char *SQL)
{
	char *errmsg = NULL;
	sqlite3_stmt *stmt;
	const char *str, *ret_str;

	if (!d)
		return NULL;

	if (!SQL){
		ON_ERR(d, "SQL is NULL");
		return NULL;
	}

	/*if (kdata2_sqlite3_prepare_v2(d, SQL, &stmt))*/
	if (kdata2_sqlite3_prepare(d, SQL, &stmt))
		return NULL;

	// get first value
	sqlite3_step(stmt);
	str = (const char *)
		sqlite3_column_text(stmt, 0);
	if (!str)
		return NULL;
	
	ret_str = strdup(str);
	sqlite3_finalize(stmt);
	return (char *)ret_str;
}	

void kdata2_get(
		kdata2_t *d, 
		const char *SQL, 
		void *user_data,
		int (*callback)(
			void *user_data,
			int num_cols,
			enum KDATA2_TYPE types[],
			const char *columns[], 
			void *values[],
			size_t sizes[]
			)
		)
{
	sqlite3_stmt *stmt;
	int num_cols;

	if (!d)
		return;
	
	if (!SQL){
		ON_ERR(d, "SQL is NULL");
		return;
	}
	
	if (!callback){
		ON_ERR(d, "callback is NULL");
		return;
	}

	/* start SQLite request */
	if (kdata2_sqlite3_prepare(d, SQL, &stmt))
		return;

	num_cols = sqlite3_column_count(stmt); //number of colums
	
	while (sqlite3_step(stmt) != SQLITE_DONE) {
		enum KDATA2_TYPE *types = MALLOC(num_cols*sizeof(enum KDATA2_TYPE));
		const char **columns = MALLOC(num_cols * sizeof(char *));
		void **values = MALLOC(num_cols*sizeof(void *));
		size_t *sizes = MALLOC(num_cols*sizeof(size_t));
		int i;

		/* iterate columns */
		for (i = 0; i < num_cols; ++i) {
			/* get datatype */
			enum KDATA2_TYPE type;
			
			int col_type = sqlite3_column_type(stmt, i);
			switch (col_type) {
				case SQLITE_BLOB:    type = KDATA2_TYPE_DATA;   break;
				case SQLITE_INTEGER: type = KDATA2_TYPE_NUMBER; break;
				case SQLITE_FLOAT:   type = KDATA2_TYPE_FLOAT;  break;
				
				default: type = KDATA2_TYPE_TEXT; break;
			}
			types[i] = type;

			/* get title */
			columns[i] = sqlite3_column_name(stmt, i);

			/* switch data types */
			switch (type) {
				case KDATA2_TYPE_NUMBER: {
					long *number = MALLOC(sizeof(long));
					number[0] = sqlite3_column_int64(stmt, i);
					values[i] = number;	
					sizes[i] = 1;	
					break;							 
				} 
				case KDATA2_TYPE_FLOAT: {
					double *number = MALLOC(sizeof(double));
					number[0] = sqlite3_column_double(stmt, i);
					values[i] = number;	
					sizes[i] = 1;	
					break;							 
				}										 
				case KDATA2_TYPE_TEXT: {
					size_t len = sqlite3_column_bytes(stmt, i); 
					values[i] = (void *)sqlite3_column_text(stmt, i);
					sizes[i] = len;	
					break;							 
				} 
				case KDATA2_TYPE_DATA: {
					size_t len = sqlite3_column_bytes(stmt, i); 
					values[i] = (void *)sqlite3_column_blob(stmt, i);
					sizes[i] = len;	
					break;							 
				
				default:
					break;
				} 
			}
		}
		
		// do callback
		if (callback){
			if (callback(user_data, num_cols, types, columns, values, sizes))
			{
			   sqlite3_finalize(stmt);
			   return;
			}
		}
		
		free(types);
		free(columns);
		for (i = 0; i < num_cols; ++i) {
			if (types[i] == KDATA2_TYPE_NUMBER || types[i] == KDATA2_TYPE_FLOAT)
				free(values[i]);
		}
		free(values);
		free(sizes);
	}

	sqlite3_finalize(stmt);
}

int kdata2_close(kdata2_t *d){
	if (!d)
		return -1;

	if (d->db)
		sqlite3_close(d->db);

	//free(d);
	return 0;
}

int kdata2_table_init(struct kdata2_table **t, const char * tablename, ...){
	struct kdata2_column **columns;
	va_list args;
	enum KDATA2_TYPE type;
	char * columnname;
	int i = 0;

	// check table pointer
	if (!t){
		return -1;
	}
	
	/* allocate new table */
	*t = NEW(struct kdata2_table);
	if (t == NULL)
		return -1;
	
	// pointer to collumns
	columns = malloc(8);
	if (!columns){
		return -1;
	}

	/* set tables attributes */
	strncpy(t[0]->tablename, tablename, sizeof(t[0]->tablename));
	t[0]->columns = NULL;
	
	//init va_args
	va_start(args, tablename);

	type = va_arg(args, enum KDATA2_TYPE);
	if (type == KDATA2_TYPE_NULL)
		return -1;

	columnname = va_arg(args, char *);
	if (!columnname)
		return -1;

	//iterate va_args
	while (type != KDATA2_TYPE_NULL && columnname != NULL){
		void *p;

		/* allocate new column */
		struct kdata2_column *new = NEW(struct kdata2_column);
		if (new == NULL)	
			break;

		/* set column attributes */
		strncpy(new->columnname, columnname, sizeof(new->columnname));
		new->type = type;

		/* add column to array */
		columns[i++] = new;

		//realloc columns array
		p = realloc(columns, i * 8 + 8);
		if (!p){
			break;
		}		
		columns = p;

		/* iterate va_args */
		type = va_arg(args, enum KDATA2_TYPE);
		columnname = va_arg(args, char *);
		if (!columnname) 
			break;
		
	}
	/* NULL-terminate array */
	columns[i] = NULL;

	t[0]->columns = columns;
	
	return 0;
}
