/**
 * File              : kdata2.h
 * Author            : Igor V. Sementsov <ig.kuzm@gmail.com>
 * Date              : 10.03.2023
 * Last Modified Date: 14.03.2023
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
#include "cYandexDisk/cYandexDisk.h"
#include "cYandexDisk/cJSON.h"
#include "cYandexDisk/uuid4/uuid4.h"
#include "base64.h"


#define NEW(T) ({T *new = malloc(sizeof(T)); new;})
#define STR(...) ({char str[BUFSIZ]; sprintf(str, __VA_ARGS__); str;})

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

/* this is data table */
typedef struct kdata2_tab {
	char name[128];            // name of table
	kdata2_col_t ** columns;   // NULL-terminated array of columns
} kdata2_tab_t;

/* this is dataset */
typedef struct kdata2 {
	char * filepath;           // file path to where store SQLite data 	
	char * access_token;       // Yandex Disk access token
	kdata2_tab_t ** tables;    // NULL-terminated array of tables
	int sec;				   // number of seconds of delay to sinc data with Yandex Disk
	void * user_data;          // data to transfer trough callback
	int (*callback)(void * user_data, char * msg); // yandex disk daemon
												   // callback	
} kdata2_t;

/* init function */
static int kdata2_init(kdata2_t * dataset);

/* set number for row with uuid; set uuid to NULL to create new */
static int kdata2_set_number_for_uuid(
		const char *filepath, 
		const char *tablename, 
		const char *column, 
		long number, 
		const char *uuid);

/* set text for row with uuid; set uuid to NULL to create new */
static int kdata2_set_text_for_uuid(
		const char *filepath, 
		const char *tablename, 
		const char *column, 
		const char *text, 
		const char *uuid);

/* set data for row with uuid; set uuid to NULL to create new */
static int kdata2_set_data_for_uuid(
		const char *filepath, 
		const char *tablename, 
		const char *column, 
		void *data, 
		int len,
		const char *uuid);

/* remove row with uuid */
static int kdata2_remove_for_uuid(
		const char *filepath, 
		const char *tablename, 
		const char *uuid);

/* get rows for table; set predicate to "WHERE uuid = 'uuid' to get row with uuid" */
static void kdata2_get(
		const char *filepath, 
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

/* IMP */

static void _uuid_new(char *uuid){
	//create uuid
	UUID4_STATE_T state; UUID4_T identifier;
	uuid4_seed(&state);
	uuid4_gen(&state, &identifier);
	if (!uuid4_to_s(identifier, uuid, 37)){
		perror("can't generate UUID\n");
		return;
	}
}

static void _yd_free_data(kdata2_t * data){
	kdata2_tab_t ** tab_ptr = data->tables; // pointer to iterate
	while (tab_ptr) {
		/* for each column in table */
		kdata2_tab_t **tab = tab_ptr++;		

		kdata2_col_t ** col_ptr = tab[0]->columns; // pointer to iterate
		while (col_ptr) {
			/* for each column in table */
			kdata2_col_t **col = col_ptr++;
			free(col[0]);
		}	
		
		free(tab[0]->columns);
		free(tab[0]);
	}	
	free(data->tables);
	free(data->filepath);
	free(data->access_token);
	free(data);
}

struct kdata2_update {
	char table[128];
	char uuid[37];
	time_t timestamp;
	bool local;
	bool deleted;
	kdata2_t *d;
	cJSON *columns;
	void *data_to_free;
	char column[128];
};

struct list {
	void *data;
	struct list *prev;
};

static int _remove_local_update(void *user_data, char *error){
	if (error){
		perror(error);
		return 0;
	}
	struct kdata2_update *update = user_data;
	/* remove from local update table */
	sqlite_connect_execute(
			STR("DELETE FROM _kdata2_updates WHERE uuid = '%s'", update->uuid), 
					update->d->filepath);	

	/* set new timestamp for data */
	sqlite_connect_execute(
			STR("UPDATE '%s' SET timestamp = %ld WHERE uuid = '%s'", 
					update->table, update->timestamp, update->uuid), 
							update->d->filepath);	
	return 0;
}

static int _ufter_upload_to_YandexDisk(size_t size, void *user_data, char *error){
	if (error)
		perror(error);

	struct kdata2_update *update = user_data;

	/* free data */
	free(update->data_to_free);

	/* get uploaded file*/
	c_yd_file_t file;
	c_yandex_disk_file_info(update->d->access_token, 
			STR("app:/database/%s", update->uuid), &file, NULL);

	if (!file.name[0]) {
		perror("can't get file in _ufter_upload_to_YandexDisk\n");	
		return 1;
	}

	/* set timestamp */
	update->timestamp = file.modified;

	/* remove local update and sync timestamps */
	_remove_local_update(update, NULL);

	return 0;
}

static int 
_fill_json_with_SQLite_data(void *user_data, int argc, char **argv, char **titles){

	struct kdata2_update *update = user_data;
	
	/* get table */
	kdata2_tab_t *table = NULL;
	kdata2_tab_t **p = update->d->tables;
	while (p){
		kdata2_tab_t **tab = p++;
		char *tablename = tab[0]->name;
		if (strcmp(tablename, update->table) == 0)
			table = *tab;
	}
	if (!table){
		perror("can't find table for update structure in _fill_json_with_SQLite_data\n");
		return 0;
	}

	/* get data for each columns */
	int i;
	for (i = 0; i < argc; ++i) {
		/* drop for uuid and timestamp columns */
		if (titles[i]){
			if (strcmp(titles[i], "uuid")
					|| strcmp(titles[i], "timestamp"))
				continue;
		}
		/* buffer overload safe get data */
		char buf[BUFSIZ] = "";
		if (argv[i]){
			strncpy(buf, argv[i], BUFSIZ-1);
			buf[BUFSIZ-1] = 0;
		}

		kdata2_col_t *col = table->columns[i]; 
		if (!col){
			perror("cant get column in _fill_json_with_SQLite_data\n");
			return 0;
		}

		/* fill json with data */
		cJSON *column = cJSON_CreateObject();
		if (!column){
			perror("cJSON_CreateObject() json\n");
			return 0;
		}
		cJSON_AddItemToObject(column, "name", cJSON_CreateString(col->name));
		cJSON_AddItemToObject(column, "type", cJSON_CreateNumber(col->type));
		
		if (col->type == KDATA2_TYPE_DATA){
			/* for datatype data do upload data and add uuid to json */
			char data_id[37+128];
			sprintf(data_id, "%s_%s",update->uuid, col->name);	
			
			/* set data uuid to json */
			cJSON_AddItemToObject(column, "data", cJSON_CreateString(data_id));

			/* allocate data to upload */
			int len = strlen(argv[i]);
			char *data = malloc(len + 1);
			if (!data){
				perror("malloc data\n");
				return 0;
			}

			/* copy data */
			strncpy(data, argv[i], len);

			update->data_to_free = data;

			/* upload data to YandexDisk */
			c_yandex_disk_upload_data(update->d->access_token, data, len + 1, 
					STR("app:/data/%s", data_id), true, false, 
							update, _ufter_upload_to_YandexDisk, NULL, NULL);

		} else {
			/* add value to json */
			if (col->type == KDATA2_TYPE_TEXT)
				cJSON_AddItemToObject(column, "value", cJSON_CreateString(buf));
			else
				cJSON_AddItemToObject(column, "value", cJSON_CreateNumber(atoi(buf)));
		}
		
		cJSON_AddItemToArray(update->columns, column);
	}

	return 0;
}

static void _upload_local_data_to_Yandex_Disk(struct kdata2_update *update){
	/*
	 * 1. get data from SQLite
	 * 2. create JSON
	 * 3. upload JSON and data
	 * 4. remove uuid from local update table
	 */

	/* create json */
	cJSON *json = cJSON_CreateObject();
	if (!json){
		perror("cJSON_CreateObject() json\n");
		return;
	}
	cJSON_AddItemToObject(json, "tablename", cJSON_CreateString(update->table));
	cJSON *columns = cJSON_CreateArray();
	if (!json){
		perror("cJSON_CreateArray() columns\n");
		return;
	}	

	/* set columns to transfer through callback */
	update->columns = columns;

	/*fill json with columns data */
	sqlite_connect_execute_function(
			STR("SELECT * FROM '%s' WHERE uuid = '%s'", update->table, update->uuid), 
					update->d->filepath, update, _fill_json_with_SQLite_data);

	/* add columns to json */
	cJSON_AddItemToObject(json, "columns", columns);

	/* upload json and remove uuid from update table */
	char *data = cJSON_Print(json);
	update->data_to_free = data;
	c_yandex_disk_upload_data(update->d->access_token, data, strlen(data) + 1, 
			STR("app:/database/%s", update->uuid), false, false, 
					update, _ufter_upload_to_YandexDisk, NULL, NULL);	

	/* free json */
	cJSON_free(json);
}

static int 
_for_each_update_in_SQLite(void *user_data, int argc, char **argv, char **titles){

	kdata2_t *d = user_data;

	/* new kdata_update */
	struct kdata2_update update;

	int i;
	for (i = 0; i < argc; ++i) {
		/* buffer overload safe get data */
		char buf[BUFSIZ] = "";
		if (argv[i]){
			strncpy(buf, argv[i], BUFSIZ-1);
			buf[BUFSIZ-1] = 0;
		}
		/* fill update with data */
		switch (i) {
			case 0: strcpy (update.table, buf)     ; break;
			case 1: strncpy(update.uuid,  buf, 36) ; update.uuid[36] = 0; break;
			case 2: update.timestamp = atol(buf)   ; break;
			case 3: update.local = atoi(buf)       ; break;
			case 4: update.deleted = atoi(buf)     ; break;
		}
	}

	/* set data to use in callback */
	update.d = d;
	
	/* path to remote data */
	char *filepath = STR("app:/database/%s/%s", update.table, update.uuid);

	/* if local update is deleted -> move remote data to deleted */
	if (update.deleted) {
		c_yandex_disk_mv(d->access_token, filepath, 
				STR("app:/deleted/%s/%s", update.table, update.uuid), true, 
						&update, _remove_local_update);

		return 0;
	}

	/* check remote data exists */
	c_yd_file_t remote;
	c_yandex_disk_file_info(d->access_token, filepath, &remote, NULL);
	if (remote.name[0] != 0) {
		/* check timestamp */
		if (update.timestamp > remote.modified) {
			/* upload local data to Yandex Disk */
			_upload_local_data_to_Yandex_Disk(&update);
		} else {
			/* no need to upload data, remove from local update table */
			_remove_local_update(&update, NULL);
		}
		
	} else {
		/* no remote data -> upload local data to Yandex Disk */
		_upload_local_data_to_Yandex_Disk(&update);
	}	

	return 0;
}

static int 
_download_data_from_YandexDisk_to_local_database_cb(size_t size, void *data, void *user_data, char *error){
	/* handle error */
	if (error)
		perror(error);

	struct kdata2_update *update = user_data;	

	/* allocate SQL string */
	char *SQL = malloc(size + BUFSIZ);
	if (!SQL){
		perror("SQL string malloc");
		return 1;
	}

	/* update local database */
	snprintf(SQL, size + BUFSIZ-1,
			"UPDATE '%s' SET '%s' = '%s' WHERE uuid = '%s'", update->table, 
					update->column, 
							(char*)data, update->uuid		
	);
	sqlite_connect_execute(SQL, update->d->filepath);	

	/* free SQL string */
	free(SQL);
	
	return 0;
}

static int 
_download_json_from_YandexDisk_to_local_database_cb(size_t size, void *data, void *user_data, char *error){
	/* handle error */
	if (error)
		perror(error);

	struct kdata2_update *update = user_data;

	/* data is json file */
	cJSON *json = cJSON_ParseWithLength(data, size);
	if (!json){
		perror("can't parse json file\n");
		free(update);
		return 1;
	}

	/* get tablename */
	cJSON *tablename = cJSON_GetObjectItem(json, "tablename");
	if (!tablename){
		perror("can't get tablename from json file\n");
		cJSON_free(json);
		free(update);
		return 1;
	}

	strncpy(update->table, cJSON_GetStringValue(tablename), 127);
	update->table[127] = 0;

	/* get columns */
	cJSON *columns = cJSON_GetObjectItem(json, "columns");
	if (!columns || !cJSON_IsArray(columns)){
		perror("can't get columns from json file\n");
		cJSON_free(json);
		free(update);
		return 1;
	}
	
	/* udate local database */
	char SQL[BUFSIZ];
	snprintf(SQL, BUFSIZ-1,
			"INSERT INTO '%s' (uuid) "
			"SELECT '%s' "
			"WHERE NOT EXISTS (SELECT 1 FROM '%s' WHERE uuid = '%s'); "
			"UPDATE '%s' SET timestamp = %ld WHERE uuid = '%s'"
			,
			update->table,
			update->uuid,
			update->table, update->uuid,
			update->table, update->timestamp, update->uuid		
	);
	sqlite_connect_execute(SQL, update->d->filepath);	

	/* get values for each column and update local database */
	cJSON *column = NULL;
	cJSON_ArrayForEach(column, columns){
		cJSON *name = cJSON_GetObjectItem(column, "name");
		if (!name || !cJSON_IsString(name)){
			perror("can't get column name from json file\n");
			continue;
		}		
		cJSON *type = cJSON_GetObjectItem(column, "type");
		if (!type || !cJSON_IsNumber(type)){
			perror("can't get column type from json file\n");
			continue;
		}		
		if (cJSON_GetNumberValue(type) == KDATA2_TYPE_DATA) {
			/* download data from Yandex Disk */
			strncpy(update->column, cJSON_GetStringValue(name), 127);
			update->column[127] = 0;

			cJSON *data = cJSON_GetObjectItem(column, "data");
			if (!data){
				perror("can't get column data from json file\n");
				continue;
			}			
			c_yandex_disk_download_data(update->d->access_token, 
					STR("app:/data/%s", cJSON_GetStringValue(data)), true, 
							update, _download_data_from_YandexDisk_to_local_database_cb, 
									NULL, NULL);	
		} else {
			cJSON *value = cJSON_GetObjectItem(column, "value");
			if (!value){
				perror("can't get column value from json file\n");
				continue;
			}		
			if (cJSON_GetNumberValue(type) == KDATA2_TYPE_NUMBER){
				snprintf(SQL, BUFSIZ-1,
						"UPDATE '%s' SET '%s' = %ld WHERE uuid = '%s'", update->table, 
								cJSON_GetStringValue(name), 
										(long)cJSON_GetNumberValue(value), update->uuid		
				);
				sqlite_connect_execute(SQL, update->d->filepath);	
			} else {
				snprintf(SQL, BUFSIZ-1,
						"UPDATE '%s' SET '%s' = '%s' WHERE uuid = '%s'", update->table, 
								cJSON_GetStringValue(name), 
										cJSON_GetStringValue(value), update->uuid		
				);				
				sqlite_connect_execute(SQL, update->d->filepath);	
			}
		}
	}

	/* free json */
	cJSON_free(json);

	/* free update */
	free(update);

	return 0;
} 


static void
_download_data_from_YandexDisk_to_local_database(kdata2_t * d, c_yd_file_t *file){
	/*
	 * 1. get json data
	 * 2. update local data for number and text datatype
	 * 3. download and update data for datatype data
	 */

	/* allocate struct and fill update */
	struct kdata2_update *update = NEW(struct kdata2_update);
	if (!update){
		perror("struct kdata2_update malloc");
		return;
	}
	update->d = d;
	
	strncpy(update->uuid, file->name, 36); 
	update->uuid[36] = 0;
	
	update->timestamp = file->modified;

	/* download data */
	c_yandex_disk_download_data(d->access_token, file->path, false, 
			update, _download_json_from_YandexDisk_to_local_database_cb, NULL, NULL);	

}

static int 
_for_each_file_in_YandexDisk_database(c_yd_file_t file, void * user_data, char * error){
	if (error){
		perror(error);
		return 0;
	}
	
	kdata2_t *d = user_data;

	/* check if data exists in table */
	bool data_exists = false; 

	/* for each table in database */
	kdata2_tab_t **tables = d->tables;
	while (tables) {
		kdata2_tab_t *table = *tables++;
		char timestamp[32] = {0};
		if (sqlite_connect_get_string(
					STR("SELECT timestamp FROM '%s' WHERE uuid = '%s'", 
							table->name, file.name), d->filepath, timestamp))
		{
			/* no such table - do nothing*/
							
		} else {
			data_exists = true;
			/* compare timestamps */
			if (file.modified > atol(timestamp)){
				/* download data from remote YandexDisk to local database */
				_download_data_from_YandexDisk_to_local_database(d, &file);
			}
		}
	}

	/* download data from YandexDisk if local data doesn't exists */
	if (!data_exists) {
		/* download data from remote YandexDisk to local database */
		_download_data_from_YandexDisk_to_local_database(d, &file);
	}
	
	return 0;
}

static int 
_for_each_file_in_YandexDisk_deleted(c_yd_file_t file, void * user_data, char * error){
	if (error){
		perror(error);
		return 0;
	}
	
	kdata2_t *d = user_data;

	/* for each table in database */
	kdata2_tab_t **tables = d->tables;
	while (tables) {
		kdata2_tab_t *table = *tables++;
	
		/* remove data from SQLite database */
		sqlite_connect_execute(
				STR("DELETE FROM '%s' WHERE uuid = '%s'", table->name, file.name), 
						d->filepath);
	}
	
	return 0;
}

static void _yd_update(kdata2_t *d){
	/*
	 * To update data:
	 * 1. get list of updates in update and deleted table in local database with timestamps
	 * 2. check timestamp of data in Yandex Disk
	 * 3. upload local data to Yandex Disk if local timestamp >, or data is deleted
	 * 4. remove data from local updates (deleted) table
	 * 5. get list of updates in remote YandexDisk
	 * 6. download data from Yandex Disk if timestamp > or local data is not exists
	 * 7. get list of deleted in Yandex Disk
	 * 8. remove local data for deleted
	 */
	
	/* do for each update in local updates table */
	sqlite_connect_execute_function(
			"SELECT * from _kdata2_updates", d->filepath, 
					d, _for_each_update_in_SQLite);

	/* do for each file in YandexDisk database */	
	c_yandex_disk_ls(d->access_token, "app:/database", d, 
			_for_each_file_in_YandexDisk_database);

	/* do for each file in YandexDisk deleted */	
	c_yandex_disk_ls(d->access_token, "app:/deleted", d, 
			_for_each_file_in_YandexDisk_deleted);	

}

static void * _yd_thread(void * data){
	struct kdata2 *d = data; 

	while (1) {
		if (d->callback)
			if (d->callback(d->user_data, "yd_daemon: updating data...\n"))
				break;
		_yd_update(d);
		sleep(d->sec);
	}

	free(d);

	pthread_exit(0);	
}

static void _yd_daemon_init(kdata2_t * data){
	int err;
	pthread_t tid; //thread id
	pthread_attr_t attr; //thread attributives
	
	err = pthread_attr_init(&attr);
	if (err) {
		if (data->callback)
			data->callback(data->user_data, 
					STR("yd_daemon: can't set thread attributes: %d\n", err));
		exit(err);
	}	
	
	//create new thread
	err = pthread_create(&tid, &attr, _yd_thread, data);
	if (err) {
		if (data->callback)
			data->callback(data->user_data, 
					STR("yd_daemon: can't create thread: %d\n", err));
		exit(err);
	}
}

int kdata2_init(kdata2_t * dataset){

	/*
	 * For init:
	 * 1. Create (if not exists) loacal database 
	 * 2. Create (if not exists) tables - add uuid and timestamp columns
	 * 3. Create (if not exists) database in Yandex Disk
	 * 4. Allocate data to transfer trough new tread
	 * 5. Start Yandex Disk daemon in thread
	 */

	int err = 0;
	
	/* init SQLIte database */
	/* create database if needed */
	if (sqlite_connect_create_database(dataset->filepath))
		return 1;

	/* Create Yandex Disk database */
	if (dataset->access_token && dataset->access_token[0] != 0){
		char *error = NULL;
		c_yandex_disk_mkdir(dataset->access_token, "app:/database", &error);
		if (error){
			perror(error);
			error = NULL;
		}

		c_yandex_disk_mkdir(dataset->access_token, "app:/deleted", &error);
		if (error){
			perror(error);
			error = NULL;
		}	

		c_yandex_disk_mkdir(dataset->access_token, "app:/data", &error);
		if (error){
			perror(error);
			error = NULL;
		}		
	}

	/* create tables and columns */
	/* to run program in thread we need to allocate memory and store tables there */
	kdata2_tab_t **tables = malloc(8);
	if (!tables){
		perror("tables alloc");
		return -1;
	}
	int tab_c = 0;  // tables count
	
	/* fill SQL string with data and update tbles in memory*/
	kdata2_tab_t ** tab_ptr = dataset->tables; // pointer to iterate
	while (tab_ptr) {
		/* create SQL string */
		char SQL[BUFSIZ] = "";

		/* for each table in dataset */
		kdata2_tab_t **tab = tab_ptr++;

		/* check if columns exists */
		if (!tab[0]->columns)
			continue;

		/* check if name exists */
		if (tab[0]->name[0] == 0)
			continue;

		/* Create Table in remote Yandex Disk database */
		if (dataset->access_token && dataset->access_token[0] != 0){
			char *error = NULL;
			c_yandex_disk_mkdir(dataset->access_token, STR("app:/database/%s", tab[0]->name), &error);
			if (error){
				perror(error);
				error = NULL;
			}

			c_yandex_disk_mkdir(dataset->access_token, STR("app:/deleted/%s", tab[0]->name), &error);
			if (error){
				perror(error);
				error = NULL;
			}			
		}

		/* allocate table*/
		kdata2_tab_t *table = NEW(kdata2_tab_t);
		if (!table){
			perror("table NEW");
			break;
		}

		/* add table to tables */
		tables[tab_c++] = table;
		tables = realloc(tables, tab_c * 8 + 8);
		if (!tables){
			perror("tables realloc");
			break;
		}		

		/* copy table name */
		strcpy(table->name, tab[0]->name);

		/* allocate columns */
		kdata2_col_t **columns = malloc(8); // 8 - size of pointer
		if (!columns){
			perror("columns alloc");
			break;
		}
		int col_c = 0; // columns count
		columns[0] = NULL;

		/* add columns pointer to table */
		table->columns = columns;

		/* add to SQL string */
		sprintf(SQL, "CREATE TABLE IF NOT EXISTS '%s' ( ", tab[0]->name);

		kdata2_col_t ** col_ptr = tab[0]->columns; // pointer to iterate
		while (col_ptr) {
			/* for each column in table */
			kdata2_col_t **col = col_ptr++;

			/* check if name exists */
			if (col[0]->name[0] == 0)
				continue;

			/* each table should have uuid column and timestamp column */
			/* check if column name is uuid or timestamp */
			if (strcmp(col[0]->name, "uuid") == 0 
					|| strcmp(col[0]->name, "timestamp") == 0)
				continue;

			/* allocate column and fill name and type */
			kdata2_col_t *column = NEW(kdata2_col_t);
			strcpy(column->name, col[0]->name);
			column->type = col[0]->type;

			/* add column to columns */
			columns[col_c++] = column;
			columns = realloc(columns, col_c * 8 + 8);
			if (!columns){
				perror("columns realloc");
				break;
			}

			/* append SQL string */
			strcat(SQL, col[0]->name);
			strcat(SQL, " ");
			switch (col[0]->type) {
				case KDATA2_TYPE_NUMBER:
					strcat(SQL, "INT"); break;
				case KDATA2_TYPE_TEXT:
					strcat(SQL, "TEXT"); break;
				case KDATA2_TYPE_DATA:
					strcat(SQL, "BLOB"); break;
			}

			/* append ',' */
			strcat(SQL, ", ");
		}
		/* add uuid column */
		strcat(SQL, "uuid TEXT, timestamp INT)");

		/* run SQL command */
		err = sqlite_connect_execute(SQL, dataset->filepath);
		if (err)
			return err;
	}

	/* create table to store updates */
	char SQL[] = 
		"CREATE TABLE IF NOT EXISTS "
		"_kdata2_updates "
		"( "
		"table TEXT"
		"uuid TEXT, "
		"timestamp INT, "
		"local INT, "
		"deleted INT "
		")"
		;	

	/* run SQL command */
	err = sqlite_connect_execute(SQL, dataset->filepath);
	if (err)
		return err;	

	/* if no token - exit function */
	if (!dataset->access_token || dataset->access_token[0] == 0)
		return 1;

	/* start Yandex Disk daemon */
	/* allocate data */
	kdata2_t *data = NEW(kdata2_t);
	if (!data){
		perror("data NEW");
		return -1;
	}
	/* allocate and copy filepath and token */
	char *filepath = malloc(BUFSIZ);
	strncpy(filepath, dataset->filepath, BUFSIZ-1);
	filepath[BUFSIZ-1] = 0;
	data->filepath = filepath;

	char *access_token = malloc(BUFSIZ);
	strncpy(access_token, dataset->access_token, BUFSIZ-1);
	access_token[BUFSIZ-1] = 0;
	data->access_token = access_token;	

	/* set other data params */
	data->user_data = dataset->user_data;
	data->tables = tables;
	data->sec = dataset->sec;
	data->callback = dataset->callback;

	/* init daemon */
	_yd_daemon_init(data);
	
	return 0;
}

int kdata2_set_number_for_uuid(
		const char *filepath, 
		const char *tablename, 
		const char *column, 
		long number, 
		const char *uuid)
{
	if (!uuid){
		char _uuid[37];
		_uuid_new(_uuid);
		uuid = _uuid;
	}

	time_t timestamp = time(NULL);

	/* update database */
	char SQL[BUFSIZ];
	snprintf(SQL, BUFSIZ-1,
			"INSERT INTO '%s' (uuid) "
			"SELECT '%s' "
			"WHERE NOT EXISTS (SELECT 1 FROM '%s' WHERE uuid = '%s'); "
			"UPDATE '%s' SET timestamp = %ld, '%s' = %ld WHERE uuid = '%s'"
			,
			tablename,
			uuid,
			tablename, uuid,
			tablename, timestamp, column, number, uuid		
	);
	sqlite_connect_execute(SQL, filepath);	

	/* update update table */
	snprintf(SQL, BUFSIZ-1,
			"INSERT INTO _kdata2_updates (uuid) "
			"SELECT '%s' "
			"WHERE NOT EXISTS (SELECT 1 FROM _kdata2_updates WHERE uuid = '%s'); "
			"UPDATE _kdata2_updates SET timestamp = %ld, table = '%s', deleted = 0 WHERE uuid = '%s'"
			,
			uuid,
			uuid,
			timestamp, tablename, uuid		
	);
	sqlite_connect_execute(SQL, filepath);	

	return 0;
}

int kdata2_set_text_for_uuid(
		const char *filepath, 
		const char *tablename, 
		const char *column, 
		const char *text, 
		const char *uuid)
{
	if (!uuid){
		char _uuid[37];
		_uuid_new(_uuid);
		uuid = _uuid;
	}

	time_t timestamp = time(NULL);

	/* update database */
	char SQL[BUFSIZ];
	snprintf(SQL, BUFSIZ-1,
			"INSERT INTO '%s' (uuid) "
			"SELECT '%s' "
			"WHERE NOT EXISTS (SELECT 1 FROM '%s' WHERE uuid = '%s'); "
			"UPDATE '%s' SET timestamp = %ld, '%s' = '%s' WHERE uuid = '%s'"
			,
			tablename,
			uuid,
			tablename, uuid,
			tablename, timestamp, column, text, uuid		
	);
	sqlite_connect_execute(SQL, filepath);	

	/* update update table */
	snprintf(SQL, BUFSIZ-1,
			"INSERT INTO _kdata2_updates (uuid) "
			"SELECT '%s' "
			"WHERE NOT EXISTS (SELECT 1 FROM _kdata2_updates WHERE uuid = '%s'); "
			"UPDATE _kdata2_updates SET timestamp = %ld, table = '%s', deleted = 0 WHERE uuid = '%s'"
			,
			uuid,
			uuid,
			timestamp, tablename, uuid		
	);
	sqlite_connect_execute(SQL, filepath);	
	
	return 0;
}

int kdata2_set_data_for_uuid(
		const char *filepath, 
		const char *tablename, 
		const char *column, 
		void *data, 
		int len,
		const char *uuid)
{
	if (!uuid){
		char _uuid[37];
		_uuid_new(_uuid);
		uuid = _uuid;
	}

	if (!data || !len){
		perror("no data in kdata2_set_data_for_uuid\n");
		return 1;
	}	

	/* get base64 encode */
	size_t size;
	char *base64 = base64_encode(data, len, &size);
	if (!base64){
		perror("base64 encode error in kdata2_set_data_for_uuid\n");
		return 1;
	}

	/* allocate SQL string */
	char *SQL = malloc(size + BUFSIZ);		
	if (!SQL){
		perror("SQL malloc error in kdata2_set_data_for_uuid\n");
		free(base64);
		return 1;
	}	

	time_t timestamp = time(NULL);

	snprintf(SQL, size + BUFSIZ-1,
			"INSERT INTO '%s' (uuid) "
			"SELECT '%s' "
			"WHERE NOT EXISTS (SELECT 1 FROM '%s' WHERE uuid = '%s'); "
			"UPDATE '%s' SET timestamp = %ld, '%s' = '%s' WHERE uuid = '%s'"
			,
			tablename,
			uuid,
			tablename, uuid,
			tablename, timestamp, column, base64, uuid		
	);
	sqlite_connect_execute(SQL, filepath);	
	
	/* update update table */
	snprintf(SQL, BUFSIZ-1,
			"INSERT INTO _kdata2_updates (uuid) "
			"SELECT '%s' "
			"WHERE NOT EXISTS (SELECT 1 FROM _kdata2_updates WHERE uuid = '%s'); "
			"UPDATE _kdata2_updates SET timestamp = %ld, table = '%s', deleted = 0 WHERE uuid = '%s'"
			,
			uuid,
			uuid,
			timestamp, tablename, uuid		
	);
	sqlite_connect_execute(SQL, filepath);	

	/* fre memory */
	free(base64);
	free(SQL);
	
	return 0;
}

int kdata2_remove_for_uuid(
		const char *filepath, 
		const char *tablename, 
		const char *uuid)
{
	if (!uuid){
		perror("no uuid in kdata2_remove_for_uuid\n");
		return 1;
	}	

	sqlite_connect_execute(
			STR("DELETE FROM '%s' WHERE uuid = '%s'", tablename, uuid), filepath);

	/* update update table */
	char SQL[BUFSIZ];
	snprintf(SQL, BUFSIZ-1,
			"INSERT INTO _kdata2_updates (uuid) "
			"SELECT '%s' "
			"WHERE NOT EXISTS (SELECT 1 FROM _kdata2_updates WHERE uuid = '%s'); "
			"UPDATE _kdata2_updates SET timestamp = %ld, table = '%s', deleted = 1 WHERE uuid = '%s'"
			,
			uuid,
			uuid,
			time(NULL), tablename, uuid		
	);
	sqlite_connect_execute(SQL, filepath);	
	
	return 0;
}

struct kdata2_get_t{
	void *user_data;
	int (*callback)(void *user_data, enum KDATA2_TYPE type, const char *column, void *value, size_t size);	
};

int kdata2_get_cb(void *user_data, int argc, char **argv, char **titles){
	struct kdata2_get_t *d = user_data;

	/* iterate columns */
	int i;
	for (i = 0; i < argc; ++i) {
		/* get datatype */
		enum KDATA2_TYPE type;
		
		/* switch data types */
		if (argv[i]){
			switch (type) {
				case KDATA2_TYPE_NUMBER: {
					/* buffer overload safe get data */
					char buf[BUFSIZ] = "";
					if (argv[i]){
						strncpy(buf, argv[i], BUFSIZ-1);
						buf[BUFSIZ-1] = 0;
					}
					long number = atol(buf);
					d->callback(d->user_data, type, titles[i], &number, 1);
					break;							 
				} 
				case KDATA2_TYPE_TEXT: {
					/* buffer overload safe get data */
					char buf[BUFSIZ] = "";
					if (argv[i]){
						strncpy(buf, argv[i], BUFSIZ-1);
						buf[BUFSIZ-1] = 0;
					}										   
					d->callback(d->user_data, type, titles[i], buf, strlen(buf));
					break;							 
				} 
				case KDATA2_TYPE_DATA: {
					size_t len = strnlen(argv[i], 1024); ///* TODO: set maximum size for data */
					size_t size;
					void *data = base64_decode(argv[i], len, &size);		
					if (data && size > 0){
						d->callback(d->user_data, type, titles[i], data, size);
						free(data);
					}
					break;							 
				} 
			}
		}
	}	

	return 0;
}

void kdata2_get(
		const char *filepath, 
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
		)
{
	if (!predicate){
		predicate = "";
	}	

	if (!callback){
		perror("no callback in kdata2_get\n");
		return;
	}		

	/* data for callback */
	struct kdata2_get_t d = {
		.user_data = user_data,
		.callback = callback
	};

	sqlite_connect_execute_function(
			STR("SELECT * FROM '%s' %s", tablename, predicate), 
					filepath, &d, kdata2_get_cb);

}
#endif /* ifndef KDATA2_H */
