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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>  //for sleep
#include <pthread.h>

#include "SQLiteConnect/SQLiteConnect.h"
#include "cYandexDisk/cYandexDisk.h"
#include "cYandexDisk/cJSON.h"

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

/* IMP */

void _yd_free_data(kdata2_t * data){
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
};

struct list {
	void *data;
	struct list *prev;
};

int _remove_local_update_after_upload_to_Yandex_Disk(void *user_data, char *error){
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

void _upload_local_data_to_Yandex_Disk(struct kdata2_update *update){
	/*
	 * 1. get data from SQLite
	 * 2. create JSON
	 * 3. upload JSON and data
	 * 4. remove uuid from local update table
	 */

}

int _for_each_update_in_SQLite(void *user_data, int argc, char **argv, char **titles){

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
							&update, _remove_local_update_after_upload_to_Yandex_Disk);

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
				_remove_local_update_after_upload_to_Yandex_Disk(&update, NULL);
			}
			
		} else {
			/* no remote data -> upload local data to Yandex Disk */
			_upload_local_data_to_Yandex_Disk(&update);
		}	

	return 0;
}

int _for_each_file_in_YandexDisk_database(c_yd_file_t file, void * user_data, char * error){
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
			}
		}
	}

	/* download data from YandexDisk if local data doesn't exists */
	if (!data_exists) {
		/* download data from remote YandexDisk to local database */
	}
	
	return 0;
}

int _for_each_file_in_YandexDisk_deleted(c_yd_file_t file, void * user_data, char * error){
	if (error){
		perror(error);
		return 0;
	}
	
	kdata2_t *d = user_data;

	/* remove data from SQLite database */
	
	return 0;
}

void _yd_update(kdata2_t *d){
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

void * _yd_thread(void * data) 
{
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

void _yd_daemon_init(kdata2_t * data){
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

#endif /* ifndef KDATA2_H */
