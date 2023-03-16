/**
 * File              : kdata2.c
 * Author            : Igor V. Sementsov <ig.kuzm@gmail.com>
 * Date              : 10.03.2023
 * Last Modified Date: 16.03.2023
 * Last Modified By  : Igor V. Sementsov <ig.kuzm@gmail.com>
 */

/*
 * this is data library to use with SQLite and YandexDisk sync
 */

#include "kdata2.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifndef logfile
#define logfile stderr
#endif /* ifndef logfile */

#define NEW(T)   ({T *new = malloc(sizeof(T)); new;})
#define STR(...) ({char str[BUFSIZ]; sprintf(str, __VA_ARGS__); str;})
#define ERR(...) ({fprintf(logfile, __VA_ARGS__);})

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

static void _kdata_free_data(kdata2_t * d){
	if (!d->tables)
		return;
	
	//kdata2_tab_t ** tables = d->tables; // pointer to iterate
	//while (*tables) {
		/* for each table */
		//kdata2_tab_t *tab = *tables++;		

		//kdata2_col_t ** columns = tab->columns; // pointer to iterate
		//while (*columns) {
			/* for each column in table */
			//kdata2_col_t *col = *columns++;
			//free(col);
		//}	
		
		//free(tab->columns);
		//free(tab);
	//}	
	//free(d->tables);
	free(d);
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
	char *filepath = STR("app:/database/%s", update.uuid);

	/* if local update is deleted -> move remote data to deleted */
	if (update.deleted) {
		c_yandex_disk_mv(d->access_token, filepath, 
				STR("app:/deleted/%s/%s", update.table, update.uuid), true, 
						&update, _remove_local_update);

		return 0;
	}

	char *error = NULL;

	/* check remote data exists */
	c_yd_file_t remote;
	c_yandex_disk_file_info(d->access_token, filepath, &remote, &error);

	if (error){
		if (strcmp(error, "UnauthorizedError") == 0){
			if (d->callback)
				d->callback(d->user_data, STR("_for_each_update_in_SQLite: Unauthorized to Yandex Disk\n"));
			return -1;
		}
	}

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
		if (error){
			if (strcmp(error, "DiskNotFoundError") == 0){
				/* no remote data -> upload local data to Yandex Disk */
				_upload_local_data_to_Yandex_Disk(&update);
			} else {
				if (d->callback)
					d->callback(d->user_data, STR("YandexDisk: %s\n", error));
				return -1;
			}
		}
		if (d->callback)
			d->callback(d->user_data, STR("ERROR! _for_each_update_in_SQLite: unknown error\n"));
		return -1;
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

	char *errmsg = NULL;
	char *SQL;
	
	/* do for each update in local updates table */
	SQL = "SELECT * from _kdata2_updates"; 
	sqlite3_exec(d->db, SQL, _for_each_update_in_SQLite, d, &errmsg);
	if (errmsg){
		if (d->callback)
			d->callback(d->user_data, STR("ERROR! _yd_update: sqlite3_exec: %s, for SQL request: %s\n", errmsg, SQL));
		return;
	}	

	/* do for each file in YandexDisk database */	
	//c_yandex_disk_ls(d->access_token, "app:/database", d, 
	//		_for_each_file_in_YandexDisk_database);

	/* do for each file in YandexDisk deleted */	
	//c_yandex_disk_ls(d->access_token, "app:/deleted", d, 
	//		_for_each_file_in_YandexDisk_deleted);	

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

static void _yd_daemon_init(kdata2_t * d){
	int err;

	/* Create Yandex Disk database */
	char *error = NULL;
	c_yandex_disk_mkdir(d->access_token, "app:/database", &error);
	if (error){
		perror(error);
		error = NULL;
	}

	c_yandex_disk_mkdir(d->access_token, "app:/deleted", &error);
	if (error){
		perror(error);
		error = NULL;
	}	

	c_yandex_disk_mkdir(d->access_token, "app:/data", &error);
	if (error){
		perror(error);
		error = NULL;
	}		

	pthread_t tid; //thread id
	pthread_attr_t attr; //thread attributives
	
	err = pthread_attr_init(&attr);
	if (err) {
		if (d->callback)
			d->callback(d->user_data, 
					STR("ERROR! yd_daemon: can't set thread attributes: %d\n", err));
		exit(err);
	}	
	
	//create new thread
	err = pthread_create(&tid, &attr, _yd_thread, d);
	if (err) {
		if (d->callback)
			d->callback(d->user_data, 
					STR("ERROR! yd_daemon: can't create thread: %d\n", err));
		exit(err);
	}
}

int kdata2_init(
		kdata2_t ** database,
		const char * filepath,
		const char * access_token,
		kdata2_tab_t ** tables,
		int sec,
		void * user_data,
		int (*callback)(void * user_data, char * msg)
		)
{

	/*
	 * For init:
	 * 1. Create (if not exists) loacal database 
	 * 2. Create (if not exists) tables - add uuid and timestamp columns
	 * 3. Create (if not exists) database in Yandex Disk
	 * 4. Allocate data to transfer trough new tread
	 * 5. Start Yandex Disk daemon in thread
	 */

	int err = 0;
	char *errmsg = NULL;
	
	/* allocate kdata2_t */
	if (!database){
		ERR("ERROR! kdata2_init: database pointer is NULL\n");	
		return -1;
	}
	kdata2_t *d = NEW(kdata2_t);
	if (!d){
		ERR("ERROR! kdata2_init: can't allocate kdata2_t *database\n");	
		return -1;
	}
	*database = d;

	/* set data attributes */
	d->sec = sec;
	d->user_data = user_data;
	d->callback = callback;

	/* check filepath */
	if (!filepath){
		ERR("ERROR! kdata2_init: filepath is NULL\n");	
		return -1;
	}
	strncpy(d->filepath, filepath, BUFSIZ-1);
	d->filepath[BUFSIZ-1] = 0;
	
	/* init SQLIte database */
	/* create database if needed */
	err = sqlite3_open_v2(d->filepath, &(d->db), SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL);
	if (err){
		ERR("ERROR! kdata2_init: failed to open/create database at path: '%s': %s\n", d->filepath, sqlite3_errmsg(d->db));
		return err;
	} 

	/* check tables */
	if (!tables){
		ERR("ERROR! kdata2_init: tables array is NULL\n");	
		return -1;
	}
	d->tables = tables;

	/* fill SQL string with data and update tables in memory*/
	kdata2_tab_t ** tab_ptr = d->tables; // pointer to iterate
	while (*tab_ptr) {
		/* create SQL string */
		char SQL[BUFSIZ] = "";

		/* for each table in dataset */
		kdata2_tab_t *tab = *tab_ptr++;

		/* check if columns exists */
		if (!tab->columns)
			continue;

		/* check if name exists */
		if (tab->name[0] == 0)
			continue;

		/* add to SQL string */
		sprintf(SQL, "CREATE TABLE IF NOT EXISTS '%s' ( ", tab->name);

		kdata2_col_t ** col_ptr = tab->columns; // pointer to iterate
		while (*col_ptr) {
			/* for each column in table */
			kdata2_col_t *col = *col_ptr++;

			/* check if name exists */
			if (col->name[0] == 0)
				continue;

			/* each table should have uuid column and timestamp column */
			/* check if column name is uuid or timestamp */
			if (strcmp(col->name, "uuid") == 0 
					|| strcmp(col->name, "timestamp") == 0)
				continue;

			/* append SQL string */
			strcat(SQL, col->name);
			strcat(SQL, " ");
			switch (col->type) {
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
		sqlite3_exec(d->db, SQL, NULL, NULL, &errmsg);
		if (errmsg){
			ERR("ERROR! kdata2_init: sqlite3_exec: %s\n", errmsg);	
			return -1;
		}
	}

	/* create table to store updates */
	char SQL[] = 
		"CREATE TABLE IF NOT EXISTS "
		"_kdata2_updates "
		"( "
		"tablename TEXT, "
		"uuid TEXT, "
		"timestamp INT, "
		"local INT, "
		"deleted INT "
		")"
		;	

	/* run SQL command */
	sqlite3_exec(d->db, SQL, NULL, NULL, &errmsg);
	if (errmsg){
		ERR("ERROR! kdata2_init: sqlite3_exec: %s\n", errmsg);	
		return -1;
	}

	/* if no token - exit function */
	if (!access_token || access_token[0] == 0)
		return -1;

	strncpy(d->access_token, access_token, 63);
	d->access_token[63] = 0;
	
	/* start Yandex Disk daemon */
	_yd_daemon_init(d);
	
	return 0;
}

int kdata2_set_number_for_uuid(
		kdata2_t *d, 
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

	char *errmsg = NULL;

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
	sqlite3_exec(d->db, SQL, NULL, NULL, &errmsg);
	if (errmsg){
		ERR("ERROR! kdata2_set_number_for_uuid: sqlite3_exec: %s, for SQL request: %s\n", errmsg, SQL);	
		return -1;
	}

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
	sqlite3_exec(d->db, SQL, NULL, NULL, &errmsg);
	if (errmsg){
		ERR("ERROR! kdata2_set_number_for_uuid: sqlite3_exec: %s, for SQL request: %s\n", errmsg, SQL);	
		return -1;
	}

	return 0;
}

int kdata2_set_text_for_uuid(
		kdata2_t *d, 
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

	char *errmsg = NULL;
	
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
	sqlite3_exec(d->db, SQL, NULL, NULL, &errmsg);
	if (errmsg){
		ERR("ERROR! kdata2_set_text_for_uuid: sqlite3_exec: %s, for SQL request: %s\n", errmsg, SQL);	
		return -1;
	}

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
	sqlite3_exec(d->db, SQL, NULL, NULL, &errmsg);
	if (errmsg){
		ERR("ERROR! kdata2_set_text_for_uuid: sqlite3_exec: %s, for SQL request: %s\n", errmsg, SQL);	
		return -1;
	}
	
	return 0;
}

int kdata2_set_data_for_uuid(
		kdata2_t *d, 
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
		ERR("ERROR! kdata2_set_data_for_uuid: no data\n");
		return 1;
	}	

	time_t timestamp = time(NULL);

	/* start SQLite request */
	int res;
	char *errmsg = NULL;

	sqlite3_stmt *stmt;
	
	char SQL[BUFSIZ];
	snprintf(SQL, BUFSIZ-1,
			"INSERT INTO '%s' (uuid) "
			"SELECT '%s' "
			"WHERE NOT EXISTS (SELECT 1 FROM '%s' WHERE uuid = '%s'); ",
			tablename,
			uuid,
			tablename, uuid
	);	
	res = sqlite3_exec(d->db, SQL, NULL, NULL, &errmsg);
	if (errmsg) {
		ERR("ERROR! kdata2_set_data_for_uuid: sqlite3_exec: %s, for SQL request: %s\n", errmsg, SQL);	
		return -1;
	}	

	sprintf(SQL, "UPDATE '%s' SET '%s' = (?) WHERE uuid = '%s'", tablename, column, uuid);
	
	sqlite3_prepare_v2(d->db, SQL, -1, &stmt, NULL);
	if (res != SQLITE_OK) {
		ERR("ERROR! kdata2_set_data_for_uuid: sqlite3_exec: %s, for SQL request: %s\n", sqlite3_errmsg(d->db), SQL);	
		return -1;
	}	

	res = sqlite3_bind_blob(stmt, 1, data, len, SQLITE_TRANSIENT);
	if (res != SQLITE_OK) {
		ERR("ERROR! kdata2_set_data_for_uuid: sqlite3_bind_blob: %s\n", sqlite3_errmsg(d->db));	
	}	

    if((res = sqlite3_step(stmt)) != SQLITE_DONE)
    {
		ERR("ERROR! kdata2_set_data_for_uuid: sqlite3_step: %s\n", sqlite3_errmsg(d->db));	
        sqlite3_reset(stmt);
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
	res = sqlite3_exec(d->db, SQL, NULL, NULL, &errmsg);
	if (errmsg) {
		ERR("ERROR! kdata2_set_data_for_uuid: sqlite3_exec: %s, for SQL request: %s\n", errmsg, SQL);	
		return -1;
	}	

	return 0;
}

int kdata2_remove_for_uuid(
		kdata2_t *d, 
		const char *tablename, 
		const char *uuid)
{
	if (!uuid){
		ERR("ERROR! kdata2_set_data_for_uuid: no uuid\n");
		return 1;
	}	
	
	char *errmsg = NULL;
	
	char SQL[BUFSIZ];
	snprintf(SQL, BUFSIZ-1, "DELETE FROM '%s' WHERE uuid = '%s'", tablename, uuid);	

	sqlite3_exec(d->db, SQL, NULL, NULL, &errmsg);
	if (errmsg){
		ERR("ERROR! kdata2_set_data_for_uuid: sqlite3_exec: %s, for SQL request: %s\n", errmsg, SQL);	
		return -1;
	}	

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
	sqlite3_exec(d->db, SQL, NULL, NULL, &errmsg);
	if (errmsg){
		ERR("ERROR! kdata2_set_data_for_uuid: sqlite3_exec: %s, for SQL request: %s\n", errmsg, SQL);	
		return -1;
	}	
	
	return 0;
}

void kdata2_get(
		kdata2_t *d, 
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
	if (!predicate)
		predicate = "";

	if (!callback){
		ERR("ERROR! kdata2_get: callback is NULL\n");
		return;
	}

	/* start SQLite request */
	int res;
	char *errmsg = NULL;
	sqlite3_stmt *stmt;
	
	char *SQL = STR("SELECT * FROM '%s' %s", tablename, predicate);
	res = sqlite3_prepare_v2(d->db, SQL, -1, &stmt, NULL);
	if (res != SQLITE_OK) {
		ERR("ERROR! kdata2_get: sqlite3_prepare_v2 error %s, for SQL request: %s\n", sqlite3_errmsg(d->db), SQL);
		return;
	}	

	int num_cols = sqlite3_column_count(stmt); //number of colums
	
	while (sqlite3_step(stmt) != SQLITE_DONE) {
		/* iterate columns */
		int i;
		for (i = 0; i < num_cols; ++i) {
			/* get datatype */
			enum KDATA2_TYPE type;
			
			int col_type = sqlite3_column_type(stmt, i);
			switch (col_type) {
				case SQLITE_BLOB:    type = KDATA2_TYPE_DATA;   break;
				case SQLITE_INTEGER: type = KDATA2_TYPE_NUMBER; break;
				
				default: type = KDATA2_TYPE_TEXT; break;
			}

			/* get title */
			const char *title = sqlite3_column_name(stmt, i);

			/* switch data types */
			switch (type) {
				case KDATA2_TYPE_NUMBER: {
					long number = sqlite3_column_int64(stmt, i);
					if (callback(user_data, type, title, &number, 1))
						return;
					
					break;							 
				} 
				case KDATA2_TYPE_TEXT: {
					size_t len = sqlite3_column_bytes(stmt, i); 
					const unsigned char *value = sqlite3_column_text(stmt, i);
					
					/* buffer overload safe get data */
					char *buf = malloc(len + 1);
					if (!buf){
						ERR("ERROR! kdata2_get: can't allocate memory for buffer\n");
						break;
					}
					strncpy(buf, (const char *)value, len);
					buf[len] = 0;
					
					if (callback(user_data, type, title, buf, strlen(buf))){
						free(buf);	
						return;
					}
					free(buf);	
					
					break;							 
				} 
				case KDATA2_TYPE_DATA: {
					size_t len = sqlite3_column_bytes(stmt, i); 
					const void *value = sqlite3_column_blob(stmt, i);
					if (callback(user_data, type, title, (void *)value, len)){
						return;
					}
					break;							 
				} 
			}
		}
	}

	sqlite3_finalize(stmt);
}

struct  kdata2_col ** kdata2_column_add(
		kdata2_col_t **columns, 
		enum KDATA2_TYPE type, 
		const char *name)
{
	/* create new columns array */
	if (!columns){
		columns = malloc(8);
		if (!columns){
			ERR("ERROR! kdata2_column_add: can't allocate memory for columns array\n");
			return NULL;
		}
	}

	/* count columns */
	kdata2_col_t **p = columns; // pointer to iterate
	int i = 0;
	while (*p++)
		i++;

	/* allocate new column */
	kdata2_col_t *new = NEW(kdata2_col_t);
	if (!new){
		ERR("ERROR! kdata2_column_add: can't allocate memory for column\n");
		return NULL;
	}

	/* set column attributes */
	strncpy(new->name, name, 127);
	new->name[127] = 0;
	new->type = type;

	/* realloc columns to get more space */
	p = realloc(columns, i*8 + 2*8);
	if (!p){
		ERR("ERROR! kdata2_column_add: can't realloc memory for columns array\n");
		return NULL;
	}
	columns = p;

	/* fill columns array */ 
	columns[i]   = new;
	columns[i+1] = NULL;

	return columns;
}

struct  kdata2_tab ** kdata2_table_add(
		kdata2_tab_t **tables, 
		kdata2_col_t **columns, 
		const char *name)
{
	/* create new tables array */
	if (!tables){
		tables = malloc(8);
		if (!tables){
			ERR("ERROR! kdata2_table_add: can't allocate memory for tables array\n");
			return NULL;
		}
	}

	/* count tables */
	kdata2_tab_t **p = tables; // pointer to iterate
	int i = 0;
	while (*p++)
		i++;

	/* allocate new table */
	kdata2_tab_t *new = NEW(kdata2_tab_t);
	if (!new){
		ERR("ERROR! kdata2_table_add: can't allocate memory for table\n");
		return NULL;
	}

	/* set tables attributes */
	strncpy(new->name, name, 127);
	new->name[127] = 0;
	new->columns = columns;

	/* realloc tables to get more space */
	p = realloc(tables, i*8 + 2*8);
	if (!p){
		ERR("ERROR! kdata2_table_add: can't realloc memory for tables array\n");
		return NULL;
	}
	tables = p;

	/* fill columns array */ 
	tables[i]   = new;
	tables[i+1] = NULL;

	return tables;	
}

int kdata2_close(kdata2_t *d){
	if (!d)
		return -1;

	if (d->db)
		sqlite3_close(d->db);

	free(d);
	return 0;
}

int kdata2_set_access_token(kdata2_t * d, const char *access_token){
	if (!access_token){
		ERR("ERROR! kdata2_set_access_token: access_token is NULL\n");
		return -1;
	}

	if (!d){
		ERR("ERROR! kdata2_set_access_token: dataset is NULL\n");
		return -1;
	}

	strncpy(d->access_token, access_token, 63);
	d->access_token[63] = 0;
	
	return 0;
}
