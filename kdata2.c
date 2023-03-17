/**
 * File              : kdata2.c
 * Author            : Igor V. Sementsov <ig.kuzm@gmail.com>
 * Date              : 10.03.2023
 * Last Modified Date: 17.03.2023
 * Last Modified By  : Igor V. Sementsov <ig.kuzm@gmail.com>
 */

/*
 * this is data library to use with SQLite and YandexDisk sync
 */

#include "kdata2.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

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
	
	free(d);
}

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

static int 
_remove_local_update(void *user_data, char *error){
	struct kdata2_update *update = user_data;
	if (!update){
		ERR("ERROR! _remove_local_update: data is NULL\n");
		return -1;
	}
	
	if (error){
		ERR("ERROR! _remove_local_update: %s\n", error);
		return -1;
	}

	char *errmsg = NULL;
	char *SQL;

	/* remove from local update table */
	SQL = STR("DELETE FROM _kdata2_updates WHERE uuid = '%s'", update->uuid); 
	sqlite3_exec(update->d->db, SQL, NULL, NULL, &errmsg);
	if (errmsg){
		ERR("ERROR! _remove_local_update: sqlite3_exec: %s\n", errmsg);	
		return -1;
	}	

	/* set new timestamp for data */
	SQL = STR("UPDATE '%s' SET timestamp = %ld WHERE uuid = '%s'", 
					update->table, update->timestamp, update->uuid); 
	sqlite3_exec(update->d->db, SQL, NULL, NULL, &errmsg);
	if (errmsg){
		ERR("ERROR! _remove_local_update: sqlite3_exec: %s\n", errmsg);	
		return -1;
	}		

	/* free struct update */
	free(update);
	return 0;
}

static int 
_after_upload_to_YandexDisk(size_t size, void *user_data, char *error){
	struct kdata2_update *update = user_data;

	if (!update){
		ERR("ERROR! _after_upload_to_YandexDisk: data is NULL\n");
		return -1;
	}	

	if (error)
		ERR("ERROR! _after_upload_to_YandexDisk: %s: %s\n", update->uuid, error);
	
	/* free data */
	if (update->data_to_free)
		free(update->data_to_free);

	char *errmsg = NULL;
	
	/* get uploaded file*/
	c_yd_file_t file;
	c_yandex_disk_file_info(update->d->access_token, 
			STR("app:/%s/%s", DATABASE, update->uuid), &file, &errmsg);

	if(errmsg)
		ERR("ERROR! _after_upload_to_YandexDisk: %s\n", errmsg);

	if (!file.name[0]) {
		ERR("ERROR! _after_upload_to_YandexDisk: can't get file in _after_upload_to_YandexDisk\n");
		return -1;
	}

	/* set timestamp */
	update->timestamp = file.modified;

	/* remove local update and sync timestamps */
	_remove_local_update(update, NULL);

	return 0;
}

static void 
_upload_local_data_to_Yandex_Disk(struct kdata2_update *update){
	/*
	 * 1. get data from SQLite
	 * 2. create JSON
	 * 3. upload JSON and data
	 * 4. remove uuid from local update table
	 */

	if (!update){
		ERR("ERROR! _upload_local_data_to_Yandex_Disk: data is NULL\n");
		return;
	}	

	if (!update->d){
		ERR("ERROR! _upload_local_data_to_Yandex_Disk: update->data is NULL\n");
		return;
	}	

	/* create json */
	cJSON *json = cJSON_CreateObject();
	if (!json){
		ERR("ERROR! _upload_local_data_to_Yandex_Disk: can't cJSON_CreateObject\n");
		return;
	}
	cJSON_AddItemToObject(json, "tablename", cJSON_CreateString(update->table));
	cJSON *columns = cJSON_CreateArray();
	if (!columns){
		ERR("ERROR! _upload_local_data_to_Yandex_Disk: can't cJSON_CreateArray\n");
		return;
	}	

	char *errmsg = NULL;
	sqlite3_stmt *stmt;
	
	char *SQL = STR("SELECT * FROM '%s' WHERE uuid = '%s'", update->table, update->uuid);
	int res = sqlite3_prepare_v2(update->d->db, SQL, -1, &stmt, NULL);
	if (res != SQLITE_OK) {
		ERR("ERROR! kdata2_get: sqlite3_prepare_v2 error %s,"
				" for SQL request: %s\n", sqlite3_errmsg(update->d->db), SQL);
		return;
	}	

	
	while (sqlite3_step(stmt) != SQLITE_DONE) {
		
		int num_cols = sqlite3_column_count(stmt); //number of colums
		
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
			/* drop for uuid and timestamp columns */
			if (title){
				if (strcmp(title, "uuid") == 0
						|| strcmp(title, "timestamp") == 0)
					continue;
			} else {
				/* no column name */
				continue;
			}

			/* fill json with data */
			cJSON *column = cJSON_CreateObject();
			if (!column){
				ERR("ERROR! _fill_json_with_SQLite_data: cant cJSON_CreateObject\n");
				continue;
			}
			cJSON_AddItemToObject(column, "name", cJSON_CreateString(title));
			cJSON_AddItemToObject(column, "type", cJSON_CreateNumber(type));
			
			if (type == KDATA2_TYPE_DATA){
				/* for datatype data do upload data and add data_id to json */
				char data_id[37+128];
				sprintf(data_id, "%s_%s",update->uuid, title);	
				
				/* set data_id to json */
				cJSON_AddItemToObject(column, "data", cJSON_CreateString(data_id));

				size_t len = sqlite3_column_bytes(stmt, i); 
				const void *value = sqlite3_column_blob(stmt, i);
				
				/* buffer overload safe get data */
				void *buf = malloc(len);
				if (!buf){
					ERR("ERROR! _fill_json_with_SQLite_data:"
							" can't allocate memory for buffer size: %ld\n", len);
					break;
				}
				memcpy(buf, value, len);

				/* allocate new struct update for thread */
				struct kdata2_update *new_update = NEW(struct kdata2_update);
				if (!new_update){
					ERR("ERROR! _fill_json_with_SQLite_data:"
							" can't allocate memory for struct kdata2_update\n");
					break;
				}

				new_update->d = update->d;
				strcpy(new_update->column, update->column);
				strcpy(new_update->table, update->table);
				strcpy(new_update->uuid, update->uuid);
				new_update->timestamp = update->timestamp;
				new_update->local = update->local;
				new_update->deleted = update->deleted;
				
				new_update->data_to_free = buf;

				/* upload data to YandexDisk */
				c_yandex_disk_upload_data(update->d->access_token, buf, len, 
						STR("app:/%s/%s", DATAFILES, data_id), true, false, 
								new_update, _after_upload_to_YandexDisk, NULL, NULL);
			} else {
				/* add value to json */
				if (type == KDATA2_TYPE_TEXT){
					size_t len = sqlite3_column_bytes(stmt, i); 
					const unsigned char *value = sqlite3_column_text(stmt, i);
					
					/* buffer overload safe get data */
					char *buf = malloc(len + 1);
					if (!buf){
						ERR("ERROR! _fill_json_with_SQLite_data:"
								" can't allocate memory for buffer size: %ld\n", len + 1);
						break;
					}
					strncpy(buf, (const char *)value, len);
					buf[len] = 0;
						
					cJSON_AddItemToObject(column, "value", cJSON_CreateString(buf));

					/* free buf */
					free(buf);
				} else{
					long number = sqlite3_column_int64(stmt, i);					
					cJSON_AddItemToObject(column, "value", cJSON_CreateNumber(number));
				}
			}
			
			cJSON_AddItemToArray(columns, column);
		}
	}

	sqlite3_finalize(stmt);	

	/* add columns to json */
	cJSON_AddItemToObject(json, "columns", columns);

	/* upload json and remove uuid from update table */
	char *data = cJSON_Print(json);
	update->data_to_free = data;
	c_yandex_disk_upload_data(update->d->access_token, data, strlen(data) + 1, 
			STR("app:/%s/%s", DATABASE, update->uuid), true, false, 
					update, _after_upload_to_YandexDisk, NULL, NULL);	

	/* free json */
	cJSON_free(json);
}

static int 
_for_each_update_in_SQLite(void *user_data, int argc, char **argv, char **titles){

	kdata2_t *d = user_data;

	if (!d){
		ERR("ERROR! _for_each_update_in_SQLite: data is NULL\n");
		return 0; // return 0 - do not interrupt SQL
	}	

	/* new kdata_update */
	struct kdata2_update *update = NEW(struct kdata2_update);
	if (!update){
			ERR("ERROR! _for_each_update_in_SQLite:"
				" can't allocate memory for struct kdata2_update\n");
		return 0; // return 0 - do not interrupt SQL
	}

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
			case 0: strcpy (update->table, buf)     ; break;
			case 1: strncpy(update->uuid,  buf, 36) ; update->uuid[36] = 0; break;
			case 2: update->timestamp = atol(buf)   ; break;
			case 3: update->local = atoi(buf)       ; break;
			case 4: update->deleted = atoi(buf)     ; break;
		}
	}

	/* set data to use in callback */
	update->d = d;
	
	/* path to remote data */
	char *filepath = STR("app:/%s/%s", DATABASE, update->uuid);

	/* if local update is deleted -> move remote data to deleted */
	if (update->deleted) {
		c_yandex_disk_mv(d->access_token, filepath, 
				STR("app:/%s/%s", DELETED, update->uuid), true, 
						update, _remove_local_update);

		return 0;
	}

	char *errmsg = NULL;

	/* check remote data exists */
	c_yd_file_t remote;
	c_yandex_disk_file_info(d->access_token, filepath, &remote, &errmsg);

	if (errmsg){
		if (strcmp(errmsg, "UnauthorizedError") == 0){
			ERR("ERROR! _for_each_update_in_SQLite: Unauthorized to Yandex Disk\n");
			
			free(update);
			return 0;
		}
	}

	if (remote.name[0] != 0) {
		/* check timestamp */
		if (update->timestamp > remote.modified) {
			/* upload local data to Yandex Disk */
			_upload_local_data_to_Yandex_Disk(update);
			return 0;
		} else {
			/* no need to upload data, remove from local update table */
			_remove_local_update(update, NULL);
			return 0;
		}
		
	} else {
		if (errmsg){
			if (strcmp(errmsg, "DiskNotFoundError") == 0){
				/* no remote data -> upload local data to Yandex Disk */
				_upload_local_data_to_Yandex_Disk(update);
				return 0;
			} else {
				ERR("ERROR! _for_each_update_in_SQLite: %s\n", errmsg);
				
				free(update);
				return -1;
			}
		}
		ERR("ERROR! _for_each_update_in_SQLite: unknown error\n");
			
		free(update);
		return -1;
	}	

	return 0;
}

static int 
_download_data_from_YandexDisk_to_local_database_cb(size_t size, void *data, void *user_data, char *error){
	/* handle error */
	if (error)
		ERR("ERROR! _download_data_from_YandexDisk_to_local_database_cb: %s\n", error);

	struct kdata2_update *update = user_data;	

	if (!update){
		ERR("ERROR! _download_data_from_YandexDisk_to_local_database_cb: data is NULL\n");
		return -1;
	}	

	if (!update->d){
		ERR("ERROR! _download_data_from_YandexDisk_to_local_database_cb: update->data is NULL\n");
		return -1;
	}	

	/* allocate SQL string */
	char *SQL = malloc(size + BUFSIZ);
	if (!SQL){
		ERR("ERROR! _download_data_from_YandexDisk_to_local_database_cb:"
				" can't allocate memory for SQL string size: %ld\n", size + BUFSIZ);
		return -1;
	}

	/* update local database */
	snprintf(SQL, size + BUFSIZ-1,
			"UPDATE '%s' SET '%s' = '%s' WHERE uuid = '%s'", update->table, 
					update->column, 
							(char*)data, update->uuid		
	);

	char *errmsg = NULL;
	sqlite3_exec(update->d->db, SQL, NULL, NULL, &errmsg);
	if (errmsg){
		ERR("ERROR! _download_data_from_YandexDisk_to_local_database_cb:"
				" sqlite3_exec: %s: %s\n", SQL, errmsg);
	}

	/* free SQL string */
	free(SQL);
	
	return 0;
}

static int 
_download_json_from_YandexDisk_to_local_database_cb(size_t size, void *data, void *user_data, char *error){
	/* handle error */
	if (error)
		ERR("ERROR! _download_json_from_YandexDisk_to_local_database_cb: %s\n", error);

	struct kdata2_update *update = user_data;

	if (!update){
		ERR("ERROR! _download_json_from_YandexDisk_to_local_database_cb: data is NULL\n");
		return -1;
	}	

	if (!update->d){
		ERR("ERROR! _download_json_from_YandexDisk_to_local_database_cb: update->data is NULL\n");
		return -1;
	}	

	/* data is json file */
	cJSON *json = cJSON_ParseWithLength(data, size);
	if (!json){
		ERR("ERROR! _download_json_from_YandexDisk_to_local_database_cb: can't parse json file\n");
		free(update);
		return 1;
	}

	/* get tablename */
	cJSON *tablename = cJSON_GetObjectItem(json, "tablename");
	if (!tablename){
		ERR("ERROR! _download_json_from_YandexDisk_to_local_database_cb:"
				" can't get tablename from json file\n");
		cJSON_free(json);
		free(update);
		return 1;
	}

	strncpy(update->table, cJSON_GetStringValue(tablename), 127);
	update->table[127] = 0;

	/* get columns */
	cJSON *columns = cJSON_GetObjectItem(json, "columns");
	if (!columns || !cJSON_IsArray(columns)){
		ERR("ERROR! _download_json_from_YandexDisk_to_local_database_cb:"
				" can't get columns from json file\n");
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

	char *errmsg = NULL;
	sqlite3_exec(update->d->db, SQL, NULL, NULL, &errmsg);
	if (errmsg){
		ERR("ERROR! _download_json_from_YandexDisk_to_local_database_cb:"
				" sqlite3_exec: %s: %s\n", SQL, errmsg);
	}

	/* get values for each column and update local database */
	cJSON *column = NULL;
	cJSON_ArrayForEach(column, columns){
		cJSON *name = cJSON_GetObjectItem(column, "name");
		if (!name || !cJSON_IsString(name)){
			ERR("ERROR! _download_json_from_YandexDisk_to_local_database_cb:"
					" can't get column name from json file\n");
			continue;
		}		
		cJSON *type = cJSON_GetObjectItem(column, "type");
		if (!type || !cJSON_IsNumber(type)){
			ERR("ERROR! _download_json_from_YandexDisk_to_local_database_cb:"
					" can't get column type from json file\n");
			continue;
		}		
		if (cJSON_GetNumberValue(type) == KDATA2_TYPE_DATA) {
			/* download data from Yandex Disk */
			strncpy(update->column, cJSON_GetStringValue(name), 127);
			update->column[127] = 0;

			cJSON *data = cJSON_GetObjectItem(column, "data");
			if (!data){
				ERR("ERROR! _download_json_from_YandexDisk_to_local_database_cb:"
						" can't get column data from json file\n");
				continue;
			}			
			c_yandex_disk_download_data(update->d->access_token, 
					STR("app:/%s/%s", DATAFILES, cJSON_GetStringValue(data)), true, 
							update, _download_data_from_YandexDisk_to_local_database_cb, 
									NULL, NULL);	
		} else {
			cJSON *value = cJSON_GetObjectItem(column, "value");
			if (!value){
				ERR("ERROR! _download_json_from_YandexDisk_to_local_database_cb:"
						" can't get column value from json file\n");
				continue;
			}		
			if (cJSON_GetNumberValue(type) == KDATA2_TYPE_NUMBER){
				snprintf(SQL, BUFSIZ-1,
						"UPDATE '%s' SET '%s' = %ld WHERE uuid = '%s'", update->table, 
								cJSON_GetStringValue(name), 
										(long)cJSON_GetNumberValue(value), update->uuid		
				);
				sqlite3_exec(update->d->db, SQL, NULL, NULL, &errmsg);
				if (errmsg){
					ERR("ERROR! _download_json_from_YandexDisk_to_local_database_cb:"
							" sqlite3_exec: %s: %s\n", SQL, errmsg);
				}
			} else {
				snprintf(SQL, BUFSIZ-1,
						"UPDATE '%s' SET '%s' = '%s' WHERE uuid = '%s'", update->table, 
								cJSON_GetStringValue(name), 
										cJSON_GetStringValue(value), update->uuid		
				);				
				sqlite3_exec(update->d->db, SQL, NULL, NULL, &errmsg);
				if (errmsg){
					ERR("ERROR! _download_json_from_YandexDisk_to_local_database_cb:"
							" sqlite3_exec: %s: %s\n", SQL, errmsg);
				}
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
_download_from_YandexDisk_to_local_database(kdata2_t * d, c_yd_file_t *file){
	/*
	 * 1. get json data
	 * 2. update local data for number and text datatype
	 * 3. download and update data for datatype data
	 */

	/* allocate struct and fill update */
	struct kdata2_update *update = NEW(struct kdata2_update);
	if (!update){
		ERR("ERROR! _download_from_YandexDisk_to_local_database:"
				" can't allocate memory for struct kdata2_update\n");
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
		ERR("ERROR! _for_each_file_in_YandexDisk_database:"
				" %s\n", error);
		return 0;
	}
	
	kdata2_t *d = user_data;

	if (!d){
		ERR("ERROR! _for_each_file_in_YandexDisk_database: data is NULL\n");
		return -1;
	}	

	/* check if data exists in table */
	bool data_exists = false; 

	/* for each table in database */
	kdata2_tab_t **tables = d->tables;
	while (*tables) {
		kdata2_tab_t *table = *tables++;
	
		sqlite3_stmt *stmt;
		
		char *SQL = STR("SELECT timestamp FROM '%s' WHERE uuid = '%s'", 
							table->name, file.name);
		
		int res = sqlite3_prepare_v2(d->db, SQL, -1, &stmt, NULL);
		if (res != SQLITE_OK) {
			ERR("ERROR! _for_each_file_in_YandexDisk_database:"
					" sqlite3_prepare_v2: %s: %s\n", SQL, sqlite3_errmsg(d->db));
			continue;
		}	

		time_t timestamp = 0;
		while (sqlite3_step(stmt) != SQLITE_DONE)
			timestamp = sqlite3_column_int64(stmt, 0);
	
		sqlite3_finalize(stmt);	

		if (timestamp){
			data_exists = true;
			/* compare timestamps */
			if (file.modified > timestamp){
				/* download data from remote YandexDisk to local database */
				_download_from_YandexDisk_to_local_database(d, &file);
			}
		}
	}

	/* download data from YandexDisk if local data doesn't exists */
	if (!data_exists) {
		/* download data from remote YandexDisk to local database */
		_download_from_YandexDisk_to_local_database(d, &file);
	}
	
	return 0;
}

static int 
_for_each_file_in_YandexDisk_deleted(c_yd_file_t file, void * user_data, char * error){
	if (error){
		ERR("ERROR! _for_each_file_in_YandexDisk_deleted:"
				" %s\n", error);
		return 0;
	}

	kdata2_t *d = user_data;
	if (!d){
		ERR("ERROR! _for_each_file_in_YandexDisk_deleted: data is NULL\n");
		return -1;
	}	

	/* for each table in database */
	kdata2_tab_t **tables = d->tables;
	while (*tables) {
		kdata2_tab_t *table = *tables++;
	
		/* remove data from SQLite database */
		sqlite_connect_execute(
				STR("DELETE FROM '%s' WHERE uuid = '%s'", table->name, file.name), 
						d->filepath);

		char *errmsg = NULL;
		char *SQL = STR("DELETE FROM '%s' WHERE uuid = '%s'", table->name, file.name);
		
		sqlite3_exec(d->db, SQL, NULL, NULL, &errmsg);
		if (errmsg){
			ERR("ERROR! _for_each_file_in_YandexDisk_deleted:"
					" sqlite3_exec: %s: %s\n", SQL, errmsg);
		}		
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

	/* check Yandex Disk Connection */
	c_yd_file_t file;
	if (!c_yandex_disk_file_info(d->access_token, "app:/", &file, &errmsg)){
			ERR("ERROR! _yd_update: can't connect to Yandex Disk\n");
		return;
	}	
	
	if (errmsg){
			ERR("ERROR! _yd_update: %s\n", errmsg);
		return;
	}	

	/* Create Yandex Disk database */
	c_yandex_disk_mkdir(d->access_token, STR("app:/%s", DATABASE ), NULL);
	c_yandex_disk_mkdir(d->access_token, STR("app:/%s", DELETED  ), NULL);
	c_yandex_disk_mkdir(d->access_token, STR("app:/%s", DATAFILES), NULL);

	char *SQL;
	
	/* do for each update in local updates table */
	SQL = "SELECT * from _kdata2_updates"; 
	sqlite3_exec(d->db, SQL, _for_each_update_in_SQLite, d, &errmsg);
	if (errmsg){
			ERR("ERROR! _yd_update: sqlite3_exec: %s, for SQL request: %s\n", errmsg, SQL);
		return;
	}	

	/* do for each file in YandexDisk database */	
	c_yandex_disk_ls(d->access_token, STR("app:/%s", DATABASE), d, 
			_for_each_file_in_YandexDisk_database);

	/* do for each file in YandexDisk deleted */	
	c_yandex_disk_ls(d->access_token, STR("app:/%s", DELETED), d, 
			_for_each_file_in_YandexDisk_deleted);	

}

static void * _yd_thread(void * data){
	struct kdata2 *d = data; 

	while (1) {
		ERR("yd_daemon: updating data...\n");
		_yd_update(d);
		sleep(d->sec);
	}

	pthread_exit(0);	
}

static void _yd_daemon_init(kdata2_t * d){
	int err;

	//pthread_t tid; //thread id
	pthread_attr_t attr; //thread attributives
	
	err = pthread_attr_init(&attr);
	if (err) {
		ERR("ERROR! yd_daemon: can't set thread attributes: %d\n", err);
		exit(err);
	}	
	
	//create new thread
	err = pthread_create(&(d->tid), &attr, _yd_thread, d);
	if (err) {
		ERR("ERROR! yd_daemon: can't create thread: %d\n", err);
		exit(err);
	}
}

int kdata2_init(
		kdata2_t ** database,
		const char * filepath,
		const char * access_token,
		kdata2_tab_t ** tables,
		int sec
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
		ERR("ERROR! kdata2_init:"
				" failed to open/create database at path: '%s': %s\n", 
						d->filepath, sqlite3_errmsg(d->db));
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
