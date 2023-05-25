/**
 * File              : kdata2.c
 * Author            : Igor V. Sementsov <ig.kuzm@gmail.com>
 * Date              : 10.03.2023
 * Last Modified Date: 25.05.2023
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
#include <unistd.h>

#include "cYandexDisk/cYandexDisk.h"
#include "cYandexDisk/cJSON.h"
#include "cYandexDisk/uuid4.h"

#define NEW(T)   ({T *new = malloc(sizeof(T)); new;})

#define STR(...)     ({char s[BUFSIZ]; snprintf(s, BUFSIZ-1, __VA_ARGS__); s;}) 
#define STR_ERR(...) STR("E/_%s: %s: %s", __FILE__, __func__, STR(__VA_ARGS__))
#define STR_LOG(...) STR("I/_%ld: %s: %s: %s", time(NULL), __FILE__, __func__, STR(__VA_ARGS__))

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

int uuid_new(char uuid[37]){
	UUID4_STATE_T state; UUID4_T identifier;
	uuid4_seed(&state);
	uuid4_gen(&state, &identifier);
	if (!uuid4_to_s(identifier, uuid, 37)){
		return -1;
	}

	return 0;
}

int 
_remove_local_update(void *user_data, const char *error){
	struct kdata2_update *update = user_data;
	if (!update && !update->d)
		return -1;
	
	if (error){
		if (update->d->on_error)
			update->d->on_error(update->d->on_error_data, 
				STR_ERR("%s", error));
		return -1;
	}

	char *errmsg = NULL;
	char *SQL;

	/* remove from local update table */
	SQL = STR("DELETE FROM _kdata2_updates WHERE %s = '%s'", UUIDCOLUMN, update->uuid); 
	if (update->d->on_log)
		update->d->on_log(update->d->on_log_data, 
				STR_LOG("%s", SQL));
	
	sqlite3_exec(update->d->db, SQL, NULL, NULL, &errmsg);
	if (errmsg){
		if (update->d->on_error)
			update->d->on_error(update->d->on_error_data, 
				STR_ERR("sqlite3_exec: %s: %s", SQL, errmsg));		
		return -1;
	}	

	/* free struct update */
	free(update);
	return 0;
}

int 
_after_upload_to_YandexDisk(size_t size, void *user_data, const char *error){
	struct kdata2_update *update = user_data;

	if (!update && !update->d)
		return -1;

	if (error)
		if (update->d->on_error)
			update->d->on_error(update->d->on_error_data, 
				STR_ERR("%s", error));
	
	/* free data */
	if (update->data_to_free)
		free(update->data_to_free);

	/* Need to check if timestamp of current update is equal to update in SQLite table */
	/* if it is - remove from update table */

	/* get timestamp from sqlite table */
	sqlite3_stmt *stmt;
	
	char *SQL = STR("SELECT timestamp FROM '%s' WHERE %s = '%s'", 
						update->table, UUIDCOLUMN, update->uuid);
	if (update->d->on_log)
		update->d->on_log(update->d->on_log_data, 
				STR_LOG("%s", SQL));	
	
	int res = sqlite3_prepare_v2(update->d->db, SQL, -1, &stmt, NULL);
	if (res != SQLITE_OK) {
		if (update->d->on_error)
			update->d->on_error(update->d->on_error_data, 
				STR_ERR("sqlite3_prepare_v2: %s: %s", SQL, sqlite3_errmsg(update->d->db)));		
		return -1;
	}	

	time_t timestamp = 0;
	while (sqlite3_step(stmt) != SQLITE_DONE)
		timestamp = sqlite3_column_int64(stmt, 0);

	sqlite3_finalize(stmt);	

	/* check timestamp */
	if (timestamp > update->timestamp)
		return 1; // need to update again - don't remove from update table

	/* otherwice -> remove from update table and set new timestamp from uploaded file */
	char *errmsg = NULL;
	
	/* get uploaded file*/
	if (update->d->on_log)
		update->d->on_log(update->d->on_log_data, 
				STR_LOG("c_yandex_disk_file_info: app:/%s/%s", DATABASE, update->uuid));	

	c_yd_file_t file;
	if (c_yandex_disk_file_info(update->d->access_token, 
				STR("app:/%s/%s", DATABASE, update->uuid), &file, &errmsg))
	{
		if(errmsg){
			if (update->d->on_error)
				update->d->on_error(update->d->on_error_data, 
					STR_ERR("%s", errmsg));			
			free(errmsg);
		}
		
		if (update->d->on_error)
			update->d->on_error(update->d->on_error_data, 
				STR_ERR("%s", "can't get file"));		
		
		return -1;
	};

	/* set new timestamp to sqlite data */
	errmsg = NULL;
	if (timestamp < file.modified){
		SQL = STR("UPDATE '%s' SET timestamp = %ld WHERE %s = '%s'", 
						update->table, file.modified, UUIDCOLUMN, update->uuid); 
		if (update->d->on_log)
			update->d->on_log(update->d->on_log_data, 
					STR_LOG("%s", SQL));		
		sqlite3_exec(update->d->db, SQL, NULL, NULL, &errmsg);
		if (errmsg){
			if (update->d->on_error)
				update->d->on_error(update->d->on_error_data, 
					STR_ERR("sqlite3_exec: %s: %s", SQL, errmsg));			
			free(errmsg);			
			return -1;
		}		
	}

	/* remove local update */
	return _remove_local_update(update, NULL);
}

void 
_upload_local_data_to_Yandex_Disk(struct kdata2_update *update){
	/*
	 * 1. get data from SQLite
	 * 2. create JSON
	 * 3. upload JSON and data
	 * 4. remove uuid from local update table
	 */

	if (!update && !update->d)
		return;

	/* create json */
	cJSON *json = cJSON_CreateObject();
	if (!json){
		if (update->d->on_error)
			update->d->on_error(update->d->on_error_data, 
				STR_ERR("%s", "can't cJSON_CreateObject"));			
		return;
	}
	cJSON_AddItemToObject(json, "tablename", cJSON_CreateString(update->table));
	cJSON *columns = cJSON_CreateArray();
	if (!columns){
		if (update->d->on_error)
			update->d->on_error(update->d->on_error_data, 
				STR_ERR("can't cJSON_CreateArray for table: %s", update->table));		
		return;
	}	

	char *errmsg = NULL;
	sqlite3_stmt *stmt;
	
	char *SQL = STR("SELECT * FROM '%s' WHERE %s = '%s'", update->table, UUIDCOLUMN, update->uuid);
	if (update->d->on_log)
		update->d->on_log(update->d->on_log_data, 
				STR_LOG("%s", SQL));	

	int res = sqlite3_prepare_v2(update->d->db, SQL, -1, &stmt, NULL);
	if (res != SQLITE_OK) {
		if (update->d->on_error)
			update->d->on_error(update->d->on_error_data, 
				STR_ERR("sqlite3_prepare_v2: %s: %s", SQL, sqlite3_errmsg(update->d->db)));		
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
				case SQLITE_FLOAT:   type = KDATA2_TYPE_FLOAT;  break;
				
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
				if (update->d->on_error)
					update->d->on_error(update->d->on_error_data, 
						STR_ERR("%s", "can't cJSON_CreateObject"));		
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
					if (update->d->on_error)
						update->d->on_error(update->d->on_error_data, 
							STR_ERR("can't allocate memory for buffer size: %ld"
								, len));					
					break;
				}
				memcpy(buf, value, len);

				/* allocate new struct update for thread */
				struct kdata2_update *new_update = NEW(struct kdata2_update);
				if (!new_update){
					if (update->d->on_error)
						update->d->on_error(update->d->on_error_data, 
							STR_ERR("%s", "can't allocate memory for struct kdata2_update"));					
					
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
				if (update->d->on_log)
					update->d->on_log(update->d->on_log_data, 
							STR_LOG("c_yandex_disk_upload_data: app:/%s/%s, data: %ld", 
								DATABASE, update->uuid, len));				
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
						if (update->d->on_error)
							update->d->on_error(update->d->on_error_data, 
								STR_ERR("can't allocate memory for buffer size: %ld"
									, len + 1));					
						break;
					}
					strncpy(buf, (const char *)value, len);
					buf[len] = 0;
						
					cJSON_AddItemToObject(column, "value", cJSON_CreateString(buf));

					/* free buf */
					free(buf);
				} else if (type == KDATA2_TYPE_NUMBER){
					long number = sqlite3_column_int64(stmt, i);					
					cJSON_AddItemToObject(column, "value", cJSON_CreateNumber(number));
				} else if (type == KDATA2_TYPE_FLOAT){
					double number = sqlite3_column_double(stmt, i);					
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
	if (update->d->on_log)
		update->d->on_log(update->d->on_log_data, 
				STR_LOG("c_yandex_disk_upload_data: app:/%s/%s, data: %s", 
					DATABASE, update->uuid, data));				
	
	c_yandex_disk_upload_data(update->d->access_token, data, strlen(data) + 1, 
			STR("app:/%s/%s", DATABASE, update->uuid), true, false, 
					update, _after_upload_to_YandexDisk, NULL, NULL);	

	/* free json */
	cJSON_free(json);
}

int 
_for_each_update_in_SQLite(void *user_data, int argc, char **argv, char **titles){

	kdata2_t *d = user_data;

	if (!d)
		return 0; // return 0 - do not interrupt SQL

	/* new kdata_update */
	struct kdata2_update *update = NEW(struct kdata2_update);
	if (!update){
		if (update->d->on_error)
			update->d->on_error(update->d->on_error_data, 
				STR_ERR("%s", 
					"can't allocate memory for struct kdata2_update"));					
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
		if (update->d->on_log)
			update->d->on_log(update->d->on_log_data, 
					STR_LOG("c_yandex_disk_mv: app:/%s/%s", DELETED, update->uuid));	
		c_yandex_disk_mv(d->access_token, filepath, 
				STR("app:/%s/%s", DELETED, update->uuid), true, 
						update, _remove_local_update);

		return 0;
	}

	char *errmsg = NULL;

	/* check remote data exists */
	c_yd_file_t remote;
	if (update->d->on_log)
		update->d->on_log(update->d->on_log_data, 
				STR_LOG("c_yandex_disk_file_info: %s", 
					filepath));	
	c_yandex_disk_file_info(d->access_token, filepath, &remote, &errmsg);

	if (errmsg){
		if (strcmp(errmsg, "UnauthorizedError") == 0){
			if (update->d->on_error)
				update->d->on_error(update->d->on_error_data, 
					STR_ERR("%s", 
						"Unauthorized to Yandex Disk"));					
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
				if (update->d->on_log)
					update->d->on_log(update->d->on_log_data, 
							STR_LOG("_upload_local_data_to_Yandex_Disk: %s", 
								update->uuid));	
				free(errmsg);
				return 0;
			} else {
				if (update->d->on_error)
					update->d->on_error(update->d->on_error_data, 
						STR_ERR("%s", errmsg));	
				free(update);
				free(errmsg);
				return -1;
			}
		}
		if (update->d->on_error)
			update->d->on_error(update->d->on_error_data, 
				STR_ERR("%s", "unknown error"));	
			
		free(update);
		return -1;
	}	

	return 0;
}

int 
_download_data_from_YandexDisk_to_local_database_cb(size_t size, void *data, void *user_data, const char *error){

	struct kdata2_update *update = user_data;	

	if (!update && !update->d)
		return -1;
	
	/* handle error */
	if (error)
		if (update->d->on_error)
			update->d->on_error(update->d->on_error_data, 
				STR_ERR("%s", error));	

	/* allocate SQL string */
	char *SQL = malloc(size + BUFSIZ);
	if (!SQL){
		if (update->d->on_error)
			update->d->on_error(update->d->on_error_data, 
				STR_ERR("can't allocate memory for SQL string size: %ld", size + BUFSIZ));	
		return -1;
	}

	/* update local database */
	snprintf(SQL, size + BUFSIZ-1,
			"UPDATE '%s' SET '%s' = '%s' WHERE %s = '%s'", update->table, UUIDCOLUMN, 
					update->column, 
							(char*)data, update->uuid		
	);

	if (update->d->on_log)
		update->d->on_log(update->d->on_log_data, 
				STR_LOG("%s", SQL));	
	char *errmsg = NULL;
	sqlite3_exec(update->d->db, SQL, NULL, NULL, &errmsg);
	if (errmsg){
		if (update->d->on_error)
			update->d->on_error(update->d->on_error_data, 
				STR_ERR("sqlite3_exec: %s: %s", SQL, errmsg));		
		free(errmsg);
	}

	/* free SQL string */
	free(SQL);
	
	return 0;
}

int 
_download_json_from_YandexDisk_to_local_database_cb(size_t size, void *data, void *user_data, const char *error){
	struct kdata2_update *update = user_data;	

	if (!update && !update->d)
		return -1;
	
	/* handle error */
	if (error)
		if (update->d->on_error)
			update->d->on_error(update->d->on_error_data, 
				STR_ERR("%s", error));	

	/* data is json file */
	cJSON *json = cJSON_ParseWithLength(data, size);
	if (!json){
		if (update->d->on_error)
			update->d->on_error(update->d->on_error_data, 
				STR_ERR("%s", "can't parse json file"));			
		free(update);
		return 1;
	}

	/* get tablename */
	cJSON *tablename = cJSON_GetObjectItem(json, "tablename");
	if (!tablename){
		if (update->d->on_error)
			update->d->on_error(update->d->on_error_data, 
				STR_ERR("%s", "can't get tablename from json file"));		
		cJSON_free(json);
		free(update);
		return 1;
	}

	strncpy(update->table, cJSON_GetStringValue(tablename), 127);
	update->table[127] = 0;

	/* get columns */
	cJSON *columns = cJSON_GetObjectItem(json, "columns");
	if (!columns || !cJSON_IsArray(columns)){
		if (update->d->on_error)
			update->d->on_error(update->d->on_error_data, 
				STR_ERR("%s", "can't get columns from json file"));		
		cJSON_free(json);
		free(update);
		return 1;
	}
	
	/* udate local database */
	char SQL[BUFSIZ];
	snprintf(SQL, BUFSIZ-1,
			"INSERT INTO '%s' (%s) "
			"SELECT '%s' "
			"WHERE NOT EXISTS (SELECT 1 FROM '%s' WHERE %s = '%s'); "
			"UPDATE '%s' SET timestamp = %ld WHERE %s = '%s'"
			,
			update->table, UUIDCOLUMN,
			update->uuid,
			update->table, UUIDCOLUMN, update->uuid,
			update->table, update->timestamp, UUIDCOLUMN, update->uuid		
	);

	if (update->d->on_log)
		update->d->on_log(update->d->on_log_data, 
				STR_LOG("%s", SQL));	
	char *errmsg = NULL;
	sqlite3_exec(update->d->db, SQL, NULL, NULL, &errmsg);
	if (errmsg){
		if (update->d->on_error)
			update->d->on_error(update->d->on_error_data, 
				STR_ERR("sqlite3_exec: %s: %s", SQL, errmsg));		
		free(errmsg);
	}

	/* get values for each column and update local database */
	cJSON *column = NULL;
	cJSON_ArrayForEach(column, columns){
		cJSON *name = cJSON_GetObjectItem(column, "name");
		if (!name || !cJSON_IsString(name)){
			if (update->d->on_error)
				update->d->on_error(update->d->on_error_data, 
					STR_ERR("%s", "can't get column name from json file"));			
			continue;
		}		
		cJSON *type = cJSON_GetObjectItem(column, "type");
		if (!type || !cJSON_IsNumber(type)){
			if (update->d->on_error)
				update->d->on_error(update->d->on_error_data, 
					STR_ERR("%s", "can't get column type from json file"));			
			continue;
		}		
		if (cJSON_GetNumberValue(type) == KDATA2_TYPE_DATA) {
			/* download data from Yandex Disk */
			strncpy(update->column, cJSON_GetStringValue(name), 127);
			update->column[127] = 0;

			cJSON *data = cJSON_GetObjectItem(column, "data");
			if (!data){
				if (update->d->on_error)
					update->d->on_error(update->d->on_error_data, 
						STR_ERR("%s", "can't get column data from json file"));			
				continue;
			}			
			
			if (update->d->on_log)
				update->d->on_log(update->d->on_log_data, 
						STR_LOG("c_yandex_disk_download_data: app:/%s/%s", 
							DATAFILES, cJSON_GetStringValue(data)));	
			c_yandex_disk_download_data(update->d->access_token, 
					STR("app:/%s/%s", DATAFILES, cJSON_GetStringValue(data)), true, 
							update, _download_data_from_YandexDisk_to_local_database_cb, 
									NULL, NULL);	
		} else {
			cJSON *value = cJSON_GetObjectItem(column, "value");
			if (!value){
				if (update->d->on_error)
					update->d->on_error(update->d->on_error_data, 
						STR_ERR("%s", "can't get column value from json file"));			
				continue;
			}		
			if (cJSON_GetNumberValue(type) == KDATA2_TYPE_NUMBER){
				snprintf(SQL, BUFSIZ-1,
						"UPDATE '%s' SET '%s' = %ld WHERE %s = '%s'", update->table, 
								cJSON_GetStringValue(name), 
										(long)cJSON_GetNumberValue(value), UUIDCOLUMN, update->uuid		
				);
				if (update->d->on_log)
					update->d->on_log(update->d->on_log_data, 
							STR_LOG("%s", SQL));				
				sqlite3_exec(update->d->db, SQL, NULL, NULL, &errmsg);
				if (errmsg){
					if (update->d->on_error)
						update->d->on_error(update->d->on_error_data, 
								STR_ERR("sqlite3_exec: %s: %s", SQL, errmsg));	
					free(errmsg);
					errmsg = NULL;
				}
			} else if (cJSON_GetNumberValue(type) == KDATA2_TYPE_FLOAT){
				snprintf(SQL, BUFSIZ-1,
						"UPDATE '%s' SET '%s' = %lf WHERE %s = '%s'", update->table, 
								cJSON_GetStringValue(name), 
										(double)cJSON_GetNumberValue(value), UUIDCOLUMN, update->uuid		
				);
				if (update->d->on_log)
					update->d->on_log(update->d->on_log_data, 
							STR_LOG("%s", SQL));				
				sqlite3_exec(update->d->db, SQL, NULL, NULL, &errmsg);
				if (errmsg){
					if (update->d->on_error)
						update->d->on_error(update->d->on_error_data, 
								STR_ERR("sqlite3_exec: %s: %s", SQL, errmsg));	
					free(errmsg);
					errmsg = NULL;
				}
			} else {
				snprintf(SQL, BUFSIZ-1,
						"UPDATE '%s' SET '%s' = '%s' WHERE %s = '%s'", update->table, 
								cJSON_GetStringValue(name), 
										cJSON_GetStringValue(value), UUIDCOLUMN, update->uuid		
				);				
				if (update->d->on_log)
					update->d->on_log(update->d->on_log_data, 
							STR_LOG("%s", SQL));				
				sqlite3_exec(update->d->db, SQL, NULL, NULL, &errmsg);
				if (errmsg){
					if (update->d->on_error)
						update->d->on_error(update->d->on_error_data, 
								STR_ERR("sqlite3_exec: %s: %s", SQL, errmsg));	
					free(errmsg);
					errmsg = NULL;
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


void
_download_from_YandexDisk_to_local_database(kdata2_t * d, c_yd_file_t *file){
	/*
	 * 1. get json data
	 * 2. update local data for number and text datatype
	 * 3. download and update data for datatype data
	 */

	/* allocate struct and fill update */
	struct kdata2_update *update = NEW(struct kdata2_update);
	if (!update){
		if (d->on_error)
			d->on_error(d->on_error_data, 
					STR_ERR("%s", "can't allocate memory for struct kdata2_update"));		
		return;
	}
	update->d = d;
	
	strncpy(update->uuid, file->name, 36); 
	update->uuid[36] = 0;
	
	update->timestamp = file->modified;

	/* download data */
	if (d->on_log)
		d->on_log(d->on_log_data, 
				STR_LOG("c_yandex_disk_download_data: %s", file->path));	
	c_yandex_disk_download_data(d->access_token, file->path, false, 
			update, _download_json_from_YandexDisk_to_local_database_cb, NULL, NULL);	

}

static int 
_for_each_file_in_YandexDisk_database(c_yd_file_t *file, void * user_data, const char * error){
	kdata2_t *d = user_data;

	if (!d)
		return -1;

	if (error){
		if (d->on_error)
			d->on_error(d->on_error_data, 
					STR_ERR("%s", error));		
		return 0;
	}

	if (!file){
		if (d->on_error)
			d->on_error(d->on_error_data, 
					STR_ERR("%s", "file is NULL"));		
		return -1;
	}	
	
	/* check if data exists in table */
	bool data_exists = false; 

	/* for each table in database */
	struct kdata2_table **tables = d->tables;
	while (*tables) {
		struct kdata2_table *table = *tables++;
	
		sqlite3_stmt *stmt;
		
		char *SQL = STR("SELECT timestamp FROM '%s' WHERE %s = '%s'", 
							table->tablename, UUIDCOLUMN, file->name);
		
		if (d->on_log)
			d->on_log(d->on_log_data, 
					STR_LOG("%s", SQL));		
		int res = sqlite3_prepare_v2(d->db, SQL, -1, &stmt, NULL);
		if (res != SQLITE_OK) {
			if (d->on_error)
				d->on_error(d->on_error_data, 
						STR_ERR("sqlite3_prepare_v2: %s: %s", SQL, sqlite3_errmsg(d->db)));		
			continue;
		}	

		time_t timestamp = 0;
		while (sqlite3_step(stmt) != SQLITE_DONE)
			timestamp = sqlite3_column_int64(stmt, 0);
	
		sqlite3_finalize(stmt);	

		if (timestamp){
			data_exists = true;
			/* compare timestamps */
			if (file->modified > timestamp){
				/* download data from remote YandexDisk to local database */
				if (d->on_log)
					d->on_log(d->on_log_data, 
							STR_LOG("_download_from_YandexDisk_to_local_database: %s",
							   	file->name));				
				_download_from_YandexDisk_to_local_database(d, file);
			}
		}
	}

	/* download data from YandexDisk if local data doesn't exists */
	if (!data_exists) {
		/* download data from remote YandexDisk to local database */
		_download_from_YandexDisk_to_local_database(d, file);
	}
	
	return 0;
}

int 
_for_each_file_in_YandexDisk_deleted(c_yd_file_t *file, void * user_data, const char * error){
	kdata2_t *d = user_data;

	if (!d)
		return -1;

	if (error){
		if (d->on_error)
			d->on_error(d->on_error_data, 
					STR_ERR("%s", error));		
		return 0;
	}

	if (!file){
		if (d->on_error)
			d->on_error(d->on_error_data, 
					STR_ERR("%s", "file is NULL"));		
		return -1;
	}	

	/* for each table in database */
	struct kdata2_table **tables = d->tables;
	while (*tables) {
		struct kdata2_table *table = *tables++;
	
		/* remove data from SQLite database */
		char *errmsg = NULL;
		char *SQL = STR("DELETE FROM '%s' WHERE %s = '%s'", table->tablename, UUIDCOLUMN, file->name);
		
		if (d->on_log)
			d->on_log(d->on_log_data, STR_LOG("%s", SQL));		
		sqlite3_exec(d->db, SQL, NULL, NULL, &errmsg);
		if (errmsg){
			if (d->on_error)
				d->on_error(d->on_error_data, 
						STR_ERR("sqlite3_exec: %s: %s", SQL, errmsg));		
			free(errmsg);
		}		
	}
	
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

	if (!d)
		return;
	
	char *errmsg = NULL;

	/* check Yandex Disk Connection */
	if (d->on_log)
		d->on_log(d->on_log_data, STR_LOG("%s", "c_yandex_disk_file_info: app:/"));	
	if (c_yandex_disk_file_info(d->access_token, "app:/", NULL, &errmsg)){
		if (d->on_log)
			d->on_log(d->on_log_data, 
					STR_LOG("%s", "can't connect to Yandex Disk"));		
		return;
	}	
	
	if (errmsg){
		if (d->on_log)
			d->on_log(d->on_log_data, 
					STR_LOG("%s", errmsg));		
		free(errmsg);		
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
		if (d->on_log)
			d->on_log(d->on_log_data, 
					STR_LOG("sqlite3_exec: %s: %s", SQL, errmsg));		
		free(errmsg);		
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
		if (d->on_log)
			d->on_log(d->on_log_data, STR_LOG("%s", "updating data..."));	
		_yd_update(d);
		sleep(d->sec);
	}

	pthread_exit(0);	
}

void _yd_daemon_init(kdata2_t * d){
	int err;

	//pthread_t tid; //thread id
	pthread_attr_t attr; //thread attributives
	
	err = pthread_attr_init(&attr);
	if (err) {
		if (d->on_error)
			d->on_error(d->on_error_data, 
					STR_ERR("can't set thread attributes: %d", err));		
		return;
	}	
	
	//create new thread
	err = pthread_create(&(d->tid), &attr, _yd_thread, d);
	if (err) {
		if (d->on_error)
			d->on_error(d->on_error_data, 
					STR_ERR("can't create thread: %d", err));		
		return;
	}
}

int kdata2_init(
		kdata2_t ** database,
		const char * filepath,
		const char * access_token,
		void          *on_error_data,
		void         (*on_error)      (void *on_error_data, const char *error),
		void          *on_log_data,
		void         (*on_log)        (void *on_log_data, const char *message),		
		int sec,
		...
		)
{
	if (on_log)
		on_log(on_log_data, STR_LOG("%s", "init..."));	

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
	
	/* check filepath */
	if (!filepath){
		if (on_error)
			on_error(on_error_data, STR_ERR("%s", "filepath is NULL"));	
		return -1;
	}

	/* check database pointer */
	if (!database){
		if (on_error)
			on_error(on_error_data, STR_ERR("%s", "database pointer is NULL"));	
		return -1;
	}
	/* allocate kdata2_t */
	kdata2_t *d = NEW(kdata2_t);
	if (!d){
		if (on_error)
			on_error(on_error_data, STR_ERR("%s", "can't allocate kdata2_t"));			
		return -1;
	}
	*database = d;
	
	/* set callbacks to NULL */
	d->on_error_data = on_error_data;
	d->on_error      = on_error;
	d->on_log_data   = on_log_data;
	d->on_log        = on_log;

	/* set data attributes */
	d->sec = sec;

	strncpy(d->filepath, filepath, BUFSIZ-1);
	d->filepath[BUFSIZ-1] = 0;
	
	/* init SQLIte database */
	/* create database if needed */
	if (on_log)
		on_log(on_log_data, STR_LOG("sqlite3_open_v2: %s", d->filepath));	
	err = sqlite3_open_v2(d->filepath, &(d->db), SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL);
	if (err){
		if (on_error)
			on_error(on_error_data, 
					STR_ERR("failed to open/create database at path: '%s': %s", 
						d->filepath, sqlite3_errmsg(d->db)));		
		return err;
	} 

	/* allocate and fill tables array */
	d->tables = malloc(8);
	if (!d->tables){
		if (on_error)
			on_error(on_error_data, STR_ERR("%s", "can't allocate kdata2_t_table"));		
		return -1;
	}	
	int tcount = 0;

	//init va_args
	va_list args;
	va_start(args, sec);

	struct kdata2_table * table = va_arg(args, struct kdata2_table *);
	if (!table)
		return -1;

	//iterate va_args
	while (table){
		// add table to tables
		d->tables[tcount++] = table;
		
		// realloc tables
		void *p = realloc(d->tables, tcount * 8 + 8);
		if (!p) {
			if (on_error)
				on_error(on_error_data, 
						STR_ERR("can't realloc tables with size: %d", tcount * 8 + 8));			
			break;
		}
		d->tables = p;

		// iterate
		table = va_arg(args, struct kdata2_table *);
	}	
	/* NULL-terminate tables array */
	d->tables[tcount] = NULL;

	/* fill SQL string with data and update tables in memory*/
	struct kdata2_table ** tables = d->tables; // pointer to iterate
	while (*tables) {

		/* create SQL string */
		char SQL[BUFSIZ] = "";

		/* for each table in dataset */
		struct kdata2_table *table = *tables++;
		
		/* check if columns exists */
		if (!table->columns)
			continue;

		/* check if name exists */
		if (table->tablename[0] == 0)
			continue;

		/* add to SQL string */
		sprintf(SQL, "CREATE TABLE IF NOT EXISTS %s ( ", table->tablename);

		struct kdata2_column ** col_ptr = table->columns; // pointer to iterate
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

			/* append SQL string */
			strcat(SQL, col->columnname);
			strcat(SQL, " ");
			switch (col->type) {
				case KDATA2_TYPE_NUMBER:
					strcat(SQL, "INT"); break;
				case KDATA2_TYPE_TEXT:
					strcat(SQL, "TEXT"); break;
				case KDATA2_TYPE_DATA:
					strcat(SQL, "BLOB"); break;
				case KDATA2_TYPE_FLOAT:
					strcat(SQL, "REAL"); break;
				default: continue;
			}

			/* append ',' */
			if (*col_ptr)
				strcat(SQL, ", ");
		}
		/* add uuid column */
		//strcat(SQL, "uuid TEXT, timestamp INT)");
		strcat(SQL, ")");
		
		/* run SQL command */
		sqlite3_exec(d->db, SQL, NULL, NULL, &errmsg);
		if (errmsg){
			if (on_error)
				on_error(on_error_data, 
						STR_ERR("sqlite3_exec: %s: %s", SQL, errmsg));			
			free(errmsg);
			return -1;
		}
		
		/* add columns */
		sprintf(SQL, "ALTER TABLE '%s' ADD COLUMN %s TEXT;", table->tablename, UUIDCOLUMN);
		sqlite3_exec(d->db, SQL, NULL, NULL, &errmsg);
		if (errmsg){
			if (on_log)
				on_log(on_log_data, 
						STR_LOG("sqlite3_exec: %s: %s", SQL, errmsg));			
			free(errmsg);
			errmsg = NULL;
		}

		sprintf(SQL, "ALTER TABLE '%s' ADD COLUMN timestamp INT;", table->tablename);
		sqlite3_exec(d->db, SQL, NULL, NULL, &errmsg);
		if (errmsg){
			if (on_log)
				on_log(on_log_data, 
						STR_LOG("sqlite3_exec: %s: %s", SQL, errmsg));			
			free(errmsg);
			errmsg = NULL;
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
	if (d->on_log)
		d->on_log(d->on_log_data, STR_LOG("%s", SQL));	
	sqlite3_exec(d->db, SQL, NULL, NULL, &errmsg);
	if (errmsg){
		if (on_error)
			on_error(on_error_data, 
					STR_ERR("sqlite3_exec: %s: %s", SQL, errmsg));			
		free(errmsg);
		return -1;
	}

	/* if no token */
	if (!access_token)
		access_token = "";

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
	if (!d)
		return -1;

	if (!uuid){
		char _uuid[37];
		if (uuid_new(_uuid)){
			if (d->on_error)
				d->on_error(d->on_error_data, 
						STR_ERR("%s", "can't generate uuid"));			
			return -1;
		}
		uuid = _uuid;
	}

	char *errmsg = NULL;

	time_t timestamp = time(NULL);

	/* update database */
	char SQL[BUFSIZ];
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
	if (d->on_log)
		d->on_log(d->on_log_data, STR_LOG("%s", SQL));	
	sqlite3_exec(d->db, SQL, NULL, NULL, &errmsg);
	if (errmsg){
		if (d->on_error)
			d->on_error(d->on_error_data, 
					STR_ERR("sqlite3_exec: %s: %s", SQL, errmsg));			
		free(errmsg);
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
	if (d->on_log)
		d->on_log(d->on_log_data, STR_LOG("%s", SQL));	
	sqlite3_exec(d->db, SQL, NULL, NULL, &errmsg);
	if (errmsg){
		if (d->on_error)
			d->on_error(d->on_error_data, 
					STR_ERR("sqlite3_exec: %s: %s", SQL, errmsg));			
		free(errmsg);
		return -1;
	}

	return 0;
}

int kdata2_set_float_for_uuid(
		kdata2_t * d, 
		const char *tablename, 
		const char *column, 
		double number, 
		const char *uuid)
{
	if (!d)
		return -1;

	if (!uuid){
		char _uuid[37];
		if (uuid_new(_uuid)){
			if (d->on_error)
				d->on_error(d->on_error_data, 
						STR_ERR("%s", "can't generate uuid"));			
			return -1;
		}
		uuid = _uuid;
	}

	char *errmsg = NULL;

	time_t timestamp = time(NULL);

	/* update database */
	char SQL[BUFSIZ];
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
	if (d->on_log)
		d->on_log(d->on_log_data, STR_LOG("%s", SQL));	
	sqlite3_exec(d->db, SQL, NULL, NULL, &errmsg);
	if (errmsg){
		if (d->on_error)
			d->on_error(d->on_error_data, 
					STR_ERR("sqlite3_exec: %s: %s", SQL, errmsg));			
		free(errmsg);
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
	if (d->on_log)
		d->on_log(d->on_log_data, STR_LOG("%s", SQL));	
	sqlite3_exec(d->db, SQL, NULL, NULL, &errmsg);
	if (errmsg){
		if (d->on_error)
			d->on_error(d->on_error_data, 
					STR_ERR("sqlite3_exec: %s: %s", SQL, errmsg));			
		free(errmsg);
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
	if (!d)
		return -1;

	if (!uuid){
		char _uuid[37];
		if (uuid_new(_uuid)){
			if (d->on_error)
				d->on_error(d->on_error_data, 
						STR_ERR("%s", "can't generate uuid"));			
			return -1;
		}
		uuid = _uuid;
	}

	char *errmsg = NULL;
	
	time_t timestamp = time(NULL);

	/* update database */
	char SQL[BUFSIZ];
	snprintf(SQL, BUFSIZ-1,
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
	if (d->on_log)
		d->on_log(d->on_log_data, STR_LOG("%s", SQL));	
	sqlite3_exec(d->db, SQL, NULL, NULL, &errmsg);
	if (errmsg){
		if (d->on_error)
			d->on_error(d->on_error_data, 
					STR_ERR("sqlite3_exec: %s: %s", SQL, errmsg));			
		free(errmsg);
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
	if (d->on_log)
		d->on_log(d->on_log_data, STR_LOG("%s", SQL));	
	sqlite3_exec(d->db, SQL, NULL, NULL, &errmsg);
	if (errmsg){
		if (d->on_error)
			d->on_error(d->on_error_data, 
					STR_ERR("sqlite3_exec: %s: %s", SQL, errmsg));			
		free(errmsg);
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
	if (!d)
		return -1;

	if (!uuid){
		char _uuid[37];
		if (uuid_new(_uuid)){
			if (d->on_error)
				d->on_error(d->on_error_data, 
						STR_ERR("%s", "can't generate uuid"));			
			return -1;
		}
		uuid = _uuid;
	}

	if (!data || !len){
		if (d->on_error)
			d->on_error(d->on_error_data, 
					STR_ERR("%s", "no data"));			
		return 1;
	}	

	time_t timestamp = time(NULL);

	/* start SQLite request */
	int res;
	char *errmsg = NULL;

	sqlite3_stmt *stmt;
	
	char SQL[BUFSIZ];
	snprintf(SQL, BUFSIZ-1,
			"INSERT INTO '%s' (%s) "
			"SELECT '%s' "
			"WHERE NOT EXISTS (SELECT 1 FROM '%s' WHERE %s = '%s'); ",
			tablename, UUIDCOLUMN,
			uuid,
			tablename, UUIDCOLUMN, uuid
	);	
	if (d->on_log)
		d->on_log(d->on_log_data, STR_LOG("%s", SQL));	
	res = sqlite3_exec(d->db, SQL, NULL, NULL, &errmsg);
	if (errmsg) {
		if (d->on_error)
			d->on_error(d->on_error_data, 
					STR_ERR("sqlite3_exec: %s: %s", SQL, errmsg));			
		free(errmsg);
		return -1;
	}	

	sprintf(SQL, "UPDATE '%s' SET '%s' = (?) WHERE %s = '%s'", tablename, column, UUIDCOLUMN, uuid);
	
	sqlite3_prepare_v2(d->db, SQL, -1, &stmt, NULL);
	if (res != SQLITE_OK) {
		if (d->on_error)
			d->on_error(d->on_error_data, 
					STR_ERR("sqlite3_prepare_v2: %s: %s", SQL, errmsg));		
		free(errmsg);
		return -1;
	}	

	res = sqlite3_bind_blob(stmt, 1, data, len, SQLITE_TRANSIENT);
	if (res != SQLITE_OK) {
		if (d->on_error)
			d->on_error(d->on_error_data, 
					STR_ERR("sqlite3_bind_blob: %s", sqlite3_errmsg(d->db)));		
	}	

    if((res = sqlite3_step(stmt)) != SQLITE_DONE)
    {
		if (d->on_error)
			d->on_error(d->on_error_data, 
					STR_ERR("sqlite3_step: %s", sqlite3_errmsg(d->db)));		
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
	if (d->on_log)
		d->on_log(d->on_log_data, STR_LOG("%s", SQL));	
	res = sqlite3_exec(d->db, SQL, NULL, NULL, &errmsg);
	if (errmsg) {
		if (d->on_error)
			d->on_error(d->on_error_data, 
					STR_ERR("sqlite3_exec: %s: %s", SQL, errmsg));			
		free(errmsg);
		return -1;
	}	

	return 0;
}

int kdata2_remove_for_uuid(
		kdata2_t *d, 
		const char *tablename, 
		const char *uuid)
{
	if (!d)
		return -1;

	if (!uuid){
		if (d->on_error)
			d->on_error(d->on_error_data, 
					STR_ERR("%s", "no uuid"));		
		return 1;
	}	
	
	char *errmsg = NULL;
	
	char SQL[BUFSIZ];
	snprintf(SQL, BUFSIZ-1, "DELETE FROM '%s' WHERE %s = '%s'", tablename, UUIDCOLUMN, uuid);	

	if (d->on_log)
		d->on_log(d->on_log_data, STR_LOG("%s", SQL));	
	sqlite3_exec(d->db, SQL, NULL, NULL, &errmsg);
	if (errmsg){
		if (d->on_error)
			d->on_error(d->on_error_data, 
					STR_ERR("sqlite3_exec: %s: %s", SQL, errmsg));			
		free(errmsg);
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
	if (d->on_log)
		d->on_log(d->on_log_data, STR_LOG("%s", SQL));	
	sqlite3_exec(d->db, SQL, NULL, NULL, &errmsg);
	if (errmsg){
		if (d->on_error)
			d->on_error(d->on_error_data, 
					STR_ERR("sqlite3_exec: %s: %s", SQL, errmsg));			
		free(errmsg);
		return -1;
	}	
	
	return 0;
}

void kdata2_get(
		kdata2_t *d, 
		const char *SQL, 
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
	if (!d)
		return;
	
	if (!SQL){
		if (d->on_error)
			d->on_error(d->on_error_data, 
					STR_ERR("%s", "SQL is NULL"));		
		return;
	}
	
	if (!callback){
		if (d->on_error)
			d->on_error(d->on_error_data, 
					STR_ERR("%s", "callback is NULL"));		
		return;
	}

	/* start SQLite request */
	int res;
	char *errmsg = NULL;
	sqlite3_stmt *stmt;
	
	if (d->on_log)
		d->on_log(d->on_log_data, STR_LOG("%s", SQL));	
	res = sqlite3_prepare_v2(d->db, SQL, -1, &stmt, NULL);
	if (res != SQLITE_OK) {
		if (d->on_error)
			d->on_error(d->on_error_data, 
					STR_ERR("sqlite3_prepare_v2: %s: %s", SQL, errmsg));		
		free(errmsg);
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
				case SQLITE_FLOAT:   type = KDATA2_TYPE_FLOAT;  break;
				
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
				case KDATA2_TYPE_FLOAT: {
					double number = sqlite3_column_double(stmt, i);
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
						if (d->on_error)
							d->on_error(d->on_error_data, 
									STR_ERR("%s", "can't allocate memory for buffer"));		
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
				
				default:
					break;
				} 
			}
		}
	}

	sqlite3_finalize(stmt);
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

	if (!d)
		return -1;
	
	if (!access_token){
		if (d->on_error)
			d->on_error(d->on_error_data, 
					STR_ERR("%s", "access_token is NULL"));		
		return -1;
	}

	strncpy(d->access_token, access_token, 63);
	d->access_token[63] = 0;
	
	return 0;
}

int kdata2_table_init(struct kdata2_table **t, const char * tablename, ...){
	// check table pointer
	if (!t){
		return -1;
	}
	
	/* allocate new table */
	*t = NEW(struct kdata2_table);
	if (!*t){
		return -1;
	}
	
	// pointer to collumns
	struct kdata2_column **columns = malloc(8);
	if (!columns){
		return -1;
	}

	/* set tables attributes */
	strncpy(t[0]->tablename, tablename, 127);
	t[0]->tablename[127] = 0;
	t[0]->columns = NULL;
	
	//init va_args
	va_list args;
	va_start(args, tablename);

	enum KDATA2_TYPE type = va_arg(args, enum KDATA2_TYPE);
	if (type == KDATA2_TYPE_NULL)
		return -1;

	char * columnname = va_arg(args, char *);
	if (!columnname)
		return -1;

	//iterate va_args
	int i = 0;
	while (type != KDATA2_TYPE_NULL && columnname != NULL){

		/* allocate new column */
		struct kdata2_column *new = NEW(struct kdata2_column);
		if (!new){
			break;
		}

		/* set column attributes */
		strncpy(new->columnname, columnname, 127);
		new->columnname[127] = 0;
		new->type = type;

		/* add column to array */
		columns[i++] = new;

		//realloc columns array
		void *p = realloc(columns, i * 8 + 8);
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
