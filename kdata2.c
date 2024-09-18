/**
 * File              : kdata2.c
 * Author            : Igor V. Sementsov <ig.kuzm@gmail.com>
 * Date              : 10.03.2023
 * Last Modified Date: 16.09.2024
 * Last Modified By  : Igor V. Sementsov <ig.kuzm@gmail.com>
 */

/*
 * this is data library to use with SQLite and YandexDisk sync
 */

#include "kdata2.h"
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "cYandexDisk/cYandexDisk.h"
#include "cYandexDisk/cJSON.h"
#include "cYandexDisk/uuid4.h"
#include "cYandexDisk/alloc.h"
#include "cYandexDisk/log.h"

#define ON_ERR(ptr, msg) \
	({if (ptr->on_error) ptr->on_error(ptr->on_error_data, msg);})
#define ON_LOG(ptr, msg) \
	({if (ptr->on_log) ptr->on_log(ptr->on_log_data, msg);})

#define STRCOPY(dst, src) \
	({strncpy(dst, src, sizeof(dst)-1); dst[sizeof(dst)-1]=0;})

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

static int uuid_new(char uuid[37]){
	UUID4_STATE_T state; UUID4_T identifier;
	uuid4_seed(&state);
	uuid4_gen(&state, &identifier);
	if (!uuid4_to_s(identifier, uuid, 37)){
		return -1;
	}

	return 0;
}

static int kdata2_sqlite3_exec(
		kdata2_t *d, const char *sql)
{
	char *errmsg = NULL;

	ON_LOG(d, sql);
	
	sqlite3_exec(d->db, sql, NULL, NULL, &errmsg);
	if (errmsg){
		ON_ERR(d, 
				STR("sqlite3_exec: %s: %s", sql, errmsg));		
		sqlite3_free(errmsg);	
		return -1;
	}	

	return 0;
}

static int kdata2_sqlite3_prepare_v2(
		kdata2_t *d, const char *sql, sqlite3_stmt **stmt)
{
	char *errmsg = NULL;
	ON_LOG(d, sql);

	int res = 
		sqlite3_prepare_v2(d->db, sql, -1, stmt, NULL);
	if (res != SQLITE_OK) {
		ON_ERR(d,
				STR("sqlite3_prepare_v2: %s: %s", 
					sql, sqlite3_errmsg(d->db)));		
		return -1;
	}	
	return 0;
}

int _remove_local_update(
		void *data, const char *error)
{
	struct kdata2_update *update = data;
	if (!update && !update->d)
	  return -1;
	
	if (error){
		ON_ERR(update->d, error);
		return -1;
	}

	/* remove from local update table */
	char *SQL = 
		STR("DELETE FROM _kdata2_updates WHERE uuid = '%s'", 
				update->uuid); 

	if (kdata2_sqlite3_exec(update->d, SQL))
		return -1;

	free(update);
	return 0;
}

void _after_upload_to_YandexDisk(
		void *data, size_t size, 
		void *user_data, const char *error)
{
	struct kdata2_update *update = user_data;

	if (!update && !update->d)
		return;

	if (error){
		ON_ERR(update->d, error);
		return;
	}
	
	/* free data */
	//if (update->data_to_free)
		//free(update->data_to_free);

	/* Need to check if timestamp of current
	   update is equal to update in SQLite table
	   if it is - remove from update table */

	/* get timestamp from sqlite table */
	sqlite3_stmt *stmt;
	
	char *SQL = STR("SELECT timestamp FROM '%s' WHERE %s = '%s'", 
						update->table, UUIDCOLUMN, update->uuid);
	ON_LOG(update->d, SQL);

	if (kdata2_sqlite3_prepare_v2(update->d, SQL, &stmt))
		return;
	
	time_t timestamp = 0;
	while (sqlite3_step(stmt) != SQLITE_DONE)
		timestamp = sqlite3_column_int64(stmt, 0);

	sqlite3_finalize(stmt);	

	/* check timestamp */
	if (timestamp > update->timestamp)
		return; // need to update again - don't remove from update table

	/* otherwice -> remove from update table and set new timestamp from uploaded file */
	char *errmsg = NULL;
	
	/* get uploaded file*/
	ON_LOG(update->d, 
			STR("c_yandex_disk_file_info: app:/%s/%s", 
				DATABASE, update->uuid));	

	c_yd_file_t file;
	if (c_yandex_disk_file_info(update->d->access_token, 
				STR("app:/%s/%s", 
					DATABASE, update->uuid), &file, &errmsg))
	{
		if(errmsg){
			ON_ERR(update->d, errmsg);
			free(errmsg);
		}
		ON_ERR(update->d, "can't get file");
	};

	/* set new timestamp to sqlite data */
	if (timestamp < file.modified){
		SQL = STR("UPDATE '%s' SET timestamp = %ld WHERE %s = '%s'", 
						update->table, file.modified, UUIDCOLUMN, update->uuid); 
		if (kdata2_sqlite3_exec(update->d, SQL))
			return;
	}

	/* remove local update */
  _remove_local_update(update, NULL);
}

void _upload_local_data_to_Yandex_Disk(
		struct kdata2_update *update)
{
	/* 1. get data from SQLite
	 * 2. create JSON
	 * 3. upload JSON and data
	 * 4. remove uuid from local update table */

	if (!update && !update->d)
		return;

	/* create json */
	cJSON *json = cJSON_CreateObject();
	if (!json){
		ON_ERR(update->d,
				STR("%s", "can't cJSON_CreateObject"));			
		return;
	}
	cJSON_AddItemToObject(json, "tablename",
			cJSON_CreateString(update->table));
	cJSON *columns = cJSON_CreateArray();
	if (!columns){
		ON_ERR(update->d,
				STR("can't cJSON_CreateArray for table: %s", 
					update->table));		
		return;
	}	

	char *SQL = 
		STR("SELECT * FROM '%s' WHERE %s = '%s'", 
				update->table, UUIDCOLUMN, update->uuid);
	ON_LOG(update->d, SQL);

	sqlite3_stmt *stmt;
	if (kdata2_sqlite3_prepare_v2(update->d, SQL, 
				&stmt))
		return;
	
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
				ON_ERR(update->d,
						STR("%s", "can't cJSON_CreateObject"));		
				continue;
			}
			cJSON_AddItemToObject(column, "name",
					cJSON_CreateString(title));
			cJSON_AddItemToObject(column, "type", 
					cJSON_CreateNumber(type));
			
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
					ON_ERR(update->d,
							STR("can't allocate memory for buffer size: %ld"
								, len));					
					break;
				}
				memcpy(buf, value, len);

				/* allocate new struct update for thread */
				struct kdata2_update *new_update = 
					NEW(struct kdata2_update,
					ON_ERR(update->d,
						STR("%s", 
							"can't allocate memory for struct kdata2_update"));
						break
					);					
					
				new_update->d = update->d;
				strcpy(new_update->column, update->column);
				strcpy(new_update->table, update->table);
				strcpy(new_update->uuid, update->uuid);
				new_update->timestamp = update->timestamp;
				new_update->local = update->local;
				new_update->deleted = update->deleted;
				
				new_update->data_to_free = buf;

				/* upload data to YandexDisk */
				ON_LOG(update->d,
							STR("c_yandex_disk_upload_data: app:/%s/%s, data: %ld", 
								DATABASE, update->uuid, len));				
				c_yandex_disk_upload_data(
						update->d->access_token,
						buf,
						len, 
						STR("app:/%s/%s", DATAFILES, data_id),
						true, false,
						new_update,
						_after_upload_to_YandexDisk,
						NULL, NULL);
			} else {
				/* add value to json */
				if (type == KDATA2_TYPE_TEXT){
					size_t len = sqlite3_column_bytes(stmt, i); 
					const unsigned char *value = sqlite3_column_text(stmt, i);
					
					/* buffer overload safe get data */
					char *buf = MALLOC(len + 1,
						ON_ERR(update->d,
								STR("can't allocate memory for buffer size: %ld"
									, len + 1));					
						break;
					);
					strncpy(buf, (const char *)value, len);
					buf[len] = 0;
						
					cJSON_AddItemToObject(
							column, 
							"value", 
							cJSON_CreateString(buf));

					/* free buf */
					//free(buf);
				} else if (type == KDATA2_TYPE_NUMBER){
					long number = sqlite3_column_int64(stmt, i);					
					cJSON_AddItemToObject(
							column, 
							"value", 
							cJSON_CreateNumber(number));
				} else if (type == KDATA2_TYPE_FLOAT){
					double number = 
						sqlite3_column_double(stmt, i);					
					cJSON_AddItemToObject(
							column, 
							"value", 
							cJSON_CreateNumber(number));
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
	ON_LOG(update->d,
		STR("c_yandex_disk_upload_data: app:/%s/%s, data: %s", 
					DATABASE, update->uuid, data));				
	
	c_yandex_disk_upload_data(
			update->d->access_token, 
			data, 
			strlen(data) + 1, 
			STR("app:/%s/%s", DATABASE, update->uuid), 
			true, 
			false, 
			update, 
			_after_upload_to_YandexDisk, 
			NULL, 
			NULL);	

	/* free json */
	cJSON_free(json);
}

int _for_each_update_in_SQLite(
		void *user_data, int argc, char **argv, char **titles)
{
	kdata2_t *d = user_data;
	if (!d)
		return 0; // return 0 - do not interrupt SQL

	/* new kdata_update */
	struct kdata2_update *update = 
		NEW(struct kdata2_update,
			ON_ERR(d,
			STR("%s", 
				"can't allocate memory for struct kdata2_update"));					
		return 0; // return 0 - do not interrupt SQL
	);

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
			case 0: strcpy (update->table, buf); break;
			case 1: STRCOPY(update->uuid,  buf); break;
			case 2: update->timestamp = atol(buf)   ; break;
			case 3: update->local = atoi(buf)       ; break;
			case 4: update->deleted = atoi(buf)     ; break;
		}
	}

	if (strlen(update->uuid) < 10)
		return 0;

	/* set data to use in callback */
	update->d = d;
	
	/* path to remote data */
	char *filepath = STR("app:/%s/%s", DATABASE, update->uuid);

	/* if local update is deleted -> move remote data to deleted */
	if (update->deleted) {
		ON_LOG(d,
			STR("c_yandex_disk_mv: app:/%s/%s", 
				DELETED, update->uuid));	
		
		c_yandex_disk_mv(
				d->access_token, 
				filepath, 
				STR("app:/%s/%s", DELETED, update->uuid), 
				true, 
				update,
				_remove_local_update);

		return 0;
	}


	/* check remote data exists */
	c_yd_file_t remote;
	ON_LOG(d,
		STR("c_yandex_disk_file_info: %s", 
					filepath));	
	
	char *errmsg = NULL;
	c_yandex_disk_file_info(
			d->access_token, 
			filepath, 
			&remote, 
			&errmsg);

	if (errmsg && strcmp(errmsg, "UnauthorizedError") == 0)
	{
		ON_ERR(d, "Unauthorized to Yandex Disk");					
		free(errmsg);
		free(update);
		return -1;
	}

	if (remote.name[0] != 0) {
		/* check timestamp */
		if (update->timestamp > remote.modified) 
		{
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
				ON_LOG(d, 
					STR("_upload_local_data_to_Yandex_Disk: %s", 
								update->uuid));	
				_upload_local_data_to_Yandex_Disk(update);
				free(errmsg);
				return 0;
			} else {
				ON_ERR(d, errmsg);	
				free(update);
				free(errmsg);
				return -1;
			}
		}
		ON_ERR(d, "unknown error");	
		free(update);
		return -1;
	}	

	return 0;
}

void _download_data_from_YandexDisk_to_local_database_cb(
		void *data, size_t size, void *user_data, 
		const char *error)
{
	struct kdata2_update *update = user_data;	
	if (!update && !update->d)
		return;
	
	/* handle error */
	if (error){
		ON_ERR(update->d, error);
		return;
	}

	/* allocate SQL string */
	char *SQL = MALLOC(size + BUFSIZ,
		if (update->d->on_error)
			update->d->on_error(update->d->on_error_data, 
				STR("can't allocate memory for SQL string size: %ld", 
					size + BUFSIZ));	
		return;
	);

	/* update local database */
	snprintf(SQL, size + BUFSIZ-1,
			"UPDATE '%s' SET '%s' = '%s' WHERE %s = '%s'", update->table, UUIDCOLUMN, 
					update->column, 
							(char*)data, update->uuid		
	);

	kdata2_sqlite3_exec(update->d, SQL);
	
	/* free SQL string */
	free(SQL);
}

void _download_json_from_YandexDisk_to_local_database_cb(
		void *data, size_t size, void *user_data, 
		const char *error)
{
	struct kdata2_update *update = user_data;	
	if (!update && !update->d)
		return;
	
	/* handle error */
	if (error){
		ON_ERR(update->d, error);
		return;
	}

	/* data is json file */
	cJSON *json = 
		cJSON_ParseWithLength(data, size);
	if (!json){
		ON_ERR(update->d, "can't parse json file");
		free(update);
		return;
	}

	/* get tablename */
	cJSON *tablename = 
		cJSON_GetObjectItem(json, "tablename");
	if (!tablename){
		ON_ERR(update->d, "can't get tablename from json file");
		cJSON_free(json);
		free(update);
		return;
	}

	STRCOPY(update->table, 
			cJSON_GetStringValue(tablename));

	/* get columns */
	cJSON *columns = 
		cJSON_GetObjectItem(json, "columns");
	if (!columns || !cJSON_IsArray(columns))
	{
		ON_ERR(update->d, "can't get columns from json file");
		cJSON_free(json);
		free(update);
		return;
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
	ON_LOG(update->d, SQL);

	if (kdata2_sqlite3_exec(update->d, SQL))
		return;

	/* get values for each column and update local database */
	cJSON *column = NULL;
	cJSON_ArrayForEach(column, columns){
		cJSON *name = cJSON_GetObjectItem(column, "name");
		if (!name || !cJSON_IsString(name)){
			ON_ERR(update->d, "can't get column name from json file");			
			continue;
		}		
		cJSON *type = cJSON_GetObjectItem(column, "type");
		if (!type || !cJSON_IsNumber(type)){
			ON_ERR(update->d, "can't get column type from json file");			
			continue;
		}		
		if (cJSON_GetNumberValue(type) == KDATA2_TYPE_DATA) {
			/* download data from Yandex Disk */
			STRCOPY(update->column, cJSON_GetStringValue(name));

			cJSON *data = cJSON_GetObjectItem(column, "data");
			if (!data){
				ON_ERR(update->d, "can't get column data from json file");			
				continue;
			}			
			
			ON_LOG(update->d,
						STR("c_yandex_disk_download_data: app:/%s/%s", 
							DATAFILES, cJSON_GetStringValue(data)));	

			c_yandex_disk_download_data(
					update->d->access_token, 
					STR("app:/%s/%s", DATAFILES, cJSON_GetStringValue(data)),
					true, 
					update, 
					_download_data_from_YandexDisk_to_local_database_cb, 
					NULL, 
					NULL);

		} else {
			cJSON *value = cJSON_GetObjectItem(column, "value");
			if (!value){
				ON_ERR(update->d, "can't get column value from json file");			
				continue;
			}		
			if (cJSON_GetNumberValue(type) == KDATA2_TYPE_NUMBER)
			{
				snprintf(SQL, BUFSIZ-1,
						"UPDATE '%s' SET '%s' = %ld WHERE %s = '%s'", update->table, 
								cJSON_GetStringValue(name), 
										(long)cJSON_GetNumberValue(value), 
										UUIDCOLUMN, update->uuid		
				);
				ON_LOG(update->d, SQL);
				kdata2_sqlite3_exec(update->d, SQL);
			} 
			else if (cJSON_GetNumberValue(type) == KDATA2_TYPE_FLOAT)
			{
				snprintf(SQL, BUFSIZ-1,
						"UPDATE '%s' SET '%s' = %lf WHERE %s = '%s'", update->table, 
								cJSON_GetStringValue(name), 
										(double)cJSON_GetNumberValue(value), 
										UUIDCOLUMN, update->uuid		
				);
				ON_LOG(update->d, SQL);
				kdata2_sqlite3_exec(update->d, SQL);
			} 
			else 
			{
				snprintf(SQL, BUFSIZ-1,
						"UPDATE '%s' SET '%s' = '%s' WHERE %s = '%s'", update->table, 
								cJSON_GetStringValue(name), 
										cJSON_GetStringValue(value), 
										UUIDCOLUMN, update->uuid		
				);				
				ON_LOG(update->d, SQL);
				kdata2_sqlite3_exec(update->d, SQL);
			}
		}
	}

	/* free json */
	cJSON_free(json);

	/* free update */
	free(update);
} 


void _download_from_YandexDisk_to_local_database(
		kdata2_t * d, const c_yd_file_t *file)
{
	/* 1. get json data
	 * 2. update local data for number and text datatype
	 * 3. download and update data for datatype data */

	/* allocate struct and fill update */
	struct kdata2_update *update = NEW(struct kdata2_update,
		if (!update)
			ON_ERR(d, "can't allocate memory for struct kdata2_update");		
		return;
	);
	
	update->d = d;
	STRCOPY(update->uuid, file->name); 
	update->timestamp = file->modified;

	/* download data */
	ON_LOG(d, STR("c_yandex_disk_download_data: %s", file->path));	

	c_yandex_disk_download_data(
			d->access_token, 
			file->path, 
			false, 
			update, 
			_download_json_from_YandexDisk_to_local_database_cb, 
			NULL, 
			NULL);	
}

static int _for_each_file_in_YandexDisk_database(
		const c_yd_file_t *file, void * user_data, 
		const char * error)
{
	kdata2_t *d = user_data;

	if (!d)
		return -1;

	if (error){
		ON_ERR(d, error);
		return 0;
	}

	if (!file){
		ON_ERR(d, "file is NULL");
		return -1;
	}	
	
	/* check if data exists in table */
	bool data_exists = false; 

	/* for each table in database */
	struct kdata2_table **tables = d->tables;
	while (*tables) {
		struct kdata2_table *table = *tables++;
	
		
		char *SQL = 
			STR("SELECT timestamp FROM '%s' WHERE %s = '%s'", 
					table->tablename, UUIDCOLUMN, file->name);
		
		sqlite3_stmt *stmt;
		if (kdata2_sqlite3_prepare_v2(d, SQL, &stmt))
			continue;

		time_t timestamp = 0;
		while (sqlite3_step(stmt) != SQLITE_DONE)
			timestamp = sqlite3_column_int64(stmt, 0);
	
		sqlite3_finalize(stmt);	

		if (timestamp){
			data_exists = true;
			/* compare timestamps */
			if (file->modified > timestamp){
				/* download data from remote YandexDisk to local database */
				ON_LOG(d, STR("_download_from_YandexDisk_to_local_database: %s",

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

int _for_each_file_in_YandexDisk_deleted(
		const c_yd_file_t *file, void * user_data, 
		const char * error)
{
	kdata2_t *d = user_data;

	if (!d)
		return -1;

	if (error){
		ON_ERR(d, error);
		return 0;
	}

	if (!file){
		ON_ERR(d, "file is NULL");
		return -1;
	}	
	
	/* for each table in database */
	struct kdata2_table **tables = d->tables;
	while (*tables) {
		struct kdata2_table *table = *tables++;
	
		/* remove data from SQLite database */
		char *SQL = 
			STR("DELETE FROM '%s' WHERE %s = '%s'", 
					table->tablename, UUIDCOLUMN, file->name);
		kdata2_sqlite3_exec(d, SQL);
	}
		
	return 0;
}

void _yd_update(kdata2_t *d){
	/* To update data:
	 * 1. get list of updates in update and deleted table in local database
	 with timestamps
	 * 2. check timestamp of data in Yandex Disk
	 * 3. upload local data to Yandex Disk if local timestamp >, or data is
	 deleted
	 * 4. remove data from local updates (deleted) table
	 * 5. get list of updates in remote YandexDisk
	 * 6. download data from Yandex Disk if timestamp > or local data is not
	 exists
	 * 7. get list of deleted in Yandex Disk
	 * 8. remove local data for deleted */

	if (!d || d->access_token[0] == 0)
		return;
	
	ON_LOG(d, "c_yandex_disk_file_info: app:/");	

	/* check Yandex Disk Connection */
	char *errmsg = NULL;
	c_yandex_disk_file_info(
				d->access_token, 
				"app:/", 
				NULL,
				&errmsg);
	if (errmsg){
		ON_ERR(d, errmsg);
		free(errmsg);		
		return;
	}	

	/* Create Yandex Disk database */
	c_yandex_disk_mkdir(
			d->access_token, 
			STR("app:/%s", DATABASE ), 
			NULL);
	c_yandex_disk_mkdir(
			d->access_token, 
			STR("app:/%s", DELETED  ), 
			NULL);
	c_yandex_disk_mkdir(
			d->access_token, 
			STR("app:/%s", DATAFILES), 
			NULL);

	/* do for each update in local updates table */
	char *SQL = "SELECT * from _kdata2_updates"; 
	if (kdata2_sqlite3_exec(d, SQL))
		return;

	/* do for each file in YandexDisk database */	
	c_yandex_disk_ls(
			d->access_token, 
			STR("app:/%s", DATABASE), 
			d, 
			_for_each_file_in_YandexDisk_database);

	/* do for each file in YandexDisk deleted */	
	c_yandex_disk_ls(
			d->access_token, 
			STR("app:/%s", DELETED), 
			d, 
			_for_each_file_in_YandexDisk_deleted);	

}

static void * _yd_thread(void * data)
{
	struct kdata2 *d = data; 
	if (!d)
		return NULL;

	while (d && d->do_update) {
		ON_LOG(d, "updating data...");	
		_yd_update(d);
		sleep(d->sec);
	}

	pthread_exit(0);	
}

void _yd_daemon_init(kdata2_t * d)
{
	if (!d)
		return;
	d->do_update = true;
	
	//create new thread
	int err = pthread_create(
			&(d->tid), 
			NULL, 
			_yd_thread, 
			d);
	if (err) {
		ON_ERR(d, STR("can't create thread: %d", err));		
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
		on_log(on_log_data, "init...");	

	/* For init:
	 * 1. Create (if not exists) loacal database 
	 * 2. Create (if not exists) tables - add uuid and timestamp columns
	 * 3. Create (if not exists) database in Yandex Disk
	 * 4. Allocate data to transfer trough new tread
	 * 5. Start Yandex Disk daemon in thread */

	int err = 0;
	char *errmsg = NULL;
	
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
	kdata2_t *d = NEW(kdata2_t,
		if (on_error)
			on_error(on_error_data, "can't allocate kdata2_t");			
		return -1;
		);
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
	ON_LOG(d, STR("sqlite3_open_v2: %s", d->filepath));	
	err = sqlite3_open_v2(
			d->filepath, 
			&(d->db), 
			SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, 
			NULL);
	if (err){
		ON_ERR(d,  
			STR("failed to open/create database at path: '%s': %s", 
						d->filepath, sqlite3_errmsg(d->db)));		
		return err;
	} 

	/* allocate and fill tables array */
	d->tables = MALLOC(8,
		ON_ERR(d, "can't allocate kdata2_t_table");		
		return -1;
		);
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
		if (kdata2_sqlite3_exec(d, SQL))
			return -1;
		
		/* add columns */
		sprintf(SQL, "ALTER TABLE '%s' ADD COLUMN %s TEXT;", 
				table->tablename, UUIDCOLUMN);
		kdata2_sqlite3_exec(d, SQL);

		sprintf(SQL, "ALTER TABLE '%s' ADD COLUMN timestamp INT;", 
				table->tablename);
		kdata2_sqlite3_exec(d, SQL);
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
	ON_LOG(d, SQL);
	if (kdata2_sqlite3_exec(d, SQL))
		return -1;

	/* if no token */
	if (!access_token)
		access_token = "";

	STRCOPY(d->access_token, access_token);
	
	/* start Yandex Disk daemon */
	_yd_daemon_init(d);
	
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

	time_t timestamp = time(NULL);

	/* start SQLite request */
	char SQL[BUFSIZ];
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
	
	sqlite3_stmt *stmt;
	if (kdata2_sqlite3_prepare_v2(d, SQL, &stmt))
		return NULL;

	int res = sqlite3_bind_blob(stmt, 1, data, len, SQLITE_TRANSIENT);
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
	if (!d)
		return -1;

	if (!uuid){
		ON_ERR(d, "no uuid");
		return -1;
	}	
	
	char SQL[BUFSIZ];
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
	if (!d)
		return NULL;

	if (!SQL){
		ON_ERR(d, "SQL is NULL");
		return NULL;
	}

	char *errmsg = NULL;
	sqlite3_stmt *stmt;
	
	ON_LOG(d, SQL);
	if (kdata2_sqlite3_prepare_v2(d, SQL, &stmt))
		return NULL;

	// get first value
	sqlite3_step(stmt);
	const char *str = (const char *)
		sqlite3_column_text(stmt, 0);
	if (!str)
		return NULL;
	
	char *ret_str = strdup(str);
	sqlite3_finalize(stmt);
	return ret_str;
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
	sqlite3_stmt *stmt;
	
	ON_LOG(d, SQL);
	if (kdata2_sqlite3_prepare_v2(d, SQL, &stmt))
		return;

	int num_cols = sqlite3_column_count(stmt); //number of colums
	
	while (sqlite3_step(stmt) != SQLITE_DONE) {
		enum KDATA2_TYPE types[num_cols];
		const char *columns[num_cols];
		void *values[num_cols];
		size_t sizes[num_cols];

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
			types[i] = type;

			/* get title */
			columns[i] = sqlite3_column_name(stmt, i);

			/* switch data types */
			switch (type) {
				case KDATA2_TYPE_NUMBER: {
					long number = sqlite3_column_int64(stmt, i);
					values[i] = &number;	
					sizes[i] = 1;	
					break;							 
				} 
				case KDATA2_TYPE_FLOAT: {
					double number = sqlite3_column_double(stmt, i);
					values[i] = &number;	
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
			// do callback
			if (callback){
				if (callback(user_data, num_cols, types, columns, values, sizes))
				{
				   sqlite3_finalize(stmt);
				   return;
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

	//free(d);
	return 0;
}

int kdata2_set_access_token(kdata2_t * d, const char *access_token){

	if (!d)
		return -1;
	
	if (!access_token){
		ON_ERR(d, "access_token is NULL");
		return -1;
	}

	STRCOPY(d->access_token, access_token);
	return 0;
}

int kdata2_table_init(struct kdata2_table **t, const char * tablename, ...){
	// check table pointer
	if (!t){
		return -1;
	}
	
	/* allocate new table */
	*t = NEW(struct kdata2_table,
		return -1;
	);
	
	// pointer to collumns
	struct kdata2_column **columns = malloc(8);
	if (!columns){
		return -1;
	}

	/* set tables attributes */
	STRCOPY(t[0]->tablename, tablename);
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
		struct kdata2_column *new = NEW(struct kdata2_column,
			break;
		);

		/* set column attributes */
		STRCOPY(new->columnname, columnname);
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
