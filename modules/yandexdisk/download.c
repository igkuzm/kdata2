#include "cYandexDisk/alloc.h"
#include "internal.h"
#include "base64.h"
#include "list.h"
#include "yandexdisk.h"
#include "../../kdata2.h"
#include "cYandexDisk/cYandexDisk.h"
#include "cYandexDisk/cJSON.h"
#include "strtok_foreach.h"
#include <assert.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

struct ddata_t {
	kdydm_t *d;
	char tablename[128];
	char uuid[37];
	time_t timestamp;
	time_t current;
	int deleted;
	list_t *to_download;
	int ret;
};

struct ddata_node {
	struct ddata_t *t;
	char tablename[128];
	char uuid[37];
	time_t timestamp;
	int deleted;
};

static int json_to_database_for_column(
		struct ddata_node *node, cJSON *object, struct kdata2_column *column)
{
	char *uuid = NULL;
	cJSON *item;

	ON_LOG(node->t->d->database, 
			STR("json to database for column: %s", column->columnname));

	item = cJSON_GetObjectItem(object, column->columnname);
	if (item == NULL)
		return 1;

	switch (column->type) {
		case KDATA2_TYPE_NUMBER:
			{
				long value = cJSON_GetNumberValue(item);
				uuid = kdata2_set_number_for_uuid(
						node->t->d->database, 
						node->tablename, 
						column->columnname, 
						value, 
						node->uuid);
			}
			break;
	
		case KDATA2_TYPE_FLOAT:
			{
				double value = cJSON_GetNumberValue(item);
				uuid = kdata2_set_float_for_uuid(
						node->t->d->database, 
						node->tablename, 
						column->columnname, 
						value, 
						node->uuid);
			}
			break;
		
		case KDATA2_TYPE_TEXT:
			{
				const char *value = cJSON_GetStringValue(item);
				uuid = kdata2_set_text_for_uuid(
						node->t->d->database, 
						node->tablename, 
						column->columnname, 
						value, 
						node->uuid);
			}
			break;

		case KDATA2_TYPE_DATA:
			{
				size_t len = 0;
				unsigned char *data = NULL;
				const char *value = cJSON_GetStringValue(item);

				data = base64_decode(
						value, 
						strlen(value), 
						&len);

				if (data)
				{
					uuid = kdata2_set_data_for_uuid(
							node->t->d->database, 
							node->tablename, 
							column->columnname, 
							data,
						  len,	
							node->uuid);
				}
			}
			break;

		default:
			break;
			
	}

	return uuid == NULL;
}

static void json_to_database(
		struct ddata_node *node, cJSON *object)
{
	int err = 0;
	char SQL[BUFSIZ];

	do {
		kdata2_table_for_each(node->t->d->database)
		{
			// find table
			if (strcmp(table->tablename, node->tablename) == 0)
			{
				do {
					kdata2_column_for_each(table)
					{
						json_to_database_for_column(node, object, column);
					}
				} while(0);

				//if (err){
					//ON_ERR(node->t->d->database, 
							//STR("ERROR in tables while parsing JSON "
								//"with path: app:/%s/%s/%s/%ld",
							//node->deleted?DELETED:DATABASE, 
							//node->tablename, node->uuid, node->timestamp));
					//break;
				//}

				snprintf(SQL, BUFSIZ, 
						"UPDATE _kdata2_updates SET "
						"timestamp = %ld, "
						"YANDEX_DISK_UPLOADED = %ld "
						"WHERE uuid = '%s';",
						node->timestamp, node->timestamp, node->uuid);
				kdata2_sqlite3_exec(node->t->d->database, SQL);

				snprintf(SQL, BUFSIZ, 
						"UPDATE '%s' SET "
						"timestamp = %ld, "
						"YANDEX_DISK_UPLOADED = 1 "
						"WHERE %s = '%s';",
						node->tablename, 
						node->timestamp, 
						UUIDCOLUMN, node->uuid);
				kdata2_sqlite3_exec(node->t->d->database, SQL);

				snprintf(SQL, BUFSIZ, 
						"UPDATE _yandexdisk_updates SET "
						"YANDEX_DISK_UPLOADED = %ld;",
						node->t->current);
				kdata2_sqlite3_exec(node->t->d->database, SQL);
			}
		}
	} while(0);
}

static void parse_json(
		void *json, size_t size, void *data, const char *error)
{
	struct ddata_node *node = data;
	cJSON *object;

	ON_LOG(node->t->d->database, 
		STR("parsing JSON with path: app:/%s/%s/%s/%ld",
		node->deleted?DELETED:DATABASE, 
		node->tablename, node->uuid, node->timestamp));

	if (error)
		ON_ERR(node->t->d->database, error);

	if (json)
	{
		object = cJSON_ParseWithLength(
				(const char *)json, size);
		free(json);
		if (object){
			json_to_database(node, object);
			return;
		}
	}
		
	ON_ERR(node->t->d->database, 
		STR("ERROR parsing JSON with path: app:/%s/%s/%s/%ld",
		node->deleted?DELETED:DATABASE, 
		node->tablename, node->uuid, node->timestamp));
}

static int make_downloads(struct ddata_node *node)
{	
	int err = 0;
	pphase phase = PPHASE_DOWNLOADING;
	char path[BUFSIZ];
	char SQL[BUFSIZ];

	assert(node);
	assert(node->t);
	assert(node->t->d);
	assert(node->t->d->database);


	ON_LOG(node->t->d->database, 
			STR("Making %s for uuid: %s timestamp: %ld", 
				phase==PPHASE_DOWNLOADING?"downloads":"deletings",
				node->uuid, node->timestamp));

	if (node->deleted)
		phase = PPHASE_DELETING;

	if (node->t->d->progress)
		node->t->d->progress(
				node->t->d->progressp, 
				phase, 
				node->t->d->current++, 
				node->t->d->total);

	if (node->deleted == 0){
		snprintf(path, BUFSIZ, "app:/%s/%s/%s/%ld",
				node->deleted?DELETED:DATABASE, 
				node->tablename, node->uuid, node->timestamp);
		
		err = c_yandex_disk_download_data(
			node->t->d->access_token, 
			path, 
			true, 
			node, 
			parse_json,
			node->t->d->file_progressp, 
			node->t->d->file_progress);

	} else {
		// remove from database
		sprintf(SQL, 
				"DELETE FROM '%s' WHERE %s = '%s';",
				node->tablename, UUIDCOLUMN, node->uuid);

		kdata2_sqlite3_exec(node->t->d->database, SQL);
	}

	if (node->t->d->progress)
		node->t->d->progress(
				node->t->d->progressp, 
				phase, 
				node->t->d->current, 
				node->t->d->total);

	return 0;
}

static int for_each_timestamp(
		const c_yd_file_t *timestamp, void *data, const char *error)
{
	struct ddata_t *t = data;
	char path[BUFSIZ];

	if (error)
		ON_ERR(t->d->database, error);

	if (timestamp)
	{
		char SQL[BUFSIZ], *timestamp_local;
		
		t->timestamp = atol(timestamp->name);
		
		snprintf(SQL, BUFSIZ, 
				"SELECT timestamp FROM '%s' "
				"WHERE %s = '%s';",
				t->tablename, UUIDCOLUMN, t->uuid);

		timestamp_local = 
			kdata2_get_string(t->d->database, SQL); 

		if (t->deleted){
			ON_LOG(t->d->database, 
					STR("Check delete timestamp: %ld(local): %ld(remote) for uuid: %s", 
						timestamp_local?atol(timestamp_local):0,
					t->timestamp, t->uuid));
		} else {
			ON_LOG(t->d->database, 
					STR("Check update timestamp: %ld(local): %ld(remote) for uuid: %s", 
						timestamp_local?atol(timestamp_local):0,
						t->timestamp, t->uuid));
		}

		if (
				(t->deleted && 
				timestamp_local != NULL && 
				atol(timestamp_local) < t->timestamp) ||

				(t->deleted == 0 &&
				(timestamp_local == NULL || 
				 atol(timestamp_local) < t->timestamp))
			 )
		{
			// add to download list
			ON_LOG(t->d->database, "add to download list"); 

			struct ddata_node *node = NEW(struct ddata_node);
			if (node){
				node->t = t;
				node->deleted = t->deleted;
				node->timestamp = t->timestamp;
				strncpy(node->tablename,
					 	t->tablename, sizeof(node->tablename));
				strncpy(node->uuid,
					 	t->uuid, sizeof(node->uuid));

				if (list_add(&t->to_download, node) == 0)
					t->d->total++;

				return 0;
			}
			
			ON_ERR(t->d->database, "memory allocation error"); 
		} 

		ON_LOG(t->d->database, "No need to update");
		return 0;
	}

	return 1;
}


static int for_each_uuid(
		const c_yd_file_t *uuid, void *data, const char *error)
{
	struct ddata_t *t = data;
	char path[BUFSIZ];

	if (error)
		ON_ERR(t->d->database, error);

	if (uuid)
	{
		strncpy(t->uuid, uuid->name, sizeof(t->uuid));

		snprintf(path, BUFSIZ, "app:/%s/%s/%s",
				t->deleted?DELETED:DATABASE, t->tablename, t->uuid);

		ON_LOG(t->d->database, 
				STR("search updates in: %s", path));

		c_yandex_disk_sort_ls(
				t->d->access_token, 
				path, 
				"-name",
				1,
				t, 
				for_each_timestamp);

		return 0;
	}

	snprintf(path, BUFSIZ, "app:/%s/%s",
			t->deleted?DELETED:DATABASE, t->tablename);

	ON_ERR(t->d->database, 
			STR("ERROR in path: %s", path));

	return 1;
}

static int for_each_table(struct ddata_t *t)
{
	char path[BUFSIZ];

	snprintf(path, BUFSIZ, "app:/%s/%s",
			t->deleted?DELETED:DATABASE, t->tablename);

	ON_LOG(t->d->database, 
			STR("search updates in: %s", path));

	c_yandex_disk_ls(
				t->d->access_token, 
				path, 
				t, 
				for_each_uuid);

	return 0;
}

static void update_list_from_update_file(
		void *data, size_t size, void *userdata, const char *error)
{
	struct ddata_t *t = userdata;
	char *value = data, *row, *token;
	char path[BUFSIZ];

	if (error)
		ON_ERR(t->d->database, error);

	if (value){
		strtok_foreach(value, "\n", row){
			int i = 0;
			ON_LOG(t->d->database, STR("got update row: %s", row));
			strtok_foreach(row, "/", token){
				switch (i) {
					case 0:
						t->deleted = (strcmp(token, DELETED) == 0);
						break;

					case 1:
						strncpy(t->tablename, token, sizeof(t->tablename));
						break;

					case 2:
						strncpy(t->uuid, token, sizeof(t->uuid));
					
					default:
						break;
				}
				i++;	
			}
			if (i == 3){
				// get last timestamp and add to update list
				t->ret = 1;

				snprintf(path, BUFSIZ, "app:/%s/%s/%s",
						t->deleted?DELETED:DATABASE, t->tablename, t->uuid);

				ON_LOG(t->d->database, 
						STR("search updates in: %s", path));
				c_yandex_disk_sort_ls(
						t->d->access_token, 
						path, 
						"-name",
						1,
						t, 
						for_each_timestamp);
			}
		}
	}
}

static int for_each_timestamp_in_updates(
		const c_yd_file_t *timestamp, void *data, const char *error)
{
	struct ddata_t *t = data;
	char path[BUFSIZ];

	assert(t);
	assert(t->d);
	assert(t->d->database);

	if (error)
		ON_ERR(t->d->database, error);

	if (timestamp){
		ON_LOG(t->d->database, STR("check timestamp: %s", timestamp));
		// drop old timestamps
		if (t->d->timestamp >= atol(timestamp->name))
			return 1;

		// add to list
		snprintf(path, BUFSIZ, 
				"app:/%s/%s", UPDATES, 
				timestamp->name);

		c_yandex_disk_download_data(
				t->d->access_token, 
				path, 
				true, 
				t, 
				update_list_from_update_file, 
				NULL, 
				NULL);
	}

	return 0;
}

static int check_updates(struct ddata_t *t)
{
	char SQL[BUFSIZ], path[BUFSIZ], *last_update;
	struct ddata_node *node = NULL;
	t->d->current = 0;
	t->d->total = 0;


	sprintf(SQL, 
			"SELECT YANDEX_DISK_UPLOADED FROM _yandexdisk_updates;");
	last_update = kdata2_get_string(t->d->database, SQL);
	
	ON_LOG(t->d->database, 
			STR("Check updates from last update: %ld", last_update));
	
	if (last_update)
	{
		t->d->timestamp = atol(last_update);
		// get updates files
		sprintf(path, "app:/%s", UPDATES);
		c_yandex_disk_sort_ls(
				t->d->access_token, 
				path, 
				"-name", 
				0, 
				t, 
				for_each_timestamp_in_updates);

		return t->ret;
	}

	return 0;
}

void download_from_yandex_disk(kdydm_t *d)
{
	int i, ret = 0, updates = 0, deleted[] = {0, 1};
	struct ddata_t t;
	struct ddata_node *node = NULL;
		
	assert(d);
	assert(d->database);
		
	memset(&t, 0, sizeof(t));
	t.d = d;
	t.deleted = deleted[i];
	t.current = time(NULL);

	d->current = 0;
	d->total = 0;

	if (d->progress)
		d->progress(d->progressp, PPHASE_COUNTING, 0, 0);
	
	// check updates first
	updates = check_updates(&t);
	if (updates){
		list_for_each(t.to_download, node)
		{
			make_downloads(node);
			free(node);
		}
		list_free(&t.to_download);
		return;
	}

	// check all tables if no updates
	for (i = 0; i < 2; ++i) {
		node = NULL;
		d->current = 0;
		d->total = 0;
		d->current_table = 0;
		d->total_tables = kdata2_count_tables(d->database);

		do {
			kdata2_table_for_each(d->database)
			{
				if (d->progress)
					d->progress(d->progressp, PPHASE_COUNTING, 
							d->current_table++, d->total_tables);

				strncpy(t.tablename, table->tablename, sizeof(t.tablename));
				for_each_table(&t);
			}
		} while (0);

		list_for_each(t.to_download, node)
		{
			make_downloads(node);
			free(node);
		}
		list_free(&t.to_download);
	}
}
