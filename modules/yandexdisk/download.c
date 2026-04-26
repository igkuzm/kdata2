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
	//char tablename[128];
	/*char uuid[37];*/
	/*time_t timestamp;*/
	time_t start_of_update;
	time_t last_update;
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
						node->t->start_of_update);
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

static int cmp_local_and_remote_timestamp(
		struct ddata_t *t, struct ddata_node *node)
{
	char path[BUFSIZ]; char SQL[BUFSIZ], *timestamp_local;
	
	snprintf(SQL, BUFSIZ, 
			"SELECT timestamp FROM '%s' "
			"WHERE %s = '%s';",
			node->tablename, UUIDCOLUMN, node->uuid);

	timestamp_local = 
		kdata2_get_string(t->d->database, SQL); 
	
	if (timestamp_local == NULL){
		ON_ERR(t->d->database, "corrupted data");
		return 0;
	}

	ON_LOG(t->d->database, 
			STR("Check %s timestamp: %ld(local): %ld(remote) for uuid: %s", 
				node->deleted?"delete":"update",
				timestamp_local?atol(timestamp_local):0,
				node->timestamp, node->uuid));

	if (
			(t->deleted && 
			timestamp_local != NULL && 
			atol(timestamp_local) < node->timestamp) ||

			(t->deleted == 0 &&
			(timestamp_local == NULL || 
			 atol(timestamp_local) < node->timestamp))
		 )
	{
		// add to download list
		ON_LOG(t->d->database, "add to download list"); 
		return 1;
	} 

	return 0;
}

static struct ddata_node *node_from_filename(
		struct ddata_t *t, const char *filename)
{
	int i = 0;
	char *token = NULL;
	struct ddata_node *node = NULL;

	assert(t);
	assert(t->d);
	assert(t->d->database);


	ON_LOG(t->d->database, 
		STR("check timestamp for filename: %s", filename));

	node = NEW(struct ddata_node);
	if (node == NULL){
		ON_ERR(t->d->database, "memory allocation error"); 
		return NULL;
	}

	node->t = t;
	node->deleted = t->deleted;

	strtok_foreach(filename, ".", token){
		switch (i) {
			case 0:
				node->timestamp = atol(token);
				break;
			case 1:
				strncpy(node->tablename, token, sizeof(node->tablename));
				break;
			case 2:
				strncpy(node->uuid, token, sizeof(node->uuid));
				break;
			
			default:
				break;
				
		}

		i++;
	}

	if (i != 3){
		ON_ERR(t->d->database, 
				STR("corrupted data for filename: %s", filename));
		free(node);
		return NULL;
	}

	return node;
}

static int for_each_filename(
		const c_yd_file_t *file, void *data, const char *error)
{
	struct ddata_t *t = data;
	char path[BUFSIZ];

	if (error)
		ON_ERR(t->d->database, error);

	if (file)
	{
		struct ddata_node *node = 
			node_from_filename(t, file->name);
		if (node == NULL)
			return 0;

		// check last update
		ON_LOG(t->d->database, 
				STR("timestamps: %ld(last_update) %ld(remote)", 
				t->last_update, node->timestamp));

		if (node->timestamp <= t->last_update){
			ON_LOG(t->d->database, "no need to download");
			free(node);
			return 1; // stop listing files
		}

		// check local timestamp
		if (cmp_local_and_remote_timestamp(t, node) == 0){
			ON_LOG(t->d->database, "no need to download");
			free(node);
			return 0; // continue listing files
		}

		if (list_add(&t->to_download, node) == 0)
			t->d->total++;
	}

	return 0;
}

static int for_each_file_in_yandex_disk(struct ddata_t *t)
{
	char path[BUFSIZ];

	snprintf(path, BUFSIZ, "app:/%s",
			t->deleted?DELETED:DATABASE);

	ON_LOG(t->d->database, 
			STR("search updates in: %s", path));

	c_yandex_disk_sort_ls(
				t->d->access_token, 
				path, 
				"-name",
				0,
				t, 
				for_each_filename);

	return 0;
}

void download_from_yandex_disk(kdydm_t *d)
{
	int i, ret = 0, updates = 0;
	struct ddata_t t;
	struct ddata_node *node = NULL;
	char SQL[BUFSIZ], *last_update = NULL;
		
	assert(d);
	assert(d->database);
		
	memset(&t, 0, sizeof(t));
	t.d = d;
	t.start_of_update = time(NULL);

	d->current = 0;
	d->total = 0;

	if (d->progress)
		d->progress(d->progressp, PPHASE_COUNTING, 0, 0);

	// get timestamp of last update
	snprintf(SQL, BUFSIZ, 
			"SELECT YANDEX_DISK_UPLOADED FROM _yandexdisk_updates SET");
	last_update = kdata2_get_string(d->database, SQL); 	
	if (last_update)
		t.last_update = atol(last_update);
	
	// check for updates and deleted
	for (i = 0; i < 2; ++i) {
		node = NULL;
		d->current = 0;
		d->total = 0;
		d->current_table = 0;
		d->total_tables = kdata2_count_tables(d->database);
	
		t.deleted = i;

		if (d->progress)
			d->progress(d->progressp, PPHASE_COUNTING, 
					d->current_table++, d->total_tables);

		//strncpy(t.tablename, table->tablename, sizeof(t.tablename));
		for_each_file_in_yandex_disk(&t);

		list_for_each(t.to_download, node)
		{
			make_downloads(node);
			free(node);
		}
		list_free(&t.to_download);
	}
}
