#include "cYandexDisk/alloc.h"
#include "internal.h"
#include "base64.h"
#include "list.h"
#include "yandexdisk.h"
#include "../../kdata2.h"
#include "cYandexDisk/cYandexDisk.h"
#include "cYandexDisk/cJSON.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

struct ddata_t {
	kdydm_t *d;
	char *tablename;
	char uuid[37];
	time_t timestamp;
	int deleted;
	list_t *to_download;
};

struct ddata_node {
	struct ddata_t *t;
	char tablename[128];
	char uuid[37];
	time_t timestamp;
	int deleted;
};

static void json_to_database(
		struct ddata_node *node, cJSON *object)
{
	char SQL[BUFSIZ];

}

static void parse_json(
		void *json, size_t size, void *data, const char *error)
{
	struct ddata_node *node = data;
	cJSON *object;

	if (error)
		ON_ERR(node->t->d->database, error);

	if (json)
	{
		object = cJSON_ParseWithLength(
				(const char *)json, size);
		if (object){
			json_to_database(node, object);
		}
	}
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

		if (timestamp_local == NULL || 
				atol(timestamp_local) < t->timestamp)
		{
			// add to download list
			struct ddata_node *node = NEW(struct ddata_node);
			if (node){
				node->deleted = t->deleted;
				node->timestamp = t->timestamp;
				strncpy(node->tablename,
					 	t->tablename, sizeof(node->tablename));
				strncpy(node->uuid,
					 	t->uuid, sizeof(node->uuid));

				if (list_add(&t->to_download, node) == 0)
					t->d->total++;
			}
		}
	}

	return 0;
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

		c_yandex_disk_sort_ls(
				t->d->access_token, 
				path, 
				"-name",
				1,
				t, 
				for_each_timestamp);
	}

	return 0;
}

static int for_each_table(struct ddata_t *t)
{
	int ret;
	char path[BUFSIZ];

	snprintf(path, BUFSIZ, "app:/%s/%s",
			t->deleted?DELETED:DATABASE, t->tablename);

	ret = c_yandex_disk_ls(
				t->d->access_token, 
				path, 
				t, 
				for_each_uuid);

	return ret;
}

void download_from_yandex_disk(kdydm_t *d)
{
	int i, ret = 0, deleted[] = {0, 1};

	assert(d);
	assert(d->database);
		
	d->current = 0;
	d->total = 0;
	if (d->progress)
		d->progress(d->progressp, PPHASE_COUNTING, 0, 0);

	for (i = 0; i < 2; ++i) {
		struct ddata_t t;
		struct ddata_node *node = NULL;
		
		memset(&t, 0, sizeof(t));
		t.d = d;
		t.deleted = deleted[i];
		d->current = 0;
		d->total = 0;

		do {
			kdata2_table_for_each(d->database)
			{
				t.tablename = table->tablename;
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
