#include "internal.h"
#include "base64.h"
#include "yandexdisk.h"
#include "../../kdata2.h"
#include "../../str.h"
#include "cYandexDisk/cYandexDisk.h"
#include "cYandexDisk/cJSON.h"
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

struct udata_t {
	kdydm_t *d;
	char *tablename;
	char *uuid;
	time_t timestamp;
	int deleted;
	int uploaded;
	struct str list_of_updates;
};

static int upload_json(
		kdydm_t *d, 
		const char *tablename, 
		const char *uuid,
		time_t timestamp,
		int deleted,
		char *json)
{
	int res = 0;
	char path[BUFSIZ];

	if (d->progress)
		d->progress(
				d->progressp, 
				PPHASE_UPLOADING, 
				d->current++, 
				d->total);
	
	if (deleted)
		sprintf(path, "app:/%s/%s/%s", 
				DELETED, tablename, uuid);
	else
		sprintf(path, "app:/%s/%s/%s", 
				DATABASE, tablename, uuid);
	
	ON_LOG(d->database, STR("create path: %s", 
		   path));
	c_yandex_disk_mkdir(
						d->access_token, 
						path, 
						NULL);

	snprintf(path, BUFSIZ, 
			"%s/%ld", path, timestamp);

	ON_LOG(d->database, STR("upload json to path: %s", 
		   path));
	res =  c_yandex_disk_upload_data(
			d->access_token, 
			json, 
			strlen(json), 
			path, 
			false, 
			true, 
			NULL, 
			NULL, 
			d->file_progressp, 
			d->file_progress);

	if (d->progress)
		d->progress(
				d->progressp, 
				PPHASE_UPLOADING, 
				d->current, 
				d->total);

	return res;
}

static int upload_data_row_to_yandex_disk(
				void *user_data,
				int	num_cols,
				enum KDATA2_TYPE types[],
				const char *columns[], 
				void *values[],
				size_t sizes[]
				)
{
	int i;
	time_t timestamp = 0;
	struct udata_t *t = user_data;
	char *json = NULL, *uuid = NULL;
	cJSON *object = NULL;
	
	assert(t);
	assert(t->d);
	assert(t->d->database);
	assert(values);
	
	if (values[0] == NULL || values[num_cols-1] == NULL)
	{
		ON_ERR(t->d->database, "broken table row data");
		return 1;
	}
	
	ON_LOG(t->d->database, STR("%s '%s' table data with uuid: %s", 
				t->deleted?"deleting":"uploading",
		    t->tablename, values[0]));

	object = cJSON_CreateObject();
	if (object == NULL){
		ON_ERR(t->d->database, "can't init JSON");
		return 1;
	}
	
	for (i=0; i<num_cols; ++i) {
		cJSON *item = NULL;
		char *base64 = NULL;
		size_t length = 0;

		if (values[i] == NULL)
			continue;
		
		switch (types[i]) {
			case KDATA2_TYPE_NUMBER:
				item = cJSON_CreateNumber(*(long *)(values[i]));
				break;
			case KDATA2_TYPE_FLOAT:
				item = cJSON_CreateNumber(*(double *)(values[i]));
				break;
			case KDATA2_TYPE_TEXT:
				item = cJSON_CreateString((char *)(values[i]));
				break;
			case KDATA2_TYPE_DATA:
			{
				base64 = base64_encode((unsigned char *)(values[i]), sizes[i], &length);
				if (base64){
					item = cJSON_CreateString(base64);
					free(base64);
				}
				break;			
			}
			default:
				break;
		}
		if (item)
			cJSON_AddItemToObject(object, columns[i], item);
	}
			
	uuid      =  (char *)(values[0]);
	timestamp = *(long *)(values[i-1]);
	//ON_LOG(t->d->database, cJSON_Print(json));
	
	json = cJSON_Print(object);
	if (upload_json(t->d, 
				t->tablename, 
				uuid, 
				timestamp,
				t->deleted,
				json))
	{
		// on error
		ON_LOG(t->d->database, "can't upload data");
	} else {
		// set as uploaded
		char SQL[BUFSIZ];
		ON_LOG(t->d->database, "data uploaded!");
		sprintf(SQL, 
				"UPDATE '%s' SET YANDEX_DISK_UPLOADED = 1 "
				"WHERE %s = '%s';", 
				t->tablename, UUIDCOLUMN, uuid);
		kdata2_sqlite3_exec(t->d->database, SQL);
		t->uploaded = 1;

		// add to update rows
		str_appendf(&t->list_of_updates, "%s/%s/%s\n",
				t->deleted?DELETED:DATABASE, 
				t->tablename, uuid);
	}

	cJSON_free(object);
	free(json);
	
	return 0;
}

static int get_updates(
				void *user_data,
				int	num_cols,
				enum KDATA2_TYPE types[],
				const char *columns[], 
				void *values[],
				size_t sizes[]
				)
{
	char SQL[BUFSIZ], *request = NULL;
	kdydm_t *d = user_data;
	struct udata_t t;
	struct str s;

	assert(d);
	assert(d->database);

	if (str_init(&s)){
		ON_ERR(d->database, "can't allocate memory");
		return 1;
	}
	t.d = d;
	t.tablename = values[0];
	t.uuid = values[1];
	t.timestamp = *(long *)values[2];
	t.deleted = *(long *)values[4];
	t.uploaded = 0;

	if (t.deleted)
	{
		char *json = "0";
		if (upload_json(t.d, 
					t.tablename, 
					t.uuid, 
					t.timestamp,
					t.deleted,
					json))
		{
			// on error
			ON_LOG(t.d->database, "can't delete data");
		} else {
			t.uploaded = 1;
		}
		goto get_updates_end;
	}

	/* get row from table and upload to YD */
	request = kdata2_sql_select_table_request(
				t.d->database, t.tablename);
	if (request == NULL)
		return 0;

	str_append(&s, request, strlen(request));
	free(request);
	
	str_appendf(&s, "WHERE %s = '%s'", 
			UUIDCOLUMN, t.uuid);
	kdata2_get(d->database, s.str, 
			&t, upload_data_row_to_yandex_disk);
	free(s.str);
	
get_updates_end:
	if (t.uploaded) {
		// remove from _kdata2_updates
		sprintf(SQL, "UPDATE _kdata2_updates "
				"SET YANDEX_DISK_UPLOADED = %ld "
				"WHERE uuid = '%s'", t.timestamp, t.uuid);
		kdata2_sqlite3_exec(d->database, SQL);
	}
	
	return 0;
}

static void save_update_rows(struct udata_t *t)
{
	int res;
	char path[BUFSIZ];

	assert(t);
	assert(t->d);
	assert(t->d->database);
		
	sprintf(path, "app:/%s/%ld", 
				UPDATES, t->d->timestamp);

	res =  c_yandex_disk_upload_data(
			t->d->access_token, 
			t->list_of_updates.str, 
			t->list_of_updates.len, 
			path, 
			false, 
			true, 
			NULL, 
			NULL, 
			t->d->file_progressp, 
			t->d->file_progress);

	if (res){
		ON_LOG(t->d->database, 
				STR("can't upload update to path: %s", path));
	}
}

void upload_to_yandex_disk(kdydm_t *d)
{
	char SQL[BUFSIZ], *count = NULL, *request = NULL;

	assert(d);
	assert(d->database);

	// updates
	d->current = 0;
	d->total = 0;
	if (d->progress)
		d->progress(d->progressp, PPHASE_COUNTING, 0, 1);

	sprintf(SQL, 
			"SELECT COUNT(*) FROM _kdata2_updates "
			"WHERE (YANDEX_DISK_UPLOADED IS NULL "
			"OR YANDEX_DISK_UPLOADED != timestamp)");
	count = kdata2_get_string(d->database, SQL);
	d->total = atoi(count);
	free(count);
	if (d->progress)
		d->progress(d->progressp, PPHASE_COUNTING, 1, 1);

	ON_LOG(d->database, 
			STR("Found %d new rows for upload", d->total));
	
	if (d->total){
		sprintf(SQL, 
				"SELECT * FROM _kdata2_updates "
				"WHERE (YANDEX_DISK_UPLOADED IS NULL "
				"OR YANDEX_DISK_UPLOADED != timestamp)");
		kdata2_get(d->database, SQL, 
				d, get_updates);
	}
	
	// new records in tables
	d->current = 0;
	d->total_tables = kdata2_count_tables(d->database);
	do {
		kdata2_table_for_each(d->database) {
			struct udata_t t;
			struct str s;

			if (str_init(&t.list_of_updates)){
		    ON_ERR(d->database, "allocation error");	
				continue;
			}

			if (str_init(&s))
				continue;;
	
			t.d = d;
			t.tablename = table->tablename;
			t.deleted = 0;
			t.uploaded = 0;
			t.timestamp = time(NULL);

			d->current = 0;
			d->total = 0;
			if (d->progress)
				d->progress(d->progressp, PPHASE_COUNTING, 
						d->current_table++, d->total_tables);

			snprintf(SQL, BUFSIZ,
				"SELECT COUNT(*) FROM '%s' "
				"WHERE (YANDEX_DISK_UPLOADED IS NULL "
				"OR YANDEX_DISK_UPLOADED = 0) ", table->tablename);
			count = kdata2_get_string(d->database, SQL);
			d->total = atoi(count);
			free(count);
			ON_LOG(d->database, 
				STR("Found %d new rows for upload", d->total));

			if (d->total == 0)
				continue;
			
			request = kdata2_sql_select_table_request(
						d->database, table->tablename);
			if (request == NULL)
			{
				ON_ERR(d->database, "SQL request is NULL");
				continue;
			}

			str_append(&s, request, strlen(request));
			free(request);

			str_appendf(&s, "timestamp FROM '%s' "
					"WHERE (YANDEX_DISK_UPLOADED IS NULL "
					"OR YANDEX_DISK_UPLOADED = 0);",
					 table->tablename);
				
			kdata2_get(d->database, s.str, 
					&t, upload_data_row_to_yandex_disk);
			free(s.str);

			save_update_rows(&t);
			free(t.list_of_updates.str);
		}
	 } while(0);
}
