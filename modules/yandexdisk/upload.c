#include "internal.h"
#include "base64.h"
#include "yandexdisk.h"
#include "../../kdata2.h"
#include "cYandexDisk/cYandexDisk.h"
#include "cYandexDisk/cJSON.h"
#include <assert.h>
#include <stdio.h>
#include <time.h>

struct udata_t {
	kdydm_t *d;
	char *tablename;
	char *uuid;
	time_t timestamp;
	int deleted;
	int uploaded;
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

	sprintf(path, "%s/%ld", path, timestamp);

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
			NULL, 
			NULL);

	if (d->progress)
		d->progress(
				d->progressp, 
				PPHASE_UPLOADING, 
				d->current++, 
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
	char SQL[BUFSIZ];
	kdydm_t *d = user_data;
	struct udata_t t;

	assert(d);
	assert(d->database);
	
	t.d = d;
	t.tablename = values[0];
	t.uuid = values[1];
	t.timestamp = *(long *)values[2];
	t.deleted = *(long *)values[4];
	t.uploaded = 0;

	if (t.deleted)
	{
		char *json = "";
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
	if (kdata2_sql_select_table_request(
				t.d->database, SQL, t.tablename))
		return 0;
	
	sprintf(SQL, "%sWHERE %s = '%s'", 
			SQL, UUIDCOLUMN, t.uuid);
	kdata2_get(d->database, SQL, 
			&t, upload_data_row_to_yandex_disk);
	
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

void upload_to_yandex_disk(kdydm_t *d)
{
	char SQL[BUFSIZ], *count;

	assert(d);
	assert(d->database);
	
	// updates
	d->current = 0;
	d->total = 0;

	if (d->progress)
		d->progress(d->progressp, PPHASE_COUNTING, 0, 0);

	sprintf(SQL, 
			"SELECT COUNT(*) FROM _kdata2_updates "
			"WHERE (YANDEX_DISK_UPLOADED IS NULL "
			"OR YANDEX_DISK_UPLOADED != timestamp)");
	count = kdata2_get_string(d->database, SQL);
	d->total = atoi(count);
	free(count);
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
	do {
		kdata2_table_for_each(d->database) {
			struct udata_t t;
			t.d = d;
			t.tablename = table->tablename;
			t.deleted = 0;
			t.uploaded = 0;
			d->current = 0;
			d->total = 0;

			if (d->progress)
				d->progress(d->progressp, PPHASE_COUNTING, 0, 0);

			sprintf(SQL, 
				"SELECT COUNT(*) FROM '%s' "
				"WHERE (YANDEX_DISK_UPLOADED IS NULL "
				"OR YANDEX_DISK_UPLOADED = 0)", table->tablename);
			count = kdata2_get_string(d->database, SQL);
			d->total = atoi(count);
			free(count);
			ON_LOG(d->database, 
				STR("Found %d new rows for upload", d->total));

			if (d->total == 0)
				break;
			
			sprintf(SQL, "SELECT %s, ", UUIDCOLUMN);
			do {
				kdata2_column_for_each(table) {
					strcat(SQL, column->columnname);
					strcat(SQL, ", ");
				}
				sprintf(SQL, "%stimestamp FROM '%s' "
						"WHERE (YANDEX_DISK_UPLOADED IS NULL "
						"OR YANDEX_DISK_UPLOADED = 0);",
					   SQL, table->tablename);
				
			} while(0);
			
			kdata2_get(d->database, SQL, 
					&t, upload_data_row_to_yandex_disk);

		}
	 } while(0);
}
