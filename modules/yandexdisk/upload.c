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
	struct kdata2_table *table;
	int uploaded;
};

int upload_json(
		kdydm_t *d, 
		const char *tablename, 
		const char *uuid,
		time_t timestamp,
		char *json)
{
	char path[BUFSIZ], *error = NULL;

	sprintf(path, "app:/%s/%s/%s", 
			DATABASE, tablename, uuid);
	
	c_yandex_disk_mkdir(
						d->access_token, 
						STR("app:/%s", DATABASE ), 
						&error);

	if (error){
		ON_ERR(d->database, error);
		free(error);
		return 1;
	}

	sprintf(path, "%s/%ld", path, timestamp);

	return c_yandex_disk_upload_data(
			d->access_token, 
			json, 
			strlen(json), 
			path, 
			true, 
			true, 
			NULL, 
			NULL, 
			NULL, 
			NULL);
}

int upload_to_yandex_disk_cb(
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
	char *json = NULL;
	cJSON *object = NULL;
	
	assert(t);
	assert(t->d);
	assert(t->d->database);
	
	ON_LOG(t->d->database, STR("uploading '%s' table data with uuid: %s", 
		   t->table->tablename, values[0]));
	
	object = cJSON_CreateObject();
	if (object == NULL){
		ON_ERR(t->d->database, "can't init JSON");
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
			
	timestamp = *(long *)(values[i-1]);
	//ON_LOG(t->d->database, cJSON_Print(json));
	
	json = cJSON_Print(object);
	if (upload_json(t->d, 
				t->table->tablename, 
				values[0], 
				timestamp,
				json))
	{

	}

	cJSON_free(object);
	free(json);
	
	return 0;
}

void upload_to_yandex_disk(kdydm_t *d)
{
	char SQL[BUFSIZ];

	assert(d);
	assert(d->database);
	
	do {
		kdata2_table_for_each(d) {
			struct udata_t t;
			t.d = d;
			t.table = table;
			t.uploaded = 0;
			
			sprintf(SQL, "SELECT %s, ", UUIDCOLUMN);
			do {
				kdata2_column_for_each(table) {
					strcat(SQL, column->columnname);
					strcat(SQL, ", ");
				}
				sprintf(SQL, "%stimestamp FROM '%s' "
					   "WHERE 'YANDEX_DISK_UPLOADED' != 1;",
					   SQL, table->tablename);
				
			} while(0);
			
			kdata2_get(d->database, SQL, &t, upload_to_yandex_disk_cb);

		}
	 } while(0);
}
