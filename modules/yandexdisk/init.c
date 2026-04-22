#include "internal.h"
#include "yandexdisk.h"
#include "cYandexDisk/alloc.h"
#include "cYandexDisk/cYandexDisk.h"
#include <assert.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void * thread(void *data);

kdydm_t *
yandex_disk_module_init(
		kdata2_t      * database, 
		const char    * access_token,
		int sec
	  )
{
	kdydm_t *module;
	
	assert(database);
	assert(access_token);

	module = NEW(kdydm_t);
	if (module == NULL)
		return module;

	if (pthread_mutex_init(&module->mutex, NULL))
	{
		free(module);
		return NULL;
	}

	module->database = database;
	strncpy(module->access_token, access_token, 
			sizeof(module->access_token));
	module->sec = sec;

	return module;
}

int yandex_disk_module_start(kdydm_t *d)
{
	assert(d);
	int err = -1;

	d->do_update = 1;
	
	//create new thread
	err = pthread_create(
			&(d->tid), 
			NULL, 
			thread, 
			d);
	if (err) {
		ON_ERR(d->database, STR("can't create thread: %d", err));		
		return err;
	}
	
	return 0;
}

void main_loop(kdydm_t *d)
{
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
	
	upload_to_yandex_disk(d);
}

void * thread(void *data)
{
	kdydm_t *d = data; 
	char SQL[BUFSIZ];

	assert(d);
	assert(d->database);

	/* Create Yandex Disk database */
	c_yandex_disk_mkdir(
						d->access_token, 
						STR("app:/%s", DATABASE ), 
						NULL);
	c_yandex_disk_mkdir(
						d->access_token, 
						STR("app:/%s", DELETED  ), 
						NULL);
	//c_yandex_disk_mkdir(
						//d->access_token, 
						//STR("app:/%s", DATAFILES), 
						//NULL);	

	/* Create YD column in each table */
	do {
		kdata2_table_for_each(d) {
			sprintf(SQL, 
				"ALTER TABLE '%s' "
				"ADD COLUMN 'YANDEX_DISK_UPLOADED' INT;", 
				table->tablename);
			kdata2_sqlite3_exec(d->database, SQL);

			c_yandex_disk_mkdir(
						d->access_token, 
						STR("app:/%s/%s", DATABASE, table->tablename), 
						NULL);
		
			c_yandex_disk_mkdir(
						d->access_token, 
						STR("app:/%s/%s", DELETED, table->tablename), 
						NULL);
		}
	} while (0);
	
	// run main loop
	while (d->do_update) {
		ON_LOG(d->database, "updating data...");	
		main_loop(d);
#ifdef _WIN32
			Sleep(d->sec*1000);
#else
			sleep(d->sec);
#endif
	}

	pthread_exit(0);
}
