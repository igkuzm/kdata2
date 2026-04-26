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
	upload_to_yandex_disk(d);
	download_from_yandex_disk(d);
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
						STR("app:/%s", DELETED  ), 
						NULL);
	c_yandex_disk_mkdir(
						d->access_token, 
						STR("app:/%s", UPDATES), 
						NULL);	
		
	sprintf(SQL, 
			"CREATE TABLE IF NOT EXISTS "
			  "_yandexdisk_updates (id INT); "
				"ALTER TABLE _yandexdisk_updates "
				"ADD COLUMN 'YANDEX_DISK_UPLOADED' Int;");
	kdata2_sqlite3_exec(d->database, SQL);

	sprintf(SQL, 
				"ALTER TABLE _kdata2_updates "
				"ADD COLUMN 'YANDEX_DISK_UPLOADED' INT;");
	kdata2_sqlite3_exec(d->database, SQL);

	/* Create YD column in each table */
	do {
		kdata2_table_for_each(d->database) {
			sprintf(SQL, 
				"ALTER TABLE '%s' "
				"ADD COLUMN 'YANDEX_DISK_UPLOADED' INT;", 
				table->tablename);
			kdata2_sqlite3_exec(d->database, SQL);
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

kdydm_t * yandex_disk_module_load(
		kdata2_t    * database, 
		const char  * access_token, // Yandex Disk access token,
		void *progressp,
		int (*progress)(
			void *progressp, pphase phase, int current, int total)
	  )
{
	kdydm_t *d = yandex_disk_module_init(
			database, access_token, YANDEX_DISK_UPDATE_SEC);
	if (d)
	{
		d->progressp = progressp;
		d->progress  = progress;
		yandex_disk_module_start(d);
	}

	return d;
}


int yandex_disk_module_unload(kdydm_t *d)
{
	if (d)
	{
		d->do_update = 0;
		pthread_join(d->tid, NULL);
		free(d);
		return 0;
	}

	return 1;
}

void yandex_disk_set_file_download_progress(
		kdydm_t *module,
		void *file_progressp,
		int (*file_progress)( 
			void *clientp,		 
			double dltotal,   
			double dlnow,		 
			double ultotal, 
			double ulnow        
		))
{
	assert(module);
	module->file_progressp = file_progressp;
	module->file_progressp = file_progress;
}


int yandex_disk_set_token(kdydm_t *d, const char *token)
{
	if (d == NULL)
		return 1;
	strncpy(d->access_token, token, sizeof(d->access_token));
	return 0;
}
