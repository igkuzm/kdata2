#include "internal.h"
#include "base64.h"
#include "list.h"
#include "yandexdisk.h"
#include "../../kdata2.h"
#include "cYandexDisk/cYandexDisk.h"
#include "cYandexDisk/cJSON.h"
#include <assert.h>
#include <stdio.h>
#include <time.h>

struct ddata_t {
	kdydm_t *d;
	char *tablename;
	char *uuid;
	time_t timestamp;
	int deleted;
	list_t *to_download;
};

static int for_each_table(struct ddata_t *t)
{
	ret = c_yandex_disk_ls(
				d->access_token, 
				const char *path, 
				void *user_data, 
				int (*callback)(const c_yd_file_t *, void *, const char *));
}

void download_from_yandex_dosk(kdydm_t *d)
{
	int i, ret = 0, ivars[] = {1, 0};
	char path[BUFSIZ], *svars[] = {DELETED, DATABASE};

	assert(d);
	assert(d->database);
		
	d->current = 0;
	d->total = 0;
	if (d->progress)
		d->progress(d->progressp, PPHASE_COUNTING, 0, 0);

	for (i = 0; i < 2; ++i) {
		struct ddata_t t;

	
	}
}
