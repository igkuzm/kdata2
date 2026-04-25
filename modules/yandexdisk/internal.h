/**
 * File              : internal.h
 * Author            : Igor V. Sementsov <ig.kuzm@gmail.com>
 * Date              : 21.04.2026
 * Last Modified Date: 25.04.2026
 * Last Modified By  : Igor V. Sementsov <ig.kuzm@gmail.com>
 */
/**
 * modules/yandexdisk/struct.h
 * Copyright (c) 2026 Igor V. Sementsov <ig.kuzm@gmail.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
#ifndef YANDEX_DISK_STRUCT_H
#define YANDEX_DISK_STRUCT_H
#include "../../kdata2.h"
#include "cYandexDisk/log.h"
#include <bits/types/time_t.h>
#include <pthread.h>
#include "yandexdisk.h"
#include "../../str.h"

#define ON_ERR(ptr, msg) \
	if (ptr->on_error) ptr->on_error(ptr->on_error_data, msg);
#define ON_LOG(ptr, msg) \
	if (ptr->on_log) ptr->on_log(ptr->on_log_data, msg);

#ifdef _WIN32
#include <windows.h>
#ifndef bool
#define bool char
#define true 1
#define false 0
#endif
#else // not WIN32
#include <unistd.h>
#include <stdbool.h>
#endif // WIN32

#define DATABASE  "database"
#define DELETED   "deleted"
#define UPDATES   "updates"

struct kdata_yandex_disk_module{
	kdata2_t *database;
	char access_token[64];         // Yandex Disk access token
	int do_update;                 // set to false to stop
	int sec;
	time_t timestamp;
	struct str rows;
	pthread_t tid;
	pthread_mutex_t mutex;
	int current;
	int total;
	int current_table;
	int total_tables;
	void *progressp;
	int (*progress)(
		void *progressp, pphase phase, int current, int total);
	void *file_progressp;
	int (*file_progress)(  //progress callback function
			void *clientp,		   //data pointer return from progress function
			double dltotal,        //downloaded total size
			double dlnow,		   //downloaded size
			double ultotal,        //uploaded total size
			double ulnow           //uploaded size
		);
};

void upload_to_yandex_disk(struct kdata_yandex_disk_module *);
void download_from_yandex_disk(struct kdata_yandex_disk_module *);

#endif /* ifndef YANDEX_DISK_STRUCT_H */
