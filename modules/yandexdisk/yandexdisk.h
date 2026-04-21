/**
 * File              : yandexdisk.h
 * Author            : Igor V. Sementsov <ig.kuzm@gmail.com>
 * Date              : 21.04.2026
 * Last Modified Date: 21.04.2026
 * Last Modified By  : Igor V. Sementsov <ig.kuzm@gmail.com>
 */
/**
 * yandexdisk.h
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

/* YandexDisk module */

#ifndef YANDEX_DISK_H
#define YANDEX_DISK_H
#include "../../kdata2.h"

#ifdef __cplusplus
extern "C" {
#endif

#ifdef _MSC_VER
#undef EXPORTDLL
#define EXPORTDLL __declspec(dllexport)
#else
#undef EXPORTDLL
#define EXPORTDLL
#endif

typedef struct kdata_yandex_disk_module kdydm_t;

kdydm_t EXPORTDLL *
yandex_disk_module_init(
		kdata2_t * database, 
		const char    * access_token, // Yandex Disk access token,
		int sec                       // number of seconds of delay to sinc data with Yandex Disk
	  );

int EXPORTDLL
yandex_disk_module_start(kdydm_t *module);

	
#ifdef __cplusplus
}  /* end of the 'extern "C"' block */
#endif

#endif /* ifndef YANDEX_DISK_H */
