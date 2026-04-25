/**
 * File              : yandexdisk.h
 * Author            : Igor V. Sementsov <ig.kuzm@gmail.com>
 * Date              : 21.04.2026
 * Last Modified Date: 25.04.2026
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


#define YANDEX_DISK_UPDATE_SEC 10

typedef struct kdata_yandex_disk_module kdydm_t;

typedef enum progress_phase {
	PPHASE_NULL,
	PPHASE_COUNTING,
	PPHASE_UPLOADING,
	PPHASE_DOWNLOADING,
	PPHASE_DELETING,
} pphase;

kdydm_t EXPORTDLL *
yandex_disk_module_load(
		kdata2_t      * database, 
		const char    * access_token, // Yandex Disk access token,
		void *progressp,
		int (*progress)(
			void *progressp, pphase phase, int current, int total)
	  );

int EXPORTDLL
yandex_disk_module_unload(kdydm_t *module);


int EXPORTDLL
yandex_disk_set_token(kdydm_t *, const char *token);

void EXPORTDLL
yandex_disk_set_file_download_progress(
		kdydm_t *module,
		void *file_progressp,
		int (*file_progress)(  //progress callback function
			void *clientp,		   //data pointer return from progress function
			double dltotal,      //downloaded total size
			double dlnow,		     //downloaded size
			double ultotal,      //uploaded total size
			double ulnow         //uploaded size
		)
	);

	
#ifdef __cplusplus
}  /* end of the 'extern "C"' block */
#endif

#endif /* ifndef YANDEX_DISK_H */
