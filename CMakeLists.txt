cmake_minimum_required(VERSION 3.0)

set(TARGET kdata2)

#To include kdata2 to your project use:
#set(KDATA2_SOURCE_DIR ${CMAKE_SOURCE_DIR}/path/to/kdata)
#add_subdirectory(${KDATA2_SOURCE_DIR})

if (NOT DEFINED KDATA2_SOURCE_DIR)
	set(KDATA2_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR})
endif ()

project(
	${TARGET} VERSION 1.2 
	DESCRIPTION "C library for SQLite database with Yandex Disk sync"
	LANGUAGES C 
)

if (CMAKE_BUILD_TYPE STREQUAL "Debug")
  list(APPEND CMAKE_C_FLAGS "-DDEBUG")
endif()

if(APPLE)
	SET(CMAKE_EXE_LINKER_FLAGS  "${CMAKE_EXE_LINKER_FLAGS} -framework Foundation")
endif()

if(${WITH_YANDEX_DISK})
	list(APPEND ADDSRC modules/yandexdisk/init.c)	
	list(APPEND ADDSRC modules/yandexdisk/upload.c)	
	list(APPEND ADDSRC modules/yandexdisk/download.c)	
	list(APPEND ADDSRC modules/yandexdisk/cYandexDisk/cYandexDisk.c)	
	list(APPEND ADDSRC modules/yandexdisk/cYandexDisk/cYandexOAuth.c)	
endif()

if(${WITH_YCLIENTS})
	list(APPEND ADDSRC modules/yclients/cYclients/src/auth.c)	
	list(APPEND ADDSRC modules/yclients/cYclients/src/categories.c)	
	list(APPEND ADDSRC modules/yclients/cYclients/src/clients.c)	
	list(APPEND ADDSRC modules/yclients/cYclients/src/companies.c)	
	list(APPEND ADDSRC modules/yclients/cYclients/src/curl_transport.c)	
	list(APPEND ADDSRC modules/yclients/cYclients/src/custom_fields.c)	
	list(APPEND ADDSRC modules/yclients/cYclients/src/records.c)	
	list(APPEND ADDSRC modules/yclients/cYclients/src/services.c)	
	list(APPEND ADDSRC modules/yclients/cYclients/src/structs.c)	
	list(APPEND ADDSRC modules/yclients/cYclients/src/stb_ds.c)	
endif()

include_directories(${CMAKE_CURRENT_SOURCE_DIR})	
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/sqlite3)

SET(SQLITE_SRC sqlite3/sqlite3.c)

# Determine the location of the libraries to use based on the platform.
if(ANDROID)
	ADD_LIBRARY(curl STATIC IMPORTED)
	SET_TARGET_PROPERTIES(curl PROPERTIES IMPORTED_LOCATION
		${CMAKE_CURRENT_SOURCE_DIR}/modules/yandexdisk/cYandexDisk/android/curl/${ANDROID_ABI}/libcurl.a)
	INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR}/modules/yandexdisk/cYandexDisk/android/curl/include)
	set(LIBZ z)
elseif(IOS)
	ADD_LIBRARY(curl STATIC IMPORTED)
	SET_TARGET_PROPERTIES(curl PROPERTIES IMPORTED_LOCATION 
		${CMAKE_CURRENT_SOURCE_DIR}/modules/yandexdisk/cYandexDisk/iOS/curl/libcurl.a)
	INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR}/modules/yandexdisk/cYandexDisk/iOS/curl/include)
elseif(WIN32)
	if (CMAKE_BUILD_TYPE STREQUAL "WINNT")
		set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DTARGETWINNT")
		set(WINDIR winnt)
	else()
		set(WINDIR win32)
	endif()
#libz
	add_library(z STATIC IMPORTED)
	set_target_properties(z PROPERTIES IMPORTED_LOCATION
		${CMAKE_CURRENT_SOURCE_DIR}/modules/yandexdisk/cYandexDisk/${WINDIR}/libz.a)
#pthreads - for winxp/winnt
find_package(Threads)
	if (Threads_FOUND)
		include_directories(${Threads_INCLUDE_DIR})
	else()
		add_library(pthread SHARED IMPORTED)
		set_target_properties(pthread PROPERTIES 
			IMPORTED_LOCATION
			${CMAKE_CURRENT_SOURCE_DIR}/modules/yandexdisk/cYandexDisk/${WINDIR}/pthreads-win32/lib/pthreadGC1.dll
			IMPORTED_IMPLIB ${CMAKE_CURRENT_SOURCE_DIR}/modules/yandexdisk/cYandexDisk/${WINDIR}/pthreads-win32/lib/libpthreadGC1.a)	
		include_directories(${CMAKE_CURRENT_SOURCE_DIR}/modules/yandexdisk/cYandexDisk/${WINDIR}/pthreads-win32/include)	
	endif()
	list(APPEND ADDLIBS pthread)
#curl
add_library(curl STATIC IMPORTED)
set_target_properties(curl PROPERTIES 
		IMPORTED_LOCATION
		${CMAKE_CURRENT_SOURCE_DIR}/modules/yandexdisk/cYandexDisk/${WINDIR}/curl/lib/libcurl.dll.a)	
	include_directories(${CMAKE_CURRENT_SOURCE_DIR}/modules/yandexdisk/cYandexDisk/${WINDIR}/curl/include)
else()
	find_package(CURL)
	include_directories(${CURL_INCLUDE_DIRS})
endif()

add_library(${TARGET} STATIC
	kdata2.c
	uuid4.c
	cJSON.c
	${SQLITE_SRC}
	${ADDSRC}
)

target_link_libraries(${TARGET} ${ADDLIBS} curl pthread)

#copy files
if (WIN32)
	#curl libs
	#Copy DLLs
	file(GLOB files 
		"${CMAKE_CURRENT_SOURCE_DIR}/modules/yandexdisk/cYandexDisk/${WINDIR}/curl/bin/*.dll"
		"${CMAKE_CURRENT_SOURCE_DIR}/modules/yandexdisk/cYandexDisk/${WINDIR}/curl/lib/*.dll"
		"${CMAKE_CURRENT_SOURCE_DIR}/modules/yandexdisk/cYandexDisk/${WINDIR}/pthreads-win32/lib/pthreadGC1.dll"
		"${CMAKE_CURRENT_SOURCE_DIR}/modules/yandexdisk/cYandexDisk/${WINDIR}/pthreads-win32/bin/libwinpthread-1.dll"
		)	
	#Copy
	foreach(_file ${files})
			file(INSTALL
				DESTINATION "${CMAKE_BINARY_DIR}"
				FILES "${_file}"
			)
	endforeach()	
endif()

if(${WITH_TEST})
	add_executable(${TARGET}_test test.c)
	target_link_libraries(${TARGET}_test ${TARGET} ${ADDLIBS})
	file(GLOB files 
		"${TARGET_SOURCE_DIR}/*.jpg"
		)	
	#Copy
	foreach(_file ${files})
			file(INSTALL
				DESTINATION "${CMAKE_BINARY_DIR}"
				FILES "${_file}"
			)
	endforeach()	
endif()
