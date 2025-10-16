cmake_minimum_required(VERSION 3.15)

set(TARGET kdata2)

#To include kdata2 to your project use:
#set(KDATA2_SOURCE_DIR ${CMAKE_SOURCE_DIR}/path/to/kdata)
#add_subdirectory(${KDATA2_SOURCE_DIR})

if (NOT DEFINED KDATA2_SOURCE_DIR)
	set(KDATA2_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR})
endif ()

project(
	${TARGET} VERSION 1.1 
	DESCRIPTION "C library for SQLite database with Yandex Disk sync"
	HOMEPAGE_URL ""
	LANGUAGES C 
)

set(CYANDEXDISK_SOURCE_DIR ${KDATA2_SOURCE_DIR}/cYandexDisk)
add_subdirectory(${CYANDEXDISK_SOURCE_DIR})

include_directories(${KDATA2_SOURCE_DIR})	
include_directories(${KDATA2_SOURCE_DIR}/sqlite3)

if(ANDROID)
	SET(SQLITE_SRC sqlite3/sqlite3.c)
elseif(WIN32)
	SET(SQLITE_SRC sqlite3/sqlite3.c)
else()
	LIST(APPEND ADDLIBS sqlite3)
endif()

if(APPLE)
	SET(CMAKE_EXE_LINKER_FLAGS  "${CMAKE_EXE_LINKER_FLAGS} -framework Foundation")
endif()

add_library(${TARGET} STATIC
	kdata2.c
	${SQLITE_SRC}
)

if(WIN32)
#libz
	add_library(z STATIC IMPORTED)
	set_target_properties(z PROPERTIES IMPORTED_LOCATION
		${CYANDEXDISK_SOURCE_DIR}/win32/libz.a)	
#pthread
	add_library(pthread SHARED IMPORTED)
	set_target_properties(pthread PROPERTIES 
		IMPORTED_LOCATION
		${CYANDEXDISK_SOURCE_DIR}/win32/pthreads-win32/lib/pthreadGC1.dll
		IMPORTED_IMPLIB ${CYANDEXDISK_SOURCE_DIR}/win32/pthreads-win32/lib/libpthreadGC1.a)	
	include_directories(${CYANDEXDISK_SOURCE_DIR}/win32/pthreads-win32/include)	
	list(APPEND ADDLIBS pthread)
#curl
	add_library(curl STATIC IMPORTED)		
	set_target_properties(curl PROPERTIES 
		IMPORTED_LOCATION
		${CYANDEXDISK_SOURCE_DIR}/win32/curl/lib/libcurl.dll.a)
	include_directories(${CYANDEXDISK_SOURCE_DIR}/win32/curl/include)	
elseif(APPLE)
else()
	if (NOT ANDROID)
		list(APPEND ADDLIBS pthread dl)
	endif()
endif()

target_link_libraries(${TARGET} cYandexDisk)

if(${KDATA_BUILD_TEST})
	add_executable(${TARGET}_test test.c)
	target_link_libraries(${TARGET}_test ${TARGET} z curl ${ADDLIBS})
	file(GLOB files 
		"${KDATA2_SOURCE_DIR}/*.jpg"
		)	
	#Copy
	foreach(_file ${files})
			file(INSTALL
				DESTINATION "${CMAKE_BINARY_DIR}"
				FILES "${_file}"
			)
	endforeach()	
endif()
