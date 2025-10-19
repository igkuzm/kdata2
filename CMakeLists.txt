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

if(APPLE)
	SET(CMAKE_EXE_LINKER_FLAGS  "${CMAKE_EXE_LINKER_FLAGS} -framework Foundation")
endif()

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

# Determine the location of the libraries to use based on the platform.
if(ANDROID)
#sqlite3
	list(APPEND ADDSRC sqlite3/sqlite3.c)	
elseif(WIN32)
	if (CMAKE_BUILD_TYPE STREQUAL "WINNT")
		set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DTARGETWINNT")
		set(WINDIR winnt)
	else()
		set(WINDIR win32)
	endif()
#sqlite3
	list(APPEND ADDSRC sqlite3/sqlite3.c)	
#pthreads
#	find_package(Threads)
#	if (NOT Threads_FOUND)
		add_library(pthread SHARED IMPORTED)
		set_target_properties(pthread PROPERTIES 
			IMPORTED_LOCATION
			${CYANDEXDISK_SOURCE_DIR}/${WINDIR}/pthreads-win32/lib/pthreadGC1.dll
			IMPORTED_IMPLIB ${CYANDEXDISK_SOURCE_DIR}/${WINDIR}/pthreads-win32/lib/libpthreadGC1.a)	
		include_directories(${CYANDEXDISK_SOURCE_DIR}/${WINDIR}/pthreads-win32/include)	
#	endif()
	list(APPEND ADDLIBS pthread)
else()
	find_package(CURL)
	include_directories(${CURL_INCLUDE_DIRS})
	list(APPEND ADDLIBS curl)	
	list(APPEND ADDLIBS z)	
endif()

add_library(${TARGET} STATIC
	kdata2.c
	${ADDSRC}
)

target_link_libraries(${TARGET} cYandexDisk ${ADDLIBS})

if(${BUILD_TEST})
	add_executable(${TARGET}_test test.c)
	target_link_libraries(${TARGET}_test ${TARGET} ${ADDLIBS})
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
