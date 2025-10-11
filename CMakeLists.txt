cmake_minimum_required(VERSION 3.15)

set(TARGET kdata2)

#To include kdata2 to your project use:
#set(KDATA2_SOURCE_DIR ${CMAKE_SOURCE_DIR}/path/to/kdata)
#add_subdirectory(${KDATA2_SOURCE_DIR})

if (NOT DEFINED KDATA2_SOURCE_DIR)
	SET(KDATA2_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR})
endif ()

project(
	${TARGET} VERSION 1.1 
	DESCRIPTION "C library for SQLite database with Yandex Disk sync"
	HOMEPAGE_URL ""
	LANGUAGES C 
)

SET(CYANDEXDISK_SOURCE_DIR ${KDATA2_SOURCE_DIR}/cYandexDisk)
add_subdirectory(${CYANDEXDISK_SOURCE_DIR})

if(WIN32 OR ANDROID)
	SET(SQLITE_SRC sqlite3.c)
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
	#pthread
	include_directories(${CYANDEXDISK_SOURCE_DIR}/cygwin/include)	
elseif(APPLE)
else()
	if (NOT ANDROID)
		LIST(APPEND ADDLIBS pthread dl)
	endif()
endif()

if(MSVC)
		INCLUDE_DIRECTORIES(${CYANDEXDISK_SOURCE_DIR}/pthreads-win32/include)	
endif()

target_link_libraries(${TARGET} cYandexDisk ${ADDLIBS})

if(${KDATA_BUILD_TEST})
	add_executable(${TARGET}_test test.c)
	target_link_libraries(${TARGET}_test ${TARGET})
endif()
