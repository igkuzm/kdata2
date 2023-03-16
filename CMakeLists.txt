cmake_minimum_required(VERSION 3.15)

set(TARGET kdata2)

#To include kdata to your project use:
#set(KDATA_SOURCE_DIR ${CMAKE_SOURCE_DIR}/path/to/kdata)
#add_subdirectory(${KDATA_SOURCE_DIR})

if (NOT DEFINED KDATA_SOURCE_DIR)
  set(KDATA_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR})
endif ()

project(
	${TARGET} VERSION 1.0 
	DESCRIPTION "C library for SQLite database with cloud sync"
	HOMEPAGE_URL ""
	LANGUAGES C 
)

set (CMAKE_C_STANDARD 11)

set(CYANDEXDISK_SOURCE_DIR ${KDATA_SOURCE_DIR}/cYandexDisk)
add_subdirectory(${CYANDEXDISK_SOURCE_DIR})

add_subdirectory(${KDATA_SOURCE_DIR}/SQLiteConnect)

add_library(${TARGET} STATIC
	kdata2.c
)
target_link_libraries(${TARGET} cYandexDisk SQLiteConnect)

if(${KDATA_BUILD_TEST})
	add_executable(${TARGET}_test test.c)
	target_link_libraries(${TARGET}_test ${TARGET})
endif()
