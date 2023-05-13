#config to create kdata2 lib with automake

SUBDIRS = cYandexDisk

lib_LTLIBRARIES = libkdata2.la

libkdata2_la_SOURCES = \
		kdata2.c kdata2.h

CURL_LINK =
SQLITE3_LINK =
CYD_LINK =
CYD_OBJ =

if WINDOWS
	libkdata2_a_SOURCES += sqlite3.c sqlite3.h
	CYD_LINK = -lcYandexDiak -LcYandexDisk/.libs
else
	SQLITE3_LINK += -lsqlite3
	CURL_LINK   += -lcurl
	CYD_OBJ = cYandexDisk/*.o 
endif


libkdata2_la_LIBADD = $(CYD_OBJ) $(CURL_LINK) $(SQLITE3_LINK) $(CYD_LINK)
