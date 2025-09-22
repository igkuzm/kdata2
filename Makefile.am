#config to create kdata2 lib with automake
ACLOCAL_AMFLAGS="-I m4"

SUBDIRS = cYandexDisk

lib_LTLIBRARIES = libkdata2.la

libkdata2_la_SOURCES = \
		kdata2.c kdata2.h

libkdata2_la_CFLAGS =

if WINDOWS
libkdata2_la_SOURCES += sqlite3.c sqlite3.h
CYD_LINK = -lcYandexDisk -LcYandexDisk/.libs
else
SQLITE3_LINK = -lsqlite3
CURL_LINK   = -lcurl
CYD_OBJ = cYandexDisk/*.o 
endif

if WIN32
libkdata2_la_CFLAGS += \
	-IcYandexDisk/curl/windows/win32/include -IcYandexDisk/pthreads-win32/include
endif

if WIN64
libkdata2_la_CFLAGS += \
	-IcYandexDisk/curl/windows/win64/include -IcYandexDisk/pthreads-win32/include
endif

libkdata2_la_LIBADD = $(CYD_OBJ) $(CURL_LINK) $(SQLITE3_LINK) $(CYD_LINK)
