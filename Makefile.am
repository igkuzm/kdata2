#config to create kdata2 lib with automake
ACLOCAL_AMFLAGS="-I m4"

SUBDIRS = cYandexDisk

lib_LTLIBRARIES = libkdata2.la

libkdata2_la_SOURCES = \
		kdata2.c kdata2.h sqlite3/sqlite3.c

libkdata2_la_CFLAGS =

if WINDOWS
CYD_LINK = -lcYandexDisk -LcYandexDisk/.libs
else
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

libkdata2_la_LIBADD = $(CYD_OBJ) $(CURL_LINK) $(CYD_LINK)
