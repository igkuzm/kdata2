#config to create kdata2 lib with automake
ACLOCAL_AMFLAGS="-I m4"
AUTOMAKE_OPTIONS = subdir-objects

SUBDIRS = 
lib_LTLIBRARIES = libkdata2.la

nobase_include_HEADERS = \
  kdata2.h \
  cJSON.h \
  log.h \
  sqlite3/sqlite3.h \

libkdata2_la_SOURCES = \
		kdata2.c sqlite3/sqlite3.c cJSON.c

libkdata2_la_CFLAGS = -I$(top_srcdir)

if WINDOWS
else
CURL_LINK   = -lcurl
endif

if WIN32
libkdata2_la_CFLAGS += \
	-IcYandexDisk/curl/windows/win32/include -IcYandexDisk/pthreads-win32/include
endif

if WIN64
libkdata2_la_CFLAGS += \
	-IcYandexDisk/curl/windows/win64/include -IcYandexDisk/pthreads-win32/include
endif

if WITH_TEST
libkdata2_la_SOURCES += test.c 
endif

if WITH_YANDEX_DISK
libkdata2_la_SOURCES += \
		modules/yandexdisk/init.c \
		modules/yandexdisk/upload.c \
		modules/yandexdisk/download.c \
		modules/yandexdisk/cYandexDisk/cYandexDisk.c \
		modules/yandexdisk/cYandexDisk/cYandexOAuth.c
endif

if WITH_YCLIENTS
libkdata2_la_SOURCES += \
		modules/yclients/cYclients/src/auth.c \
		modules/yclients/cYclients/src/categories.c \
		modules/yclients/cYclients/src/clients.c \
		modules/yclients/cYclients/src/companies.c \
		modules/yclients/cYclients/src/curl_transport.c \
		modules/yclients/cYclients/src/custom_fields.c \
		modules/yclients/cYclients/src/records.c \
		modules/yclients/cYclients/src/services.c \
		modules/yclients/cYclients/src/structs.c \
		modules/yclients/cYclients/src/stb_ds.c	
endif

libkdata2_la_LIBADD = $(CURL_LINK)
