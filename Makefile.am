#config to create static kdata2 lib with automake

noinst_LIBRARIES = libkdata2.a

libkdata2_a_SOURCES = \
		kdata2.c kdata2.h \
		cYandexDisk/cYandexDisk.c\
	  cYandexDisk/cJSON.c\
	  cYandexDisk/uuid4.c

CURL_LINK =

if WINDOWS
	libkdata2_a_SOURCES += sqlite3.c sqlite3.h
else
	CURL_LINK += -lcurl
endif


libkdata2_a_LIBADD = $(CURL_LINK)
