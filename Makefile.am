#config to create static kdata2 lib with automake

noinst_LIBRARIES = libkdata2.a

libkdata2_a_SOURCES = \
		kdata2.c kdata2.h \
		sqlite3.c sqlite3.h \
		cYandexDisk/cYandexDisk.c\
	  cYandexDisk/cJSON.c\
	  cYandexDisk/uuid4.c

libkdata2_a_LDADD = -lcurl
