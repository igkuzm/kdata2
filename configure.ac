dnl Process this file with autoconf to produce a configure script.

AC_INIT([libkdata2], [1.1])
AM_INIT_AUTOMAKE
AM_MAINTAINER_MODE

LT_INIT
AC_CONFIG_MACRO_DIRS([m4])

AC_PROG_CC
AC_PROG_CXX

AC_CONFIG_SRCDIR

AC_ARG_ENABLE([withtest], 
							[AS_HELP_STRING([--with-test], [Build
							 'test' executable @<:@no@:>@])],
							 [with_test=yes],
							 [with_test=no])

AM_CONDITIONAL([WITH_TEST], [test "$with_test" = "yes"])

AC_ARG_ENABLE([yandexdisk], 
							[AS_HELP_STRING([--with-yandexdisk], [Build
							 Yandex Disk support @<:@no@:>@])],
							 [with_yandexdisk=yes],
							 [with_yandexdisk=no])

AM_CONDITIONAL([WITH_YANDEX_DISK], [test "$with_yandexdisk" = "yes"])

AC_CANONICAL_HOST
case "${host_os}" in
    cygwin*|mingw*)
        build_windows=yes;;
    darwin*)
        build_mac=yes;;
    *)
		AC_CHECK_HEADER([curl/curl.h],[],[AC_MSG_ERROR([Please install libcurl])],[])
		AC_CHECK_HEADER([sqlite3.h],[],[AC_MSG_ERROR([Please install libsqlite3])],[])
		build_linux=yes;;
esac

AM_CONDITIONAL([LINUX],   [test "$build_linux"   = "yes"])
AM_CONDITIONAL([WINDOWS], [test "$build_windows" = "yes"])
AM_CONDITIONAL([MACOS],   [test "$build_mac"     = "yes"])

case "${host_cpu}" in
    i?86*)
        build_i386=yes;;
    x86_64*)
        build_x86_64=yes;;
    arm64*|aarch64*)
        build_arm64=yes;;
    armv7*)
        build_arm=yes;;
    *)
		;;
esac
AM_CONDITIONAL([WIN32], [test "$build_windows" = "yes" -a "$build_i386" == "yes"])
AM_CONDITIONAL([WIN64], [test "$build_windows" = "yes" -a "$build_x86_64" == "yes"])

AC_CONFIG_FILES([
Makefile
cYandexDisk/Makefile
])
AC_OUTPUT

