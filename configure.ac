dnl Process this file with autoconf to produce a configure script.

AC_INIT
AC_CONFIG_SRCDIR([configure.ac])
AM_INIT_AUTOMAKE(libkdata2, 0.1)
AM_MAINTAINER_MODE

AC_PROG_RANLIB
AC_PROG_CC

AC_CANONICAL_HOST
case "${host_os}" in
    cygwin*|mingw*)
        build_windows=yes;;
    darwin*)
        build_mac=yes;;
    *)
				build_linux=yes;;
esac
AM_CONDITIONAL([LINUX],   [test "$build_linux"   = "yes"])
AM_CONDITIONAL([WINDOWS], [test "$build_windows" = "yes"])
AM_CONDITIONAL([MACOS],   [test "$build_mac"     = "yes"])

case "${host_os}" in
	cygwin*|mingw*)
	;;
	darwin*)
	;;
	linux*)
		AC_SUBST(WEBKIT_CFLAGS)
		AC_SUBST(WEBKIT_LIBS)
	;;
esac


AC_CONFIG_FILES([
Makefile
])
AC_OUTPUT

