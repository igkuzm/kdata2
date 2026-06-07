/**
 * File              : log.h
 * Author            : Igor V. Sementsov <ig.kuzm@gmail.com>
 * Date              : 19.03.2023
 * Last Modified Date: 25.01.2026
 * Last Modified By  : Igor V. Sementsov <ig.kuzm@gmail.com>
 */

/**
 * log.h
 * Copyright (c) 2023 Igor V. Sementsov <ig.kuzm@gmail.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

/*
 * Debugging in Android/iOS and other platforms
 */

#ifndef k_lib_log_h
#define k_lib_log_h

#ifdef __cplusplus
extern "C" {
#endif

#include <stdio.h>
#include <string.h>
#include <stdarg.h>

static char __buf[BUFSIZ];

static char *STR(const char *fmt, ...) {
	va_list args;
	va_start(args, fmt);
	vsnprintf(__buf, BUFSIZ-1,fmt, args);
	va_end(args);
	return __buf;
}

#ifdef __ANDROID__
#include <android/log.h>
#define ERR(fmt, ...) __android_log_print(ANDROID_LOG_ERROR, __FILE__, ": %d: %s" __LINE__, STR(fmt, __VA_ARGS__)) 
#elif defined _MSC_VER
static void ERR(const char *fmt, ...) {
	char str[BUFSIZ];
	va_list args;
	va_start(args, fmt);
	vsnprintf(__buf, BUFSIZ-1,fmt, args);
	va_end(args);
	snprintf(str, BUFSIZ-1,"E/: %s", __buf);
	perror(str);
}
#else
#define ERR(fmt, ...) fprintf(stderr, "E/: %s: %d: %s\n", __FILE__, __LINE__, STR(fmt, __VA_ARGS__));
#endif
	
#ifndef DEBUG
static void LOG(const char *fmt, ...){} // no log
#else
#ifdef __ANDROID__
#include <android/log.h>
#define LOG(fmt, ...) __android_log_print(ANDROID_LOG_INFO,  __FILE__, ": %d: %s", __LINE__, STR(fmt, __VA_ARGS__))
#elif defined _MSC_VER
static void LOG(const char *fmt, ...) 
{
	va_list args;
	va_start(args, fmt);
	vsnprintf(__buf, BUFSIZ-1,fmt, args);
	va_end(args);
	fprintf(stderr, "I/: %s\n", __buf);
}
#else
#define LOG(fmt, ...) fprintf(stderr, "I/: %s: %d: %s\n", __FILE__, __LINE__, STR(fmt, __VA_ARGS__));
#endif
#endif // DEBUG

#ifdef __cplusplus
}
#endif

#endif /* ifndef k_lib_log_h */
