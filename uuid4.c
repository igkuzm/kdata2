/**
 * File              : uuid4.c
 * Author            : Igor V. Sementsov <ig.kuzm@gmail.com>
 * Date              : 22.09.2025
 * Last Modified Date: 22.09.2025
 * Last Modified By  : Igor V. Sementsov <ig.kuzm@gmail.com>
 */

#include <stdio.h>

#if defined(_WIN32)
#include <windows.h>
#include <wincrypt.h>
#endif

#include "uuid4.h"


static int seed[4];

static int xorshift128plus(int *s) {
  /* http://xorshift.di.unimi.it/xorshift128plus.c */
  int s1 = s[0];
  const int s0 = s[1];
  s[0] = s0;
  s1 ^= s1 << 23;
  s[1] = s1 ^ s0 ^ (s1 >> 18) ^ (s0 >> 5);
  return s[1] + s0;
}


int uuid4_init(void) {
#if defined(__linux__) || defined(__APPLE__) || defined(__FreeBSD__)
  int res;
  FILE *fp = fopen("/dev/urandom", "rb");
  if (!fp) {
    return UUID4_EFAILURE;
  }
  res = fread(seed, 1, sizeof(seed), fp);
  fclose(fp);
  if ( res != sizeof(seed) ) {
    return UUID4_EFAILURE;
  }

#elif defined(_WIN32)
  //int res;
  //HCRYPTPROV hCryptProv;
  //res = CryptAcquireContext(
    //&hCryptProv, NULL, NULL, PROV_RSA_FULL, CRYPT_VERIFYCONTEXT);
  //if (!res) {
    //return UUID4_EFAILURE;
  //}
  //res = CryptGenRandom(hCryptProv, (DWORD) sizeof(seed), (PBYTE) seed);
  //CryptReleaseContext(hCryptProv, 0);
  //if (!res) {
    //return UUID4_EFAILURE;
  //}
	seed[0] = rand();
	seed[1] = rand();
	seed[2] = rand();
	seed[3] = rand();

#else
  #error "unsupported platform"
#endif
  return UUID4_ESUCCESS;
}


void uuid4_generate(char *dst) {
  static const char *template = "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx";
  static const char *chars = "0123456789abcdef";
  union { unsigned char b[16]; int word[4]; } s;
  const char *p;
  int i, n;
  /* get random */
  s.word[0] = xorshift128plus(seed);
  s.word[1] = xorshift128plus(seed);
  s.word[2] = xorshift128plus(seed);
  s.word[3] = xorshift128plus(seed);
  /* build string */
  p = template;
  i = 0;
  while (*p) {
    n = s.b[i >> 1];
    n = (i & 1) ? (n >> 4) : (n & 0xf);
    switch (*p) {
      case 'x'  : *dst = chars[n];              i++;  break;
      case 'y'  : *dst = chars[(n & 0x3) + 8];  i++;  break;
      default   : *dst = *p;
    }
    dst++, p++;
  }
  *dst = '\0';
}
