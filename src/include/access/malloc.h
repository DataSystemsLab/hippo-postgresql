#ifndef BITSET_MALLOC_H_
#define BITSET_MALLOC_H_

#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif

#if defined(has_tcmalloc)
#  include <google/tcmalloc.h>
#  define BITSET_MALLOC_PREFIX tc_
#elif defined(has_jemalloc)
#  include <jemalloc/jemalloc.h>
#  define BITSET_MALLOC_PREFIX prefix_jemalloc
#else
#  include <stdlib.h>
#  define BITSET_MALLOC_PREFIX
#  if defined(LINUX)
#    include <malloc.h> //for mallopt()
#  endif
#endif

#define bitset_malloc(size) \
    BITSET_MALLOC_CALL(malloc)(size)

#define bitset_malloc_free(ptr) \
    BITSET_MALLOC_CALL(free)(ptr)

#define bitset_calloc(count, size) \
    BITSET_MALLOC_CALL(calloc)(count, size)

#define bitset_realloc(ptr, size) \
    BITSET_MALLOC_CALL(realloc)(ptr, size)

#if !defined(has_jemalloc) && !defined(has_tcmalloc) && defined(LINUX)
#  define bitset_mallopt(param, val) return mallopt(param, value);
#else
#  define bitset_mallopt(param, val) 1
#endif

#define BITSET_MALLOC_CALL(fn) \
    BITSET_MALLOC_CONCAT(BITSET_MALLOC_PREFIX, fn)

#define BITSET_MALLOC_CONCAT(a,b) \
    BITSET_MALLOC_CONCAT_(a,b)

#define BITSET_MALLOC_CONCAT_(a,b) \
    a##b

#endif

