#ifndef BITSET_PROBABILISTIC_H
#define BITSET_PROBABILISTIC_H

#include "bitset/bitset.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Bitset linear counting type.
 */

typedef struct bitset_linear_s {
    bitset_word *words;
    unsigned count;
    size_t size;
} bitset_linear_t;

/**
 * Bitset count N type.
 */

typedef struct bitset_countn_s {
    bitset_word **words;
    unsigned n;
    size_t size;
} bitset_countn_t;

/**
 * Estimate unique bits using an uncompressed bitset of the specified size
 * (bloom filter where n=1).
 */

bitset_linear_t *bitset_linear_new(size_t);

/**
 * Estimate unique bits in the bitset.
 */

void bitset_linear_add(bitset_linear_t *, const bitset_t *);

/**
 * Get the estimated unique bit count.
 */

unsigned bitset_linear_count(const bitset_linear_t *);

/**
 * Free the linear counter.
 */

void bitset_linear_free(bitset_linear_t *);

/**
 * Estimate the number of bits that occur N times.
 */

bitset_countn_t *bitset_countn_new(unsigned, size_t);

/**
 * Add a bitset to the counter.
 */

void bitset_countn_add(bitset_countn_t *, const bitset_t *);

/**
 * Count the number of bits that occur N times.
 */

unsigned bitset_countn_count(const bitset_countn_t *);

/**
 * Count the number of bits that occur 0..N times.
 */

unsigned *bitset_countn_count_all(const bitset_countn_t *);

/**
 * Count the number of bits that occur 0..N times using a mask.
 */

unsigned *bitset_countn_count_mask(const bitset_countn_t *, const bitset_t *);

/**
 * Free the result of a count_all or count_mask.
 */

void bitset_countn_count_free(unsigned *);

/**
 * Free the counter.
 */

void bitset_countn_free(bitset_countn_t *);

#ifdef __cplusplus
} //extern "C"
#endif

#endif

