#ifndef BITSET_OPERATION_H
#define BITSET_OPERATION_H

#include "bitset/bitset.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Bitset hash types.
 */

typedef struct bucket_ {
    bitset_offset offset;
    bitset_word word;
    struct bucket_ *next;
} bitset_hash_bucket_t;

typedef struct hash_ {
    bitset_hash_bucket_t **buckets;
    bitset_word *buffer;
    size_t size;
    unsigned count;
} bitset_hash_t;

/**
 * Bitset operation types.
 */

enum bitset_operation_type {
    BITSET_AND,
    BITSET_OR,
    BITSET_XOR,
    BITSET_ANDNOT
};

typedef struct bitset_operation_s bitset_operation_t;

typedef struct bitset_operation_step_s {
    union {
        bitset_t bitset;
        bitset_operation_t *nested;
    } data;
    bool is_nested;
    bool is_operation;
    enum bitset_operation_type type;
} bitset_operation_step_t;

struct bitset_operation_s {
    bitset_operation_step_t **steps;
    size_t length;
};

/**
 * Create a new bitset operation.
 */

bitset_operation_t *bitset_operation_new(bitset_t *b);

/**
 * Free the bitset operation.
 */

void bitset_operation_free(bitset_operation_t *);

/**
 * Add a bitset to the operation.
 */

void bitset_operation_add(bitset_operation_t *, bitset_t *, enum bitset_operation_type);

/**
 * Add a bitset buffer to the operation.
 */

void bitset_operation_add_buffer(bitset_operation_t *, bitset_word *, size_t length, enum bitset_operation_type);

/**
 * Add a nested operation.
 */

void bitset_operation_add_nested(bitset_operation_t *, bitset_operation_t *, enum bitset_operation_type);

/**
 * Execute the operation and return the result.
 */

bitset_t *bitset_operation_exec(bitset_operation_t *);

/**
 * Get the population count of the operation result without using
 * a temporary bitset.
 */

bitset_offset bitset_operation_count(bitset_operation_t *);

#ifdef __cplusplus
} //extern "C"
#endif

#endif

