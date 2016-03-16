#ifndef BITSET_VECTOR_H_
#define BITSET_VECTOR_H_

#include "bitset/operation.h"
#include "bitset/estimate.h"

/**
 * Bitset buffers can be packed together into a vector which is compressed
 * using length encoding.  Each vector buffer consists of zero or more bitsets
 * prefixed with their offset and length
 *
 *    <offset1><length1><bitset_buffer1>...<offsetN><lengthN><bitset_bufferN>
 *
 * For example, adding a bitset with a length of 12 bytes at position 3
 * followed by a bitset with a length of 4 bytes at position 12 would result in
 *
 *    <offset=3><length=12><bitset1><offset=9><length=4><bitset2>
 *
 * Offsets and lengths are encoded using the following format
 *
 *    |0xxxxxxx|xxxxxxxx| 15 bit length
 *    |1xxxxxxx|xxxxxxxx|xxxxxxxx|xxxxxxxx| 31 bit length
 */

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Bitset vector types.
 */

typedef struct bitset_vector_s {
    char *buffer;
    size_t length;
    size_t size;
    unsigned tail_offset;
} bitset_vector_t;

typedef struct bitset_vector_operation_s bitset_vector_operation_t;

typedef struct bitset_vector_operation_step_s {
    union {
        bitset_vector_t *vector;
        bitset_vector_operation_t *operation;
    } data;
    void *userdata;
    bool is_nested;
    bool is_operation;
    enum bitset_operation_type type;
} bitset_vector_operation_step_t;

struct bitset_vector_operation_s {
    bitset_vector_operation_step_t **steps;
    unsigned min;
    unsigned max;
    size_t length;
};

#define BITSET_VECTOR_START 0
#define BITSET_VECTOR_END 0

/**
 * Create a new bitset vector.
 */

bitset_vector_t *bitset_vector_new(void);

/**
 * Create a new bitset vector based on an existing buffer.
 */

bitset_vector_t *bitset_vector_import(const char *, size_t);

/**
 * Free the specified vector.
 */

void bitset_vector_free(bitset_vector_t *);

/**
 * Copy a vector.
 */

bitset_vector_t *bitset_vector_copy(const bitset_vector_t *);

/**
 * Get the vector buffer.
 */

char *bitset_vector_export(const bitset_vector_t *);

/**
 * Get the byte length of the vector buffer.
 */

size_t bitset_vector_length(const bitset_vector_t *);

/**
 * Get the number of bitsets in the vector.
 */

unsigned bitset_vector_bitsets(const bitset_vector_t *);

/**
 * Push a bitset on to the end of the vector.
 */

void bitset_vector_push(bitset_vector_t *, const bitset_t *, unsigned);

/**
 * Resize the vector buffer.
 */

void bitset_vector_resize(bitset_vector_t *, size_t);

/**
 * Iterate over all bitsets.
 */

#define BITSET_VECTOR_FOREACH(vector, bitset, offset) \
    bitset_t BITSET_TMPVAR(tmp, __LINE__); \
    bitset = &BITSET_TMPVAR(tmp, __LINE__); \
    offset = 0; \
    char *BITSET_TMPVAR(buffer, __LINE__) = vector->buffer; \
    while (BITSET_TMPVAR(buffer, __LINE__) < (vector->buffer + vector->length) \
        ? (BITSET_TMPVAR(buffer, __LINE__) = bitset_vector_advance(BITSET_TMPVAR(buffer, __LINE__), \
            bitset, &offset), 1) : 0)

char *bitset_vector_advance(char *buffer, bitset_t *, unsigned *);

/**
 * Concatenate an vector to another at the specified offset. The vector can optionally be
 * sliced by start and end before being concatted. Pass BITSET_VECTOR_START and
 * BITSET_VECTOR_END to both parameters to concat the entire vector.
 */

void bitset_vector_concat(bitset_vector_t *, const bitset_vector_t *, unsigned offset,
    unsigned start, unsigned end);

/**
 * Get a raw and unique count for set items in the vector.
 */

void bitset_vector_cardinality(const bitset_vector_t *, unsigned *, unsigned *);

/**
 * Merge (bitwise OR) each vector bitset.
 */

bitset_t *bitset_vector_merge(const bitset_vector_t *);

/**
 * Create a new vector operation.
 */

bitset_vector_operation_t *bitset_vector_operation_new(bitset_vector_t *);

/**
 * Free the specified vector operation. By default vector operands will not be
 * freed. Use the second function before calling free() to free vectors.
 */

void bitset_vector_operation_free(bitset_vector_operation_t *);
void bitset_vector_operation_free_operands(bitset_vector_operation_t *);

/**
 * Add a vector to the operation.
 */

void bitset_vector_operation_add(bitset_vector_operation_t *,
    bitset_vector_t *, enum bitset_operation_type);

/**
 * Add a nested operation.
 */

void bitset_vector_operation_add_nested(bitset_vector_operation_t *,
    bitset_vector_operation_t *, enum bitset_operation_type);

/**
 * Execute the operation and return the result.
 */

bitset_vector_t *bitset_vector_operation_exec(bitset_vector_operation_t *);

/**
 * Provide a way to associate user data with each step and use the data to lazily
 * lookup vectors.
 */

void bitset_vector_operation_add_data(bitset_vector_operation_t *,
    void *data, enum bitset_operation_type);

void bitset_vector_operation_resolve_data(bitset_vector_operation_t *,
        bitset_vector_t *(*)(void *data, void *context), void *context);

void bitset_vector_operation_free_data(bitset_vector_operation_t *, void (*)(void *data));

/**
 * Misc functions.
 */

void bitset_vector_init(bitset_vector_t *);

#ifdef __cplusplus
} //extern "C"
#endif

#endif

