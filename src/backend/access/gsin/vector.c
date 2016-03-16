#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <limits.h>
#include <assert.h>

#include "bitset/malloc.h"
#include "bitset/vector.h"

bitset_vector_t *bitset_vector_new() {
    bitset_vector_t *vector = bitset_malloc(sizeof(bitset_vector_t));
    if (!vector) {
        bitset_oom();
    }
    vector->buffer = bitset_malloc(sizeof(char));
    if (!vector->buffer) {
        bitset_oom();
    }
    vector->tail_offset = 0;
    vector->size = 1;
    vector->length = 0;
    return vector;
}

void bitset_vector_free(bitset_vector_t *vector) {
    bitset_malloc_free(vector->buffer);
    bitset_malloc_free(vector);
}

bitset_vector_t *bitset_vector_copy(const bitset_vector_t *vector) {
    bitset_vector_t *copy = bitset_vector_new();
    if (vector->length) {
        copy->buffer = bitset_realloc(copy->buffer, sizeof(char) * vector->length);
        if (!copy->buffer) {
            bitset_oom();
        }
        memcpy(copy->buffer, vector->buffer, vector->length);
        copy->length = copy->size = vector->length;
        copy->tail_offset = vector->tail_offset;
    }
    return copy;
}

void bitset_vector_resize(bitset_vector_t *vector, size_t length) {
    size_t new_size = vector->size;
    while (new_size < length) {
        new_size *= 2;
    }
    if (new_size > vector->size) {
        vector->buffer = bitset_realloc(vector->buffer, new_size * sizeof(char));
        if (!vector->buffer) {
            bitset_oom();
        }
        vector->size = new_size;
    }
    vector->length = length;
}

char *bitset_vector_export(const bitset_vector_t *vector) {
    return vector->buffer;
}

size_t bitset_vector_length(const bitset_vector_t *vector) {
    return vector->length;
}

void bitset_vector_init(bitset_vector_t *vector) {
    char *buffer = vector->buffer;
    bitset_t bitset;
    vector->tail_offset = 0;
    while (buffer < vector->buffer + vector->length) {
        buffer = bitset_vector_advance(buffer, &bitset, &vector->tail_offset);
    }
}

bitset_vector_t *bitset_vector_import(const char *buffer, size_t length) {
    bitset_vector_t *vector = bitset_vector_new();
    if (length) {
        bitset_vector_resize(vector, length);
        if (buffer) {
            memcpy(vector->buffer, buffer, length);
            bitset_vector_init(vector);
        }
    }
    return vector;
}

static inline size_t bitset_encoded_length_required_bytes(size_t length) {
    return (length >= (1 << 15)) * 2 + 2;
}

static inline void bitset_encoded_length_bytes(char *buffer, size_t length) {
    if (length < (1 << 15)) {
        buffer[0] = (unsigned char)(length >> 8);
        buffer[1] = (unsigned char)length;
    } else {
        buffer[0] = 0x80 | (unsigned char)(length >> 24);
        buffer[1] = (unsigned char)(length >> 16);
        buffer[2] = (unsigned char)(length >> 8);
        buffer[3] = (unsigned char)length;
    }
}

static inline size_t bitset_encoded_length_size(const char *buffer) {
    return ((buffer[0] & 0x80) != 0) * 2 + 2;
}

static inline size_t bitset_encoded_length(const char *buffer) {
    size_t length;
    if (buffer[0] & 0x80) {
        length = (((unsigned char)buffer[0] & 0x7F) << 24);
        length += ((unsigned char)buffer[1] << 16);
        length += ((unsigned char)buffer[2] << 8);
        length += (unsigned char)buffer[3];
    } else {
        length = (((unsigned char)buffer[0] & 0x7F) << 8);
        length += (unsigned char)buffer[1];
    }
    return length;
}

char *bitset_vector_advance(char *buffer, bitset_t *bitset, unsigned *offset) {
    *offset += bitset_encoded_length(buffer);
    buffer += bitset_encoded_length_size(buffer);
    bitset->length = bitset_encoded_length(buffer);
    buffer += bitset_encoded_length_size(buffer);
    bitset->buffer = (bitset_word *) buffer;
    return buffer + bitset->length * sizeof(bitset_word);
}

static inline char *bitset_vector_encode(bitset_vector_t *vector, const bitset_t *bitset, unsigned offset) {
    size_t length_bytes = bitset_encoded_length_required_bytes(bitset->length);
    size_t offset_bytes = bitset_encoded_length_required_bytes(offset);
    size_t current_length = vector->length;
    bitset_vector_resize(vector, vector->length + length_bytes + offset_bytes + bitset->length * sizeof(bitset_word));
    char *buffer = vector->buffer + current_length;
    bitset_encoded_length_bytes(buffer, offset);
    buffer += offset_bytes;
    bitset_encoded_length_bytes(buffer, bitset->length);
    buffer += length_bytes;
    if (bitset->length) {
        memcpy(buffer, bitset->buffer, bitset->length * sizeof(bitset_word));
    }
    return buffer + bitset->length * sizeof(bitset_word);
}

void bitset_vector_push(bitset_vector_t *vector, const bitset_t *bitset, unsigned offset) {
    if (vector->length && vector->tail_offset >= offset) {
        BITSET_FATAL("bitset vectors are append-only");
    }
    bitset_vector_encode(vector, bitset, offset - vector->tail_offset);
    vector->tail_offset = offset;
}

void bitset_vector_concat(bitset_vector_t *vector, const bitset_vector_t *next, unsigned offset, unsigned start, unsigned end) {
    if (vector->length && vector->tail_offset >= offset) {
        BITSET_FATAL("bitset vectors are append-only");
    }

    unsigned current_offset = 0;
    bitset_t bitset;

    char *buffer, *c_buffer = next->buffer, *c_start, *c_end = next->buffer + next->length;
    while (c_buffer < c_end) {
        c_buffer = bitset_vector_advance(c_buffer, &bitset, &current_offset);
        if (current_offset >= start && (!end || current_offset < end)) {
            c_start = c_buffer;

            //Copy the initial bitset from c
            buffer = bitset_vector_encode(vector, &bitset, offset + current_offset - vector->tail_offset);

            //Look for a slice end point
            if (end != BITSET_VECTOR_END && c_end > c_start) {
                do {
                    c_end = c_buffer;
                    if (c_end == next->buffer + next->length) {
                        break;
                    }
                    c_buffer = bitset_vector_advance(c_buffer, &bitset, &current_offset);
                    vector->tail_offset = current_offset + offset;
                } while (current_offset < end);
            } else {
                vector->tail_offset = next->tail_offset + offset;
            }

            //Concat the rest of the vector
            if (c_end > c_start) {
                uintptr_t buf_offset = buffer - vector->buffer;
                bitset_vector_resize(vector, vector->length + (c_end - c_start));
                memcpy(vector->buffer + buf_offset, c_start, c_end - c_start);
            }

            break;
        }
    }
}

unsigned bitset_vector_bitsets(const bitset_vector_t *vector) {
    unsigned count = 0, offset;
    bitset_t *bitset;
    BITSET_VECTOR_FOREACH(vector, bitset, offset) {
        count++;
    }
    return count;
}

void bitset_vector_cardinality(const bitset_vector_t *vector, unsigned *raw, unsigned *unique) {
    unsigned offset;
    bitset_t *bitset;
    *raw = 0;
    BITSET_VECTOR_FOREACH(vector, bitset, offset) {
        *raw += bitset_count(bitset);
    }
    if (unique) {
        if (*raw) {
            bitset_linear_t *counter = bitset_linear_new(*raw * 100);
            BITSET_VECTOR_FOREACH(vector, bitset, offset) {
                bitset_linear_add(counter, bitset);
            }
            *unique = bitset_linear_count(counter);
            bitset_linear_free(counter);
        } else {
            *unique = 0;
        }
    }
}

bitset_t *bitset_vector_merge(const bitset_vector_t *vector) {
    unsigned offset;
    bitset_t *bitset;
    bitset_operation_t *operation = bitset_operation_new(NULL);
    BITSET_VECTOR_FOREACH(vector, bitset, offset) {
        bitset_operation_add(operation, bitset, BITSET_OR);
    }
    bitset = bitset_operation_exec(operation);
    bitset_operation_free(operation);
    return bitset;
}

static inline void bitset_vector_start_end(bitset_vector_t *vector, unsigned *start, unsigned *end) {
    if (!vector->length) {
        *start = 0;
        *end = 0;
        return;
    }
    bitset_t bitset;
    bitset_vector_advance(vector->buffer, &bitset, start);
    *end = vector->tail_offset;
}

bitset_vector_operation_t *bitset_vector_operation_new(bitset_vector_t *vector) {
    bitset_vector_operation_t *operation = bitset_malloc(sizeof(bitset_vector_operation_t));
    if (!operation) {
        bitset_oom();
    }
    operation->length = operation->max = 0;
    operation->min = UINT_MAX;
    if (vector) {
        bitset_vector_operation_add(operation, vector, BITSET_OR);
    }
    return operation;
}

void bitset_vector_operation_free(bitset_vector_operation_t *operation) {
    if (operation->length) {
    	size_t i;
        for ( i = 0; i < operation->length; i++) {
            if (operation->steps[i]->is_nested) {
                if (operation->steps[i]->is_operation) {
                    bitset_vector_operation_free(operation->steps[i]->data.operation);
                } else {
                    bitset_vector_free(operation->steps[i]->data.vector);
                }
            }
            bitset_malloc_free(operation->steps[i]);
        }
        bitset_malloc_free(operation->steps);
    }
    bitset_malloc_free(operation);
}

void bitset_vector_operation_free_operands(bitset_vector_operation_t *operation) {
    if (operation->length) {
    	size_t i;
        for ( i = 0; i < operation->length; i++) {
            if (operation->steps[i]->is_nested) {
                if (operation->steps[i]->is_operation) {
                    bitset_vector_operation_free_operands(operation->steps[i]->data.operation);
                }
            } else {
                if (operation->steps[i]->data.vector) {
                    bitset_vector_free(operation->steps[i]->data.vector);
                }
                operation->steps[i]->data.vector = NULL;
            }
        }
    }
}

static inline bitset_vector_operation_step_t *
        bitset_vector_operation_add_step(bitset_vector_operation_t *operation) {
    bitset_vector_operation_step_t *step = bitset_malloc(sizeof(bitset_vector_operation_step_t));
    if (!step) {
        bitset_oom();
    }
    if (operation->length % 2 == 0) {
        if (!operation->length) {
            operation->steps = bitset_malloc(sizeof(bitset_vector_operation_step_t *) * 2);
        } else {
            operation->steps = bitset_realloc(operation->steps, sizeof(bitset_vector_operation_step_t *) * operation->length * 2);
        }
        if (!operation->steps) {
            bitset_oom();
        }
    }
    operation->steps[operation->length++] = step;
    return step;
}

void bitset_vector_operation_add(bitset_vector_operation_t *operation,
        bitset_vector_t *vector, enum bitset_operation_type type) {
    if (!vector->length) {
        return;
    }
    bitset_vector_operation_step_t *step = bitset_vector_operation_add_step(operation);
    step->is_nested = false;
    step->is_operation = false;
    step->data.vector = vector;
    step->type = type;
    unsigned start = 0, end = 0;
    bitset_vector_start_end(vector, &start, &end);
    operation->min = BITSET_MIN(operation->min, start);
    operation->max = BITSET_MAX(operation->max, end);
}

void bitset_vector_operation_add_nested(bitset_vector_operation_t *operation,
        bitset_vector_operation_t *nested, enum bitset_operation_type type) {
    bitset_vector_operation_step_t *step = bitset_vector_operation_add_step(operation);
    step->is_nested = true;
    step->is_operation = true;
    step->data.operation = nested;
    step->type = type;
    operation->min = BITSET_MIN(operation->min, nested->min);
    operation->max = BITSET_MAX(operation->max, nested->max);
}

void bitset_vector_operation_add_data(bitset_vector_operation_t *operation,
        void *data, enum bitset_operation_type type) {
    bitset_vector_operation_step_t *step = bitset_vector_operation_add_step(operation);
    step->is_nested = false;
    step->is_operation = false;
    step->data.vector = NULL;
    step->type = type;
    step->userdata = data;
}

void bitset_vector_operation_resolve_data(bitset_vector_operation_t *operation,
        bitset_vector_t *(*resolve_fn)(void *, void *), void *context) {
    unsigned start, end;
    if (operation->length) {
    	size_t j;
        for ( j = 0; j < operation->length; j++) {
            start = 0;
            end = 0;
            if (operation->steps[j]->is_operation) {
                bitset_vector_operation_resolve_data(operation->steps[j]->data.operation, resolve_fn, context);
            } else if (operation->steps[j]->userdata) {
                bitset_vector_t *vector = resolve_fn(operation->steps[j]->userdata, context);
                operation->steps[j]->data.vector = vector;
                if (vector && vector->length) {
                    bitset_vector_start_end(vector, &start, &end);
                    operation->min = BITSET_MIN(operation->min, start);
                    operation->max = BITSET_MAX(operation->max, end);
                }
            }
        }
    }
}

void bitset_vector_operation_free_data(bitset_vector_operation_t *operation, void (*free_fn)(void *)) {
    if (operation->length) {
    	size_t j;
        for ( j = 0; j < operation->length; j++) {
            if (operation->steps[j]->is_operation) {
                bitset_vector_operation_free_data(operation->steps[j]->data.operation, free_fn);
            } else if (operation->steps[j]->userdata) {
                free_fn(operation->steps[j]->userdata);
            }
        }
    }
}

bitset_vector_t *bitset_vector_operation_exec(bitset_vector_operation_t *operation) {
    if (!operation->length) {
        return bitset_vector_new();
    } else if (operation->length == 1 && !operation->steps[0]->is_operation) {
        return bitset_vector_copy(operation->steps[0]->data.vector);
    }

    bitset_vector_t *vector, *result;
    bitset_t *bitset_ptr, bitset;
    bitset_operation_t *nested;
    unsigned offset;
    char *buffer, *next, *bitset_buffer;
    size_t bitset_length, buckets, key, copy_length, offset_bytes;
    void **bucket, **and_bucket;
    enum bitset_operation_type type;

    //Recursively flatten nested operations
    size_t i,j;
    for ( i = 0; i < operation->length; i++) {
        if (operation->steps[i]->is_operation) {
            vector = bitset_vector_operation_exec(operation->steps[i]->data.operation);
            bitset_vector_operation_free(operation->steps[i]->data.operation);
            operation->steps[i]->data.vector = vector;
            operation->steps[i]->is_operation = false;
        }
    }

    //Prepare the result vector
    result = bitset_vector_new();

    //Prepare hash buckets
    buckets = operation->max - operation->min + 1;
    bucket = bitset_calloc(1, sizeof(void*) * buckets);
    if (!bucket) {
        bitset_oom();
    }

    //OR the first vector
    vector = operation->steps[0]->data.vector;

    buffer = vector->buffer;
    offset = 0;
    while (buffer < vector->buffer + vector->length) {
        next = bitset_vector_advance(buffer, &bitset, &offset);
        assert(offset >= operation->min && offset <= operation->max);
        bucket[offset - operation->min] = buffer + bitset_encoded_length_size(buffer);
        buffer = next;
    }
    for ( i = 1; i < operation->length; i++) {

        type = operation->steps[i]->type;
        vector = operation->steps[i]->data.vector;

        if (type == BITSET_AND) {

            and_bucket = bitset_calloc(1, sizeof(void*) * buckets);
            if (!and_bucket) {
                bitset_oom();
            }
            if (vector) {
                buffer = vector->buffer;
                offset = 0;
                while (buffer < vector->buffer + vector->length) {
                    next = bitset_vector_advance(buffer, &bitset, &offset);
                    assert(offset >= operation->min && offset <= operation->max);
                    key = offset - operation->min;
                    if (bucket[key]) {
                        if (BITSET_IS_TAGGED_POINTER(bucket[key])) {
                            nested = (bitset_operation_t *) BITSET_UNTAG_POINTER(bucket[key]);
                        } else {
                            bitset_buffer = bucket[key];
                            bitset_length = bitset_encoded_length(bitset_buffer);
                            bitset_buffer += bitset_encoded_length_size(bitset_buffer);
                            nested = bitset_operation_new(NULL);
                            bitset_operation_add_buffer(nested, (bitset_word*)bitset_buffer, bitset_length, BITSET_OR);
                        }
                        bitset_operation_add_buffer(nested, bitset.buffer, bitset.length, BITSET_AND);
                        and_bucket[key] = (void *) BITSET_TAG_POINTER(nested);
                    }
                    buffer = next;
                }
            }
            for (j = 0; j < buckets; j++) {
                if (and_bucket[j] && BITSET_IS_TAGGED_POINTER(bucket[j])) {
                    nested = (bitset_operation_t *) BITSET_UNTAG_POINTER(bucket[j]);
                    bitset_operation_free(nested);
                }
            }
            bitset_malloc_free(bucket);
            bucket = and_bucket;

        } else {

            buffer = vector->buffer;
            offset = 0;
            while (buffer < vector->buffer + vector->length) {
                next = bitset_vector_advance(buffer, &bitset, &offset);
                assert(offset >= operation->min && offset <= operation->max);
                key = offset - operation->min;
                if (BITSET_IS_TAGGED_POINTER(bucket[key])) {
                    nested = (bitset_operation_t *) BITSET_UNTAG_POINTER(bucket[key]);
                    bitset_operation_add_buffer(nested, bitset.buffer, bitset.length, type);
                } else if (bucket[key]) {
                    bitset_buffer = bucket[key];
                    bitset_length = bitset_encoded_length(bitset_buffer);
                    bitset_buffer += bitset_encoded_length_size(bitset_buffer);
                    nested = bitset_operation_new(NULL);
                    bitset_operation_add_buffer(nested, (bitset_word*)bitset_buffer, bitset_length, BITSET_OR);
                    bucket[key] = (void *) BITSET_TAG_POINTER(nested);
                    bitset_operation_add_buffer(nested, bitset.buffer, bitset.length, type);
                } else {
                    bucket[key] = buffer + bitset_encoded_length_size(buffer);
                }
                buffer = next;
            }
        }
    }

    //Prepare the result vector
    offset = 0;
    buffer = result->buffer;
    for (i = 0; i < buckets; i++) {
        if (BITSET_IS_TAGGED_POINTER(bucket[i])) {
            nested = (bitset_operation_t *) BITSET_UNTAG_POINTER(bucket[i]);
            bitset_ptr = bitset_operation_exec(nested);
            if (bitset_ptr->length) {
                buffer = bitset_vector_encode(result, bitset_ptr, operation->min + i - offset);
                offset = operation->min + i;
            }
            bitset_free(bitset_ptr);
            bitset_operation_free(nested);
        } else if (bucket[i]) {
            offset_bytes = bitset_encoded_length_required_bytes(operation->min + i - offset);
            bitset_length = bitset_encoded_length(bucket[i]);
            copy_length = bitset_encoded_length_required_bytes(bitset_length) + bitset_length * sizeof(bitset_word);
            uintptr_t buf_offset = buffer - result->buffer;
            bitset_vector_resize(result, result->length + offset_bytes + copy_length);
            buffer = result->buffer + buf_offset;
            bitset_encoded_length_bytes(buffer, operation->min + i - offset);
            buffer += offset_bytes;
            memcpy(buffer, bucket[i], copy_length);
            buffer += copy_length;
            offset = operation->min + i;
        }
    }

    bitset_malloc_free(bucket);

    return result;
}

