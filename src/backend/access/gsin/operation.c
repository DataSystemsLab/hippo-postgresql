#include <stdlib.h>
#include <stdio.h>

#include "bitset/malloc.h"
#include "bitset/operation.h"

bitset_operation_t *bitset_operation_new(bitset_t *bitset) {
    bitset_operation_t *operation = bitset_malloc(sizeof(bitset_operation_t));
    if (!operation) {
        bitset_oom();
    }
    operation->length = 0;
    if (bitset) {
        bitset_operation_add(operation, bitset, BITSET_OR);
    }
    return operation;
}

void bitset_operation_free(bitset_operation_t *operation) {
	size_t i;
    for ( i = 0; i < operation->length; i++) {
        if (operation->steps[i]->is_nested) {
            if (operation->steps[i]->is_operation) {
                bitset_operation_free(operation->steps[i]->data.nested);
            } else {
                bitset_malloc_free(operation->steps[i]->data.bitset.buffer);
            }
        }
        bitset_malloc_free(operation->steps[i]);
    }
    bitset_malloc_free(operation->steps);
    bitset_malloc_free(operation);
}

static inline bitset_operation_step_t *bitset_operation_add_step(bitset_operation_t *operation) {
    bitset_operation_step_t *step = bitset_malloc(sizeof(bitset_operation_step_t));
    if (!step) {
        bitset_oom();
    }
    if (operation->length % 2 == 0) {
        if (!operation->length) {
            operation->steps = bitset_malloc(sizeof(bitset_operation_step_t *) * 2);
        } else {
            operation->steps = bitset_realloc(operation->steps, sizeof(bitset_operation_step_t *) * operation->length * 2);
        }
        if (!operation->steps) {
            bitset_oom();
        }
    }
    return operation->steps[operation->length++] = step;
}

void bitset_operation_add_buffer(bitset_operation_t *operation,
        bitset_word *buffer, size_t length, enum bitset_operation_type type) {
    if (!length) {
        if (type == BITSET_AND && operation->length) {
        	size_t i;
            for ( i = 0; i < operation->length; i++) {
                bitset_malloc_free(operation->steps[i]);
            }
            operation->length = 0;
        }
        return;
    }
    bitset_operation_step_t *step = bitset_operation_add_step(operation);
    step->is_nested = false;
    step->is_operation = false;
    step->data.bitset.buffer = buffer;
    step->data.bitset.length = length;
    step->type = type;
}

void bitset_operation_add(bitset_operation_t *operation,
        bitset_t *bitset, enum bitset_operation_type type) {
    bitset_operation_add_buffer(operation, bitset->buffer, bitset->length, type);
}

void bitset_operation_add_nested(bitset_operation_t *operation, bitset_operation_t *nested,
        enum bitset_operation_type type) {
    bitset_operation_step_t *step = bitset_operation_add_step(operation);
    step->is_nested = true;
    step->is_operation = true;
    step->data.nested = nested;
    step->type = type;
}

static inline bitset_hash_t *bitset_hash_new(size_t buckets) {
    bitset_hash_t *hash = bitset_malloc(sizeof(bitset_hash_t));
    if (!hash) {
        bitset_oom();
    }
    size_t size;
    BITSET_NEXT_POW2(size, buckets);
    hash->size = size;
    hash->count = 0;
    hash->buckets = bitset_calloc(1, sizeof(bitset_hash_bucket_t *) * hash->size);
    hash->buffer = bitset_malloc(sizeof(bitset_word) * hash->size);
    if (!hash->buckets || !hash->buffer) {
        bitset_oom();
    }
    return hash;
}

static inline void bitset_hash_free(bitset_hash_t *hash) {
    bitset_hash_bucket_t *bucket, *tmp;
    size_t i;
    for ( i = 0; i < hash->size; i++) {
        bucket = hash->buckets[i];
        if (BITSET_IS_TAGGED_POINTER(bucket)) {
            continue;
        }
        while (bucket) {
            tmp = bucket;
            bucket = bucket->next;
            bitset_malloc_free(tmp);
        }
    }
    bitset_malloc_free(hash->buckets);
    bitset_malloc_free(hash->buffer);
    bitset_malloc_free(hash);
}

static inline bool bitset_hash_insert(bitset_hash_t *hash, bitset_offset offset, bitset_word word) {
    unsigned key = offset & (hash->size - 1);
    bitset_hash_bucket_t *insert, *bucket = hash->buckets[key];
    bitset_offset off;
    if (BITSET_IS_TAGGED_POINTER(bucket)) {
        off = BITSET_UINT_FROM_POINTER(bucket);
        if (off == offset) {
            return false;
        }
        insert = bitset_malloc(sizeof(bitset_hash_bucket_t));
        if (!insert) {
            bitset_oom();
        }
        insert->offset = off;
        insert->word = (uintptr_t)hash->buffer[key];
        bucket = hash->buckets[key] = insert;
        insert = bitset_malloc(sizeof(bitset_hash_bucket_t));
        if (!insert) {
            bitset_oom();
        }
        insert->offset = offset;
        insert->word = word;
        insert->next = NULL;
        bucket->next = insert;
        hash->count++;
        return true;
    } else if (!bucket) {
#if SIZEOF_VOID_P != BITSET_WORD_BYTES
        hash->buckets[key] = (bitset_hash_bucket_t *) BITSET_UINT_IN_POINTER(offset);
        hash->buffer[key] = word;
        hash->count++;
#else
        if (BITSET_UINT_CAN_TAG(offset)) {
            hash->buckets[key] = (bitset_hash_bucket_t *) BITSET_UINT_IN_POINTER(offset);
            hash->buffer[key] = word;
            hash->count++;
        } else {
            insert = bitset_malloc(sizeof(bitset_hash_bucket_t));
            if (!insert) {
                bitset_oom();
            }
            insert->offset = offset;
            insert->word = word;
            insert->next = NULL;
            hash->buckets[key] = insert;
        }
#endif
        return true;
    }
    for (;;) {
        if (bucket->offset == offset) {
            return false;
        }
        if (bucket->next == NULL) {
            break;
        }
        bucket = bucket->next;
    }
    insert = bitset_malloc(sizeof(bitset_hash_bucket_t));
    if (!insert) {
        bitset_oom();
    }
    insert->offset = offset;
    insert->word = word;
    insert->next = NULL;
    bucket->next = insert;
    hash->count++;
    return true;
}

static inline bitset_word *bitset_hash_get(const bitset_hash_t *hash, bitset_offset offset) {
    unsigned key = offset & (hash->size - 1);
    bitset_hash_bucket_t *bucket = hash->buckets[key];
    bitset_offset off;
    while (bucket) {
        if (BITSET_IS_TAGGED_POINTER(bucket)) {
            off = BITSET_UINT_FROM_POINTER(bucket);
            if (off != offset) {
                return NULL;
            }
            return &hash->buffer[key];
        }
        if (bucket->offset == offset) {
            return &bucket->word;
        }
        bucket = bucket->next;
    }
    return NULL;
}

static inline bitset_hash_t *bitset_operation_iter(bitset_operation_t *operation) {
    bitset_offset word_offset, max = 0, b_max, length, and_offset;
    bitset_operation_step_t *step;
    bitset_word word = 0, *hashed, and_word;
    unsigned position, count = 0, k, j;
    int last_k, last_j;
    size_t size, start_at,i;
    bitset_hash_t *words, *and_words = NULL;
    bitset_t *tmp, *bitset, *and;

    //Recursively flatten nested operations
    for ( i = 0; i < operation->length; i++) {
        if (operation->steps[i]->is_operation) {
            tmp = bitset_operation_exec(operation->steps[i]->data.nested);
            bitset_operation_free(operation->steps[i]->data.nested);
            operation->steps[i]->data.bitset.buffer = tmp->buffer;
            operation->steps[i]->data.bitset.length = tmp->length;
            operation->steps[i]->is_operation = false;
            bitset_malloc_free(tmp);
        }
        count += operation->steps[i]->data.bitset.length;
        b_max = bitset_max(&operation->steps[i]->data.bitset);
        max = BITSET_MAX(max, b_max);
    }

    //Work out the number of hash buckets to allocate
    if (count <= 8) {
        size = 16;
    } else if (count <= 8388608) {
        size = count * 2;
    } else {
        size = max / BITSET_LITERAL_LENGTH + 2;
        while (count < max / 64) {
            size /= 2;
            max /= 64;
        }
        size = size <= 16 ? 16 : size > 16777216 ? 16777216 : size;
    }
    words = bitset_hash_new(size);
    start_at = 1;
    bitset = &operation->steps[0]->data.bitset;
    word_offset = 0;

    //Compute (0 OR (A AND B)) instead of the usual ((0 OR A) AND B)
    if (operation->length >= 2 && operation->steps[1]->type == BITSET_AND) {
        start_at = 2;
        and_offset = 0;
        and_word = 0;
        and = &operation->steps[1]->data.bitset;
        k = 0;
        j = 0;
        last_k = -1;
        last_j = -1;
        while (1) {
            if (last_j < (int)j && j < and->length) {
                if (BITSET_IS_FILL_WORD(and->buffer[j])) {
                    and_offset += BITSET_GET_LENGTH(and->buffer[j]);
                    position = BITSET_GET_POSITION(and->buffer[j]);
                    if (!position) {
                        j++;
                        continue;
                    }
                    and_word = BITSET_CREATE_LITERAL(position - 1);
                } else {
                    and_word = and->buffer[j];
                }
                and_offset++;
                last_j = j;
            }
            if (last_k < (int)k && k < bitset->length) {
                if (BITSET_IS_FILL_WORD(bitset->buffer[k])) {
                    length = BITSET_GET_LENGTH(bitset->buffer[k]);
                    word_offset += length;
                    position = BITSET_GET_POSITION(bitset->buffer[k]);
                    if (!position) {
                        k++;
                        continue;
                    }
                    word = BITSET_CREATE_LITERAL(position - 1);
                } else {
                    word = bitset->buffer[k];
                }
                word_offset++;
                last_k = k;
            }
            if (and_offset < word_offset) {
                if (j++ >= and->length) {
                    break;
                }
            } else if (word_offset < and_offset) {
                if (k++ >= bitset->length) {
                    break;
                }
            } else {
                word &= and_word;
                if (word) {
                    bitset_hash_insert(words, word_offset, word);
                }
                j++;
                k++;
                if (j >= and->length && k >= bitset->length) {
                    break;
                }
            }
        }
    } else {
        //Populate the offset=>word hash (0 OR A)
        for ( i = 0; i < bitset->length; i++) {
            word = bitset->buffer[i];
            if (BITSET_IS_FILL_WORD(word)) {
                length = BITSET_GET_LENGTH(word);
                word_offset += length;
                position = BITSET_GET_POSITION(word);
                if (!position) {
                    continue;
                }
                word = BITSET_CREATE_LITERAL(position - 1);
            }
            word_offset++;
            bitset_hash_insert(words, word_offset, word);
        }
    }

    //Apply the remaining steps in the operation
    size_t j2;
    for ( i = start_at; i < operation->length; i++) {
        step = operation->steps[i];
        bitset = &step->data.bitset;
        word_offset = 0;
        if (step->type == BITSET_AND) {
            and_words = bitset_hash_new(words->size);
            for ( j2 = 0; j2 < bitset->length; j2++) {
                word = bitset->buffer[j2];
                if (BITSET_IS_FILL_WORD(word)) {
                    length = BITSET_GET_LENGTH(word);
                    word_offset += length;
                    position = BITSET_GET_POSITION(word);
                    if (!position) {
                        continue;
                    }
                    word = BITSET_CREATE_LITERAL(position - 1);
                }
                word_offset++;
                hashed = bitset_hash_get(words, word_offset);
                if (hashed && *hashed) {
                    word &= *hashed;
                    if (word) {
                        bitset_hash_insert(and_words, word_offset, word);
                    }
                }
            }
            bitset_hash_free(words);
            words = and_words;
        } else {

            for ( j2 = 0; j2 < bitset->length; j2++) {
                word = bitset->buffer[j2];
                if (BITSET_IS_FILL_WORD(word)) {
                    length = BITSET_GET_LENGTH(word);
                    word_offset += length;
                    position = BITSET_GET_POSITION(word);
                    if (!position) {
                        continue;
                    }
                    word = BITSET_CREATE_LITERAL(position - 1);
                }
                word_offset++;
                hashed = bitset_hash_get(words, word_offset);
                if (hashed) {
                    switch (step->type) {
                        case BITSET_OR:     *hashed |= word;  break;
                        case BITSET_ANDNOT: *hashed &= ~word; break;
                        case BITSET_XOR:    *hashed ^= word;  break;
                        default: break;
                    }
                } else if (step->type != BITSET_ANDNOT) {
                    bitset_hash_insert(words, word_offset, word);
                }
            }
        }
    }
    return words;
}

static int bitset_operation_quick_sort(const void *a, const void *b) {
    bitset_offset a_offset = *(bitset_offset *)a;
    bitset_offset b_offset = *(bitset_offset *)b;
    return a_offset < b_offset ? -1 : a_offset > b_offset;
}

static inline void bitset_operation_insertion_sort(bitset_offset *offsets, size_t count) {
	size_t i,j;
	for ( i = 0, j; i < count; ++i) {
        for (j = i; j && offsets[j-1] > offsets[j]; j--) {
            bitset_offset tmp = offsets[j];
            offsets[j] = offsets[j-1];
            offsets[j-1] = tmp;
        }
    }
}

static inline unsigned char bitset_fls(bitset_word word) {
    static char table[64] = {
        32, 31, 0, 16, 0, 30, 3, 0, 15, 0, 0, 0, 29, 10, 2, 0,
        0, 0, 12, 14, 21, 0, 19, 0, 0, 28, 0, 25, 0, 9, 1, 0,
        17, 0, 4, 0, 0, 0, 11, 0, 13, 22, 20, 0, 26, 0, 0, 18,
        5, 0, 0, 23, 0, 27, 0, 6, 0, 24, 7, 0, 8, 0, 0, 0
    };
    word = word | (word >> 1);
    word = word | (word >> 2);
    word = word | (word >> 4);
    word = word | (word >> 8);
    word = word | (word >> 16);
    word = (word << 3) - word;
    word = (word << 8) - word;
    word = (word << 8) - word;
    word = (word << 8) - word;
    return table[word >> 26] - 1;
}

bitset_t *bitset_operation_exec(bitset_operation_t *operation) {
    if (!operation->length) {
        return bitset_new();
    } else if (operation->length == 1 && !operation->steps[0]->is_operation) {
        return bitset_copy(&operation->steps[0]->data.bitset);
    }
    bitset_hash_t *words = bitset_operation_iter(operation);
    bitset_hash_bucket_t *bucket;
    bitset_t *result = bitset_new();
    bitset_offset word_offset = 0, fills, offset;
    bitset_word word, *hashed, fill = BITSET_CREATE_EMPTY_FILL(BITSET_MAX_LENGTH);
    if (!words->count) {
        bitset_hash_free(words);
        return result;
    }
    bitset_offset *offsets = bitset_malloc(sizeof(bitset_offset) * words->count);
    if (!offsets) {
        bitset_oom();
    }
    size_t i,j,pos;
    for ( i = 0, j = 0; i < words->size; i++) {
        bucket = words->buckets[i];
        if (BITSET_IS_TAGGED_POINTER(bucket)) {
            offsets[j++] = BITSET_UINT_FROM_POINTER(bucket);
            continue;
        }
        while (bucket) {
            offsets[j++] = bucket->offset;
            bucket = bucket->next;
        }
    }
    if (words->count < 64) {
        bitset_operation_insertion_sort(offsets, words->count);
    } else {
        qsort(offsets, words->count, sizeof(bitset_offset), bitset_operation_quick_sort);
    }
    for ( i = 0, pos = 0; i < words->count; i++) {
        offset = offsets[i];
        hashed = bitset_hash_get(words, offset);
        word = *hashed;
        if (!word) continue;
        if (offset - word_offset == 1) {
            bitset_resize(result, result->length + 1);
            result->buffer[pos++] = word;
        } else {
            if (offset - word_offset > BITSET_MAX_LENGTH) {
                fills = (offset - word_offset) / BITSET_MAX_LENGTH;
                bitset_resize(result, result->length + fills);
                bitset_offset i2;
                for ( i2 = 0; i2 < fills; i2++) {
                    result->buffer[pos++] = fill;
                }
                word_offset += fills * BITSET_MAX_LENGTH;
            }
            if (BITSET_IS_POW2(word)) {
                bitset_resize(result, result->length + 1);
                result->buffer[pos++] = BITSET_CREATE_FILL(offset - word_offset - 1,
                    bitset_fls(word));
            } else {
                bitset_resize(result, result->length + 2);
                result->buffer[pos++] = BITSET_CREATE_EMPTY_FILL(offset - word_offset - 1);
                result->buffer[pos++] = word;
            }
        }
        word_offset = offset;
    }
    bitset_malloc_free(offsets);
    bitset_hash_free(words);
    return result;
}

bitset_offset bitset_operation_count(bitset_operation_t *operation) {
    bitset_offset count = 0;
    bitset_hash_t *words = bitset_operation_iter(operation);
    size_t i;
    for ( i = 0; i < words->size; i++) {
        bitset_hash_bucket_t *bucket = words->buckets[i];
        if (BITSET_IS_TAGGED_POINTER(bucket)) {
            bitset_word word = words->buffer[i];
            BITSET_POP_COUNT(count, word);
            continue;
        }
        while (bucket) {
            BITSET_POP_COUNT(count, bucket->word);
            bucket = bucket->next;
        }
    }
    bitset_hash_free(words);
    return count;
}

