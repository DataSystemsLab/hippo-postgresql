#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

#include "bitset/malloc.h"
#include "bitset/estimate.h"

bitset_linear_t *bitset_linear_new(size_t size) {
    bitset_linear_t *counter = bitset_malloc(sizeof(bitset_linear_t));
    if (!counter) {
        bitset_oom();
    }
    counter->count = 0;
    size = (size_t)(size / BITSET_LITERAL_LENGTH) + 1;
    size_t pow2;
    BITSET_NEXT_POW2(pow2, size);
    counter->size = pow2;
    counter->words = bitset_calloc(1, counter->size * sizeof(bitset_word));
    if (!counter->words) {
        bitset_oom();
    }
    return counter;
}

void bitset_linear_add(bitset_linear_t *counter, const bitset_t *bitset) {
    bitset_offset offset = 0;
    bitset_word word, mask, tmp;
    unsigned position;
    unsigned offset_mask = counter->size - 1;
    size_t i=0;
    for ( i = 0; i < bitset->length; i++) {
        word = bitset->buffer[i];
        if (BITSET_IS_FILL_WORD(word)) {
            offset += BITSET_GET_LENGTH(word);
            position = BITSET_GET_POSITION(word);
            if (!position) {
                continue;
            }
            mask = BITSET_CREATE_LITERAL(position - 1);
            if ((counter->words[offset & offset_mask] & mask) == 0) {
                counter->count++;
                counter->words[offset & offset_mask] |= mask;
            }
        } else {
            tmp = counter->words[offset & offset_mask];
            counter->words[offset & offset_mask] |= word;
            word &= ~tmp;
            BITSET_POP_COUNT(counter->count, word);
        }
        offset++;
    }
}

unsigned bitset_linear_count(const bitset_linear_t *counter) {
    return counter->count;
}

void bitset_linear_free(bitset_linear_t *counter) {
    bitset_malloc_free(counter->words);
    bitset_malloc_free(counter);
}

bitset_countn_t *bitset_countn_new(unsigned n, size_t size) {
    bitset_countn_t *counter = bitset_malloc(sizeof(bitset_countn_t));
    if (!counter) {
        bitset_oom();
    }
    assert(n);
    counter->n = n;
    size = (size_t)(size / BITSET_LITERAL_LENGTH) + 1;
    size_t pow2;
    BITSET_NEXT_POW2(pow2, size);
    counter->size = pow2;
    counter->words = bitset_malloc(sizeof(bitset_word *) * (counter->n + 1));
    if (!counter->words) {
        bitset_oom();
    }
    //Create N+1 uncompressed bitsets
    size_t i;
    for ( i = 0; i <= n; i++) {
        counter->words[i] = bitset_calloc(1, counter->size * sizeof(bitset_word));
        if (!counter->words[i]) {
            bitset_oom();
        }
    }
    return counter;
}

void bitset_countn_add(bitset_countn_t *counter, const bitset_t *bitset) {
    bitset_offset offset = 0;
    bitset_word word, tmp;
    unsigned position;
    unsigned offset_mask = counter->size - 1;
    size_t i,n;
    for (i = 0; i < bitset->length; i++) {
        word = bitset->buffer[i];
        if (BITSET_IS_FILL_WORD(word)) {
            offset += BITSET_GET_LENGTH(word);
            position = BITSET_GET_POSITION(word);
            if (!position) {
                continue;
            }
            word = BITSET_CREATE_LITERAL(position - 1);
        }
        for (n = 0; n <= counter->n; n++) {
            tmp = word & counter->words[n][offset & offset_mask];
            counter->words[n][offset & offset_mask] |= word;
            word = tmp;
        }
        offset++;
    }
}

unsigned bitset_countn_count(const bitset_countn_t *counter) {
    unsigned count = 0, nth = counter->n - 1, last = counter->n;
    bitset_word word;
    //Find bits that occur in the Nth bitset, but not the N+1th bitset
    size_t offset;
    for ( offset = 0; offset < counter->size; offset++) {
        word = counter->words[nth][offset] & ~counter->words[last][offset];
        if (word) {
            BITSET_POP_COUNT(count, word);
        }
    }
    return count;
}

unsigned *bitset_countn_count_all(const bitset_countn_t *counter) {
    unsigned *counts = bitset_calloc(1, sizeof(unsigned) * counter->n);
    if (!counts) {
        bitset_oom();
    }
    bitset_word word;
    size_t offset,n;
    for ( offset = 0; offset < counter->size; offset++) {
        for (n = 1; n <= counter->n; n++) {
            word = counter->words[n-1][offset] & ~counter->words[n][offset];
            if (word) {
                BITSET_POP_COUNT(counts[n-1], word);
            }
        }
    }
    return counts;
}

unsigned *bitset_countn_count_mask(const bitset_countn_t *counter, const bitset_t *mask) {
    bitset_word *mask_words = bitset_calloc(1, counter->size * sizeof(bitset_word));
    if (!mask_words) {
        bitset_oom();
    }
    bitset_offset offset = 0;
    bitset_word word, mask_word;
    unsigned position;
    size_t i;
    for ( i = 0; i < mask->length; i++) {
        mask_word = mask->buffer[i];
        if (BITSET_IS_FILL_WORD(mask_word)) {
            offset += BITSET_GET_LENGTH(mask_word);
            if (offset >= counter->size) {
                offset %= counter->size;
            }
            position = BITSET_GET_POSITION(mask_word);
            if (!position) {
                continue;
            }
            mask_word = BITSET_CREATE_LITERAL(position - 1);
        }
        mask_words[offset] |= mask_word;
        offset++;
        if (offset >= counter->size) {
            offset -= counter->size;
        }
    }
    unsigned *counts = bitset_calloc(1, sizeof(unsigned) * counter->n);
    if (!counts) {
        bitset_oom();
    }
    size_t offset2,n;
    for ( offset2 = 0; offset2 < counter->size; offset2++) {
        for (n = 1; n <= counter->n; n++) {
            word = counter->words[n-1][offset2] & ~counter->words[n][offset2];
            word &= mask_words[offset2];
            BITSET_POP_COUNT(counts[n-1], word);
        }
    }
    bitset_malloc_free(mask_words);
    return counts;
}

void bitset_countn_count_free(unsigned *counts) {
    bitset_malloc_free(counts);
}

void bitset_countn_free(bitset_countn_t *counter) {
	size_t i;
    for ( i = 0; i <= counter->n; i++) {
        bitset_malloc_free(counter->words[i]);
    }
    bitset_malloc_free(counter->words);
    bitset_malloc_free(counter);
}

