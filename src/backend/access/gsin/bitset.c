#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "bitset/malloc.h"
#include "bitset/operation.h"

bitset_t *bitset_new() {
    bitset_t *bitset = bitset_malloc(sizeof(bitset_t));
    if (!bitset) {
        bitset_oom();
    }
    bitset->length = 0;
    bitset->buffer = NULL;
    return bitset;
}

void bitset_free(bitset_t *bitset) {
    if (bitset->length) {
        bitset_malloc_free(bitset->buffer);
    }
    bitset_malloc_free(bitset);
}

void bitset_resize(bitset_t *bitset, size_t length) {
    size_t current_size, next_size;
    BITSET_NEXT_POW2(next_size, length);
    if (!bitset->length) {
        bitset->buffer = bitset_malloc(sizeof(bitset_word) * next_size);
    } else {
        BITSET_NEXT_POW2(current_size, bitset->length);
        if (next_size > current_size) {
            bitset->buffer = bitset_realloc(bitset->buffer, sizeof(bitset_word) * next_size);
        }
    }
    if (!bitset->buffer) {
        bitset_oom();
    }
    bitset->length = length;
}

void bitset_clear(bitset_t *bitset) {
    bitset->length = 0;
}

size_t bitset_length(const bitset_t *bitset) {
    return bitset->length * sizeof(bitset_word);
}

bitset_t *bitset_copy(const bitset_t *bitset) {
    bitset_t *copy = bitset_calloc(1, sizeof(bitset_t));
    if (!copy) {
        bitset_oom();
    }
    if (bitset->length) {
        size_t size;
        BITSET_NEXT_POW2(size, bitset->length);
        copy->buffer = bitset_malloc(sizeof(bitset_word) * size);
        if (!copy->buffer) {
            bitset_oom();
        }
        memcpy(copy->buffer, bitset->buffer, bitset->length * sizeof(bitset_word));
        copy->length = bitset->length;
    }
    return copy;
}

bool bitset_get(const bitset_t *bitset, bitset_offset bit) {
    if (!bitset->length) {
        return false;
    }
    bitset_offset length, word_offset = bit / BITSET_LITERAL_LENGTH;
    size_t i=0;
    bit %= BITSET_LITERAL_LENGTH;
    for (i = 0; i < bitset->length; i++) {
        if (BITSET_IS_FILL_WORD(bitset->buffer[i])) {
            length = BITSET_GET_LENGTH(bitset->buffer[i]);
            unsigned position = BITSET_GET_POSITION(bitset->buffer[i]);
            if (word_offset < length) {
                return false;
            } else if (position) {
                if (word_offset == length) {
                    return position == bit + 1;
                }
                word_offset--;
            }
            word_offset -= length;
        } else if (!word_offset--) {
            return bitset->buffer[i] & BITSET_CREATE_LITERAL(bit);
        }
    }
    return false;
}

bitset_offset bitset_count(const bitset_t *bitset) {
    bitset_offset count = 0;
    size_t i=0;
    for (i = 0; i < bitset->length; i++) {
        if (BITSET_IS_FILL_WORD(bitset->buffer[i])) {
            if (BITSET_GET_POSITION(bitset->buffer[i])) {
                count += 1;
            }
        } else {
            bitset_word word = bitset->buffer[i];
            BITSET_POP_COUNT(count, word);
        }
    }
    return count;
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

static inline unsigned char bitset_ffs(bitset_word word) {
    static char table[32] = {
        0, 1, 28, 2, 29, 14, 24, 3, 30, 22, 20, 15, 25, 17, 4, 8,
        31, 27, 13, 23, 21, 19, 16, 7, 26, 12, 18, 6, 11, 5, 10, 9
    };
    return table[((bitset_word)((word & -word) * 0x077CB531U)) >> 27] + 1;
}

bitset_offset bitset_min(const bitset_t *bitset) {
    bitset_offset offset = 0;
    size_t i=0;
    for (i = 0; i < bitset->length; i++) {
        if (BITSET_IS_FILL_WORD(bitset->buffer[i])) {
            offset += BITSET_GET_LENGTH(bitset->buffer[i]);
            unsigned position = BITSET_GET_POSITION(bitset->buffer[i]);
            if (position) {
                return offset * BITSET_LITERAL_LENGTH + position - 1;
            }
        } else {
            return offset * BITSET_LITERAL_LENGTH + bitset_fls(bitset->buffer[i]);
        }
    }
    return 0;
}

bitset_offset bitset_max(const bitset_t *bitset) {
    if (!bitset->length) {
        return 0;
    }
    bitset_offset offset = 0;
    size_t i=0;
    for (i = 0; i < bitset->length; i++) {
        if (BITSET_IS_FILL_WORD(bitset->buffer[i])) {
            offset += BITSET_GET_LENGTH(bitset->buffer[i]);
            if (BITSET_GET_POSITION(bitset->buffer[i])) {
                offset += 1;
            }
        } else {
            offset += 1;
        }
    }
    bitset_word last = bitset->buffer[bitset->length-1];
    offset = (offset - 1) * BITSET_LITERAL_LENGTH;
    if (BITSET_IS_FILL_WORD(last)) {
        return offset + BITSET_GET_POSITION(last) - 1;
    }
    return offset + BITSET_LITERAL_LENGTH - bitset_ffs(last);
}

bool bitset_set(bitset_t *bitset, bitset_offset bit) {
    return bitset_set_to(bitset, bit, true);
}

bool bitset_unset(bitset_t *bitset, bitset_offset bit) {
    return bitset_set_to(bitset, bit, false);
}

bool bitset_set_to(bitset_t *bitset, bitset_offset bit, bool value) {
    bitset_offset word_offset = bit / BITSET_LITERAL_LENGTH;
    bit %= BITSET_LITERAL_LENGTH;
    if (bitset->length) {
        bitset_word word;
        bitset_offset fill_length;
        unsigned position;
        size_t i=0;
        for (i = 0; i < bitset->length; i++) {
            word = bitset->buffer[i];
            if (BITSET_IS_FILL_WORD(word)) {
                position = BITSET_GET_POSITION(word);
                fill_length = BITSET_GET_LENGTH(word);
                if (word_offset == fill_length - 1) {
                    if (position) {
                        bitset_resize(bitset, bitset->length + 1);
                        if (i < bitset->length - 1) {
                            memmove(bitset->buffer+i+2, bitset->buffer+i+1,
                                sizeof(bitset_word) * (bitset->length - i - 2));
                        }
                        bitset->buffer[i+1] = BITSET_CREATE_LITERAL(position - 1);
                        if (word_offset) {
                            bitset->buffer[i] = BITSET_CREATE_FILL(fill_length - 1, bit);
                        } else {
                            bitset->buffer[i] = BITSET_CREATE_LITERAL(bit);
                        }
                    } else {
                        if (fill_length - 1 > 0) {
                            bitset->buffer[i] = BITSET_CREATE_FILL(fill_length - 1, bit);
                        } else {
                            bitset->buffer[i] = BITSET_CREATE_LITERAL(bit);
                        }
                    }
                    return false;
                } else if (word_offset < fill_length) {
                    bitset_resize(bitset, bitset->length + 1);
                    if (i < bitset->length - 1) {
                        memmove(bitset->buffer+i+2, bitset->buffer+i+1,
                            sizeof(bitset_word) * (bitset->length - i - 2));
                    }
                    if (!word_offset) {
                        bitset->buffer[i] = BITSET_CREATE_LITERAL(bit);
                    } else {
                        bitset->buffer[i] = BITSET_CREATE_FILL(word_offset, bit);
                    }
                    bitset->buffer[i+1] = BITSET_CREATE_FILL(fill_length - word_offset - 1, position - 1);
                    return false;
                }
                word_offset -= fill_length;
                if (position) {
                    if (!word_offset) {
                        if (position == bit + 1) {
                            if (!value) {
                                bitset->buffer[i] = BITSET_UNSET_POSITION(word);
                            }
                            return true;
                        } else {
                            bitset_resize(bitset, bitset->length + 1);
                            if (i < bitset->length - 1) {
                                memmove(bitset->buffer+i+2, bitset->buffer+i+1,
                                    sizeof(bitset_word) * (bitset->length - i - 2));
                            }
                            bitset->buffer[i] = BITSET_UNSET_POSITION(word);
                            bitset_word literal = 0;
                            literal |= BITSET_CREATE_LITERAL(position - 1);
                            literal |= BITSET_CREATE_LITERAL(bit);
                            bitset->buffer[i+1] = literal;
                            return false;
                        }
                    }
                    word_offset--;
                } else if (!word_offset && i == bitset->length - 1) {
                    bitset->buffer[i] = BITSET_SET_POSITION(word, bit + 1);
                    return false;
                }
            } else if (!word_offset--) {
                bitset_word mask = BITSET_CREATE_LITERAL(bit);
                bool previous = word & mask;
                if (value) {
                    bitset->buffer[i] |= mask;
                } else {
                    bitset->buffer[i] &= ~mask;
                }
                return previous;
            }
        }
    }
    if (value) {
        if (word_offset > BITSET_MAX_LENGTH) {
            bitset_offset fills = word_offset / BITSET_MAX_LENGTH;
            word_offset %= BITSET_MAX_LENGTH;
            bitset_resize(bitset, bitset->length + fills);
            bitset_word fill = BITSET_CREATE_EMPTY_FILL(BITSET_MAX_LENGTH);
            bitset_offset i=0;
            for (i = 0; i < fills; i++) {
                bitset->buffer[bitset->length - fills + i] = fill;
            }
        }
        bitset_resize(bitset, bitset->length + 1);
        if (word_offset) {
            bitset->buffer[bitset->length - 1] = BITSET_CREATE_FILL(word_offset, bit);
        } else {
            bitset->buffer[bitset->length - 1] = BITSET_CREATE_LITERAL(bit);
        }
    }
    return false;
}

bitset_t *bitset_new_buffer(const char *buffer, size_t length) {
    bitset_t *bitset = bitset_malloc(sizeof(bitset_t));
    if (!bitset) {
        bitset_oom();
    }
    bitset->buffer = bitset_malloc(length * sizeof(char));
    if (!bitset->buffer) {
        bitset_oom();
    }
    memcpy(bitset->buffer, buffer, length * sizeof(char));
    bitset->length = length / sizeof(bitset_word);
    return bitset;
}

static int bitset_new_bits_sort(const void *a, const void *b) {
    bitset_offset al = *(bitset_offset *)a, bl = *(bitset_offset *)b;
    return al > bl ? 1 : -1;
}

bitset_t *bitset_new_bits(bitset_offset *bits, size_t count) {
    bitset_t *bitset = bitset_new();
    if (!count) {
        return bitset;
    }
    unsigned pos = 0, rem, next_rem, i;
    bitset_offset word_offset = 0, div, next_div, fills, last_bit;
    bitset_word fill = BITSET_CREATE_EMPTY_FILL(BITSET_MAX_LENGTH);
    qsort(bits, count, sizeof(bitset_offset), bitset_new_bits_sort);
    last_bit = bits[0];
    div = bits[0] / BITSET_LITERAL_LENGTH;
    rem = bits[0] % BITSET_LITERAL_LENGTH;
    bitset_resize(bitset, 1);
    bitset->buffer[0] = 0;
    for (i = 1; i < count; i++) {
        if (bits[i] == last_bit) {
            continue;
        }
        last_bit = bits[i];
        next_div = bits[i] / BITSET_LITERAL_LENGTH;
        next_rem = bits[i] % BITSET_LITERAL_LENGTH;
        if (div == word_offset) {
            bitset->buffer[pos] |= BITSET_CREATE_LITERAL(rem);
            if (div != next_div) {
                bitset_resize(bitset, bitset->length + 1);
                bitset->buffer[++pos] = 0;
                word_offset = div + 1;
            }
        } else {
            bitset_resize(bitset, bitset->length + 1);
            if (div == next_div) {
                bitset->buffer[pos++] = BITSET_CREATE_EMPTY_FILL(div - word_offset);
                bitset->buffer[pos] = BITSET_CREATE_LITERAL(rem);
                word_offset = div;
            } else {
                bitset->buffer[pos++] = BITSET_CREATE_FILL(div - word_offset, rem);
                bitset->buffer[pos] = 0;
                word_offset = div + 1;
            }
        }
        if (next_div - word_offset > BITSET_MAX_LENGTH) {
            fills = (next_div - word_offset) / BITSET_MAX_LENGTH;
            bitset_resize(bitset, bitset->length + fills);
            bitset_offset j=0;
            for (j = 0; j < fills; j++) {
                bitset->buffer[pos++] = fill;
            }
            word_offset += fills * BITSET_MAX_LENGTH;
        }
        div = next_div;
        rem = next_rem;
    }
    if (count > 1 && div == bits[i-2] / BITSET_LITERAL_LENGTH) {
        bitset->buffer[pos] |= BITSET_CREATE_LITERAL(rem);
    } else {
        if (div - word_offset > BITSET_MAX_LENGTH) {
            fills = (div - word_offset) / BITSET_MAX_LENGTH;
            bitset_resize(bitset, bitset->length + fills);
            bitset_offset j=0;
            for (j = 0; j < fills; j++) {
                bitset->buffer[pos++] = fill;
            }
            word_offset += fills * BITSET_MAX_LENGTH;
        }
        bitset->buffer[pos] = BITSET_CREATE_FILL(div - word_offset, rem);
    }
    return bitset;
}

bitset_iterator_t *bitset_iterator_new(const bitset_t *bitset) {
    bitset_iterator_t *iterator = bitset_malloc(sizeof(bitset_iterator_t));
    if (!iterator) {
        bitset_oom();
    }
    iterator->length = bitset_count(bitset);
    if (!iterator->length) {
        iterator->offsets = NULL;
        return iterator;
    }
    iterator->offsets = bitset_malloc(sizeof(bitset_offset) * iterator->length);
    if (!iterator->offsets) {
        bitset_oom();
    }
    bitset_offset offset = 0;
    unsigned position;
    size_t j=0;
    size_t k=0;
    size_t x = BITSET_LITERAL_LENGTH;
    for (j = 0, k = 0; j < bitset->length; j++) {
        if (BITSET_IS_FILL_WORD(bitset->buffer[j])) {
            offset += BITSET_GET_LENGTH(bitset->buffer[j]);
            position = BITSET_GET_POSITION(bitset->buffer[j]);
            if (!position) {
                continue;
            }
            iterator->offsets[k++] = BITSET_LITERAL_LENGTH * offset + position - 1;
        } else {
            for (x = BITSET_LITERAL_LENGTH; x--; ) {
                if (bitset->buffer[j] & (1 << x)) {
                    iterator->offsets[k++] = BITSET_LITERAL_LENGTH * offset
                        + BITSET_LITERAL_LENGTH - x - 1;
                }
            }
        }
        offset++;
    }
    return iterator;
}

void bitset_iterator_free(bitset_iterator_t *iterator) {
    if (iterator->length) {
        bitset_malloc_free(iterator->offsets);
    }
    bitset_malloc_free(iterator);
}

