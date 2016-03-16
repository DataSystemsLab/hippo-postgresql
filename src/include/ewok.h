/**
 * Copyright 2013, GitHub, Inc
 * Copyright 2009-2013, Daniel Lemire, Cliff Moon,
 *	David McIntosh, Robert Becho, Google Inc. and Veronika Zenz
 * 
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
#ifndef __EWOK_BITMAP_C__
#define __EWOK_BITMAP_C__

#include <stdbool.h>
#include <stdint.h>
#include "postgres.h"
#ifndef ewah_malloc
#	define ewah_malloc palloc
#endif
#ifndef ewah_realloc
#	define ewah_realloc repalloc
#endif
#ifndef ewah_calloc
#	define ewah_calloc palloc0
#endif

typedef uint64_t eword_t;
#define BITS_IN_WORD (sizeof(eword_t) * 8)

struct ewah_bitmap {
	eword_t *buffer;
	size_t buffer_size;
	size_t alloc_size;
	size_t bit_size;
	eword_t *rlw;
};


size_t ewah_add(struct ewah_bitmap *self, eword_t word);


/**
 * Allocate a new EWAH Compressed bitmap
 */
struct ewah_bitmap *ewah_new(void);

/**
 * Clear all the bits in the bitmap. Does not free or resize
 * memory.
 */
void ewah_clear(struct ewah_bitmap *bitmap);

/**
 * Free all the memory of the bitmap
 */
void ewah_free(struct ewah_bitmap *bitmap);

/**
 * Load a bitmap from a file descriptor. An empty `ewah_bitmap` instance
 * must have been allocated beforehand.
 *
 * The fd must be open in read mode.
 *
 * Returns: 0 on success, -1 if a reading error occured (check errno)
 */
int ewah_deserialize(struct ewah_bitmap *self, char *diskBitset);

/**
 * Dump an existing bitmap to a file descriptor. The bitmap
 * is dumped in compressed form, with the following structure:
 *
 * | bit_count | number_of_words | words... | rlw_position
 *
 * The fd must be open in write mode.
 *
 * Returns: 0 on success, -1 if a writing error occured (check errno)
 */
//int ewah_serialize(struct ewah_bitmap *self, int fd);
int ewah_serialize(struct ewah_bitmap *self, char *diskTuple);
/**
 * Logical not (bitwise negation) in-place on the bitmap
 *
 * This operation is linear time based on the size of the bitmap.
 */
void ewah_not(struct ewah_bitmap *self);

/**
 * Call the given callback with the position of every single bit
 * that has been set on the bitmap.
 *
 * This is an efficient operation that does not fully decompress
 * the bitmap.
 */
void ewah_each_bit(struct ewah_bitmap *self, void (*callback)(size_t, void*), void *payload);

/**
 * Set a given bit on the bitmap.
 *
 * The bit at position `pos` will be set to true. Because of the
 * way that the bitmap is compressed, a set bit cannot be unset
 * later on.
 *
 * Furthermore, since the bitmap uses streaming compression, bits
 * can only set incrementally.
 *
 * E.g.
 *		ewah_set(bitmap, 1); // ok
 *		ewah_set(bitmap, 76); // ok
 *		ewah_set(bitmap, 77); // ok
 *		ewah_set(bitmap, 8712800127); // ok
 *		ewah_set(bitmap, 25); // failed, assert raised
 */
void ewah_set(struct ewah_bitmap *self, size_t i);

/**
 * Add a stream of empty words to the bitstream
 *
 * This is an internal operation used to efficiently generate
 * compressed bitmaps.
 */
size_t ewah_add_empty_words(struct ewah_bitmap *self, bool v, size_t number);

struct ewah_iterator {
	const eword_t *buffer;
	size_t buffer_size;

	size_t pointer;
	eword_t compressed, literals;
	eword_t rl, lw;
	bool b;
};

/**
 * Initialize a new iterator to run through the bitmap in uncompressed form.
 *
 * The iterator can be stack allocated. The underlying bitmap must not be freed
 * before the iteration is over.
 *
 * E.g.
 *
 *		struct ewah_bitmap *bitmap = ewah_new();
 *		struct ewah_iterator it;
 *
 *		ewah_iterator_init(&it, bitmap);
 */
void ewah_iterator_init(struct ewah_iterator *it, struct ewah_bitmap *parent);

/**
 * Yield every single word in the bitmap in uncompressed form. This is:
 * yield single words (32-64 bits) where each bit represents an actual
 * bit from the bitmap.
 *
 * Return: true if a word was yield, false if there are no words left
 */
bool ewah_iterator_next(eword_t *next, struct ewah_iterator *it);

void ewah_or(
	struct ewah_bitmap *bitmap_i,
	struct ewah_bitmap *bitmap_j,
	struct ewah_bitmap *out);

void ewah_and_not(
	struct ewah_bitmap *bitmap_i,
	struct ewah_bitmap *bitmap_j,
	struct ewah_bitmap *out);

void ewah_xor(
	struct ewah_bitmap *bitmap_i,
	struct ewah_bitmap *bitmap_j,
	struct ewah_bitmap *out);

void ewah_and(
	struct ewah_bitmap *bitmap_i,
	struct ewah_bitmap *bitmap_j,
	struct ewah_bitmap *out);

void ewah_dump(struct ewah_bitmap *bitmap);

void ewah_add_dirty_words(
	struct ewah_bitmap *self, const eword_t *buffer, size_t number, bool negate);

/**
 * Uncompressed, old-school bitmap that can be efficiently compressed
 * into an `ewah_bitmap`.
 */
struct bitmap {
	eword_t *words;
	size_t word_alloc;
};

int estimate_ewah_size(struct ewah_bitmap *self);

struct bitmap *bitmap_new(void);
void bitmap_set(struct bitmap *self, size_t pos);
void bitmap_clear(struct bitmap *self, size_t pos);
bool bitmap_get(struct bitmap *self, size_t pos);
struct bitmap* bitmap_union(struct bitmap *a,struct bitmap *b);
struct bitmap* bitmap_copy(struct bitmap *a);

struct ewah_bitmap * bitmap_compress(struct bitmap *bitmap);
struct bitmap *ewah_to_bitmap(struct ewah_bitmap *ewah);
void bitmap_free(struct bitmap *bitmap);
#endif
