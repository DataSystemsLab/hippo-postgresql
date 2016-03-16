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
#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include "ewok.h"

#define MASK(x) ((eword_t)1 << (x % BITS_IN_WORD))
#define BLOCK(x) (x / BITS_IN_WORD)

struct bitmap *bitmap_new(void)
{
	struct bitmap *bitmap= ewah_malloc(sizeof(struct bitmap));
	/*
	 * This one is changed
	 */
	bitmap->words = ewah_calloc(32*sizeof(eword_t));
	bitmap->word_alloc = 32;
	return bitmap;
}

void bitmap_set(struct bitmap *self, size_t pos)
{
	size_t block = BLOCK(pos);

	if (block >= self->word_alloc) {
		size_t old_size = self->word_alloc;
		self->word_alloc = block * 2;
		if(self->word_alloc>10000)
		{
			elog(LOG,"bitmap_set word_alloc wrong %d position %d",self->word_alloc,pos);
		}
		self->words = ewah_realloc(self->words, self->word_alloc * sizeof(eword_t));

		memset(self->words + old_size, 0x0,
			(self->word_alloc - old_size) * sizeof(eword_t));
	}

	self->words[block] |= MASK(pos);
}

void bitmap_clear(struct bitmap *self, size_t pos)
{
	size_t block = BLOCK(pos);

	if (block < self->word_alloc)
		self->words[block] &= ~MASK(pos);
}

bool bitmap_get(struct bitmap *self, size_t pos)
{
	size_t block = BLOCK(pos);
	return block < self->word_alloc && (self->words[block] & MASK(pos)) != 0;
}

struct bitmap* bitmap_union(struct bitmap *a,struct bitmap *b)
{
	int i;
	if(a->word_alloc>b->word_alloc)
	{
		struct bitmap* result=ewah_malloc(sizeof(struct bitmap));
		eword_t ORresult;
		result->words = ewah_calloc(a->word_alloc*sizeof(eword_t));
		result->word_alloc = a->word_alloc;
		for(i=0;i<b->word_alloc;i++)
		{
			ORresult=a->words[i]|b->words[i];
			memcpy(&result->words[i],&ORresult,sizeof(eword_t));
		}
		for(i=b->word_alloc;i<a->word_alloc;i++)
		{
			memcpy(&result->words[i],&a->words[i],sizeof(eword_t));
		}
		bitmap_free(a);
		bitmap_free(b);
		return result;
	}
	else
	{
		struct bitmap* result=ewah_malloc(sizeof(struct bitmap));
		eword_t ORresult;
		result->words = ewah_calloc(b->word_alloc*sizeof(eword_t));
		result->word_alloc = b->word_alloc;
		for(i=0;i<a->word_alloc;i++)
		{
			ORresult=a->words[i]|b->words[i];
			memcpy(result->words+i,&ORresult,sizeof(eword_t));
		}
		for(i=a->word_alloc;i<b->word_alloc;i++)
		{
			memcpy(&result->words[i],&b->words[i],sizeof(eword_t));
		}
		bitmap_free(a);
		bitmap_free(b);
		return result;
	}
}

struct bitmap* bitmap_copy(struct bitmap *a)
{
	//elog(LOG,"Start to copy");
	struct bitmap* result=ewah_malloc(sizeof(struct bitmap));
	result->words = ewah_calloc(a->word_alloc*sizeof(eword_t));
	result->word_alloc = a->word_alloc;
	int i;
	//elog(LOG,"Start to memcpy");
	for(i=0;i<a->word_alloc;i++)
	{
		memcpy(result->words+i,a->words+i,sizeof(eword_t));
	}
	//elog(LOG,"Finish memcpy");
	bitmap_free(a);
	//elog(LOG,"End to memcpy");
	return result;
}

struct ewah_bitmap *bitmap_compress(struct bitmap *bitmap)
{
	struct ewah_bitmap *ewah = ewah_new();
	size_t i, running_empty_words = 0;
	eword_t last_word = 0;

	for (i = 0; i < bitmap->word_alloc; ++i) {

		if (bitmap->words[i] == 0) {
			running_empty_words++;
			continue;
		}

		if (last_word != 0) {
			ewah_add(ewah, last_word);
		}

		if (running_empty_words > 0) {
			ewah_add_empty_words(ewah, false, running_empty_words);
			running_empty_words = 0;
		}

		last_word = bitmap->words[i];

	}

	ewah_add(ewah, last_word);
	return ewah;
}

struct bitmap *ewah_to_bitmap(struct ewah_bitmap *ewah)
{
	struct bitmap *bitmap = bitmap_new();
	struct ewah_iterator it;
	eword_t blowup;
	size_t i = 0;

	ewah_iterator_init(&it, ewah);

	while (ewah_iterator_next(&blowup, &it)) {
		if (i >= bitmap->word_alloc) {
			bitmap->word_alloc *= 1.5;
			if(bitmap->word_alloc>10000)
			{
				elog(ERROR,"ewah_to_bitmap word_alloc wrong %d",bitmap->word_alloc);
			}
			bitmap->words = ewah_realloc(
				bitmap->words, bitmap->word_alloc * sizeof(eword_t));
		}

		bitmap->words[i++] = blowup;
	}

	bitmap->word_alloc = i;
	return bitmap;
}

void bitmap_free(struct bitmap *bitmap)
{
	pfree(bitmap->words);
	pfree(bitmap);
}
