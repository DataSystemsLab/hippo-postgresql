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
#include <stdlib.h>
#include <unistd.h>

#include "ewok.h"

#if defined(__linux__)
#  include <endian.h>
#elif defined(__FreeBSD__) || defined(__NetBSD__)
#  include <sys/endian.h>
#elif defined(__OpenBSD__)
#  include <sys/types.h>
#  define be16toh(x) betoh16(x)
#  define be32toh(x) betoh32(x)
#  define be64toh(x) betoh64(x)
#endif

int ewah_serialize_original(struct ewah_bitmap *self, int fd)
{
	size_t i;
	eword_t dump[2048];
	const size_t words_per_dump = sizeof(dump) / sizeof(eword_t);

	/* 32 bit -- bit size fr the map */
	uint32_t bitsize =  htobe32((uint32_t)self->bit_size);
	if (write(fd, &bitsize, 4) != 4)
		return -1;

	/** 32 bit -- number of compressed 64-bit words */
	uint32_t word_count =  htobe32((uint32_t)self->buffer_size);
	if (write(fd, &word_count, 4) != 4)
		return -1;

	/** 64 bit x N -- compressed words */
	const eword_t *buffer = self->buffer;
	size_t words_left = self->buffer_size;

	while (words_left >= words_per_dump) {
		for (i = 0; i < words_per_dump; ++i, ++buffer)
			dump[i] = htobe64(*buffer);

		if (write(fd, dump, sizeof(dump)) != sizeof(dump))
			return -1;

		words_left -= words_per_dump;
	}

	if (words_left) {
		for (i = 0; i < words_left; ++i, ++buffer)
			dump[i] = htobe64(*buffer);

		if (write(fd, dump, words_left * 8) != words_left * 8)
			return -1;
	}

	/** 32 bit -- position for the RLW */
	uint32_t rlw_pos = (uint8_t*)self->rlw - (uint8_t *)self->buffer;
	rlw_pos = htobe32(rlw_pos / sizeof(eword_t));

	if (write(fd, &rlw_pos, 4) != 4)
		return -1;

	return 0;
}
int estimate_ewah_size(struct ewah_bitmap *self)
{
	size_t itemsz,i;
	eword_t dump[2048];
	const size_t words_per_dump = sizeof(dump) / sizeof(eword_t);
	itemsz=0;
	/* 32 bit -- bit size fr the map */
	uint32_t bitsize =  htobe32((uint32_t)self->bit_size);
	itemsz+=4;

	/** 32 bit -- number of compressed 64-bit words */
	uint32_t word_count =  htobe32((uint32_t)self->buffer_size);
	itemsz+=4;

	/** 64 bit x N -- compressed words */
	const eword_t *buffer = self->buffer;
	size_t words_left = self->buffer_size;

	while (words_left >= words_per_dump) {
		for (i = 0; i < words_per_dump; ++i, ++buffer)
			dump[i] = htobe64(*buffer);

		itemsz+=sizeof(dump);

		words_left -= words_per_dump;
	}

	if (words_left) {
		for (i = 0; i < words_left; ++i, ++buffer)
			dump[i] = htobe64(*buffer);

		itemsz+=words_left * 8;

	}

	/** 32 bit -- position for the RLW */
	uint32_t rlw_pos = (uint8_t*)self->rlw - (uint8_t *)self->buffer;
	rlw_pos = htobe32(rlw_pos / sizeof(eword_t));

	itemsz+=4;

	return itemsz;
}
int ewah_serialize(struct ewah_bitmap *self, char *diskTuple)
{
	size_t i;
	eword_t dump[2048];
	const size_t words_per_dump = sizeof(dump) / sizeof(eword_t);
	/* 32 bit -- bit size fr the map */
	uint32_t bitsize =  htobe32((uint32_t)self->bit_size);
	memcpy(diskTuple,&bitsize,4);
	diskTuple+=4;

	/** 32 bit -- number of compressed 64-bit words */
	uint32_t word_count =  htobe32((uint32_t)self->buffer_size);
	memcpy(diskTuple,&word_count,4);
	diskTuple+=4;

	/** 64 bit x N -- compressed words */
	const eword_t *buffer = self->buffer;
	size_t words_left = self->buffer_size;

	while (words_left >= words_per_dump) {
		for (i = 0; i < words_per_dump; ++i, ++buffer)
			dump[i] = htobe64(*buffer);

		memcpy(diskTuple,dump,sizeof(dump));
		diskTuple+=sizeof(dump);

		words_left -= words_per_dump;
	}

	if (words_left) {
		for (i = 0; i < words_left; ++i, ++buffer)
			dump[i] = htobe64(*buffer);

		memcpy(diskTuple,dump,words_left * 8);
		diskTuple+=words_left * 8;


	}

	/** 32 bit -- position for the RLW */
	uint32_t rlw_pos = (uint8_t*)self->rlw - (uint8_t *)self->buffer;
	rlw_pos = htobe32(rlw_pos / sizeof(eword_t));

	memcpy(diskTuple,&rlw_pos,4);
	diskTuple+=4;

	return 0;
}




int ewah_deserialize2(struct ewah_bitmap *self, int fd)
{
	size_t i;
	eword_t dump[2048];
	const size_t words_per_dump = sizeof(dump) / sizeof(eword_t);

	/* 32 bit -- bit size fr the map */
	uint32_t bitsize;
	if (read(fd, &bitsize, 4) != 4)
		return -1;

	self->bit_size = (size_t)be32toh(bitsize);

	/** 32 bit -- number of compressed 64-bit words */
	uint32_t word_count;
	if (read(fd, &word_count, 4) != 4)
		return -1;

	self->buffer_size = (size_t)be32toh(word_count);
	self->buffer = ewah_realloc(self->buffer, self->buffer_size * sizeof(eword_t));

	if (!self->buffer)
		return -1;

	/** 64 bit x N -- compressed words */
	eword_t *buffer = self->buffer;
	size_t words_left = self->buffer_size;

	while (words_left >= words_per_dump) {
		if (read(fd, dump, sizeof(dump)) != sizeof(dump))
			return -1;

		for (i = 0; i < words_per_dump; ++i, ++buffer)
			*buffer = be64toh(dump[i]);

		words_left -= words_per_dump;
	}

	if (words_left) {
		if (read(fd, dump, words_left * 8) != words_left * 8)
			return -1;

		for (i = 0; i < words_left; ++i, ++buffer)
			*buffer = be64toh(dump[i]);
	}

	/** 32 bit -- position for the RLW */
	uint32_t rlw_pos;
	if (read(fd, &rlw_pos, 4) != 4)
		return -1;

	self->rlw = self->buffer + be32toh(rlw_pos);

	return 0;
}

int ewah_deserialize(struct ewah_bitmap *self, char *diskGridSet)
{
	size_t i;
	eword_t dump[2048];
	const size_t words_per_dump = sizeof(dump) / sizeof(eword_t);

	/* 32 bit -- bit size fr the map */
	uint32_t bitsize;

	memcpy(&bitsize,diskGridSet,4);
	diskGridSet+=4;
	self->bit_size = (size_t)be32toh(bitsize);

	/** 32 bit -- number of compressed 64-bit words */
	uint32_t word_count;

	memcpy(&word_count,diskGridSet,4);
	diskGridSet+=4;


	self->buffer_size = (size_t)be32toh(word_count);
	self->buffer = ewah_realloc(self->buffer, self->buffer_size * sizeof(eword_t));

	if (!self->buffer)
		return -1;

	/** 64 bit x N -- compressed words */
	eword_t *buffer = self->buffer;
	size_t words_left = self->buffer_size;

	while (words_left >= words_per_dump) {

		memcpy(dump,diskGridSet,sizeof(dump));
		diskGridSet+=sizeof(dump);

		for (i = 0; i < words_per_dump; ++i, ++buffer)
			*buffer = be64toh(dump[i]);

		words_left -= words_per_dump;
	}

	if (words_left) {

		memcpy(dump,diskGridSet,words_left * 8);
		diskGridSet+=words_left * 8;


		for (i = 0; i < words_left; ++i, ++buffer)
			*buffer = be64toh(dump[i]);
	}

	/** 32 bit -- position for the RLW */
	uint32_t rlw_pos;
	memcpy(&rlw_pos,diskGridSet,4);
	diskGridSet+=4;


	self->rlw = self->buffer + be32toh(rlw_pos);

	return 0;
}
