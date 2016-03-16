// Copyright (c) 2008, WEST, Polytechnic Institute of NYU
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
//  1. Redistributions of source code must retain the above copyright notice,
//     this list of conditions and the following disclaimer.
//  2. Redistributions in binary form must reproduce the above copyright notice,
//     this list of conditions and the following disclaimer in the documentation
//     and/or other materials provided with the distribution.
//  3. Neither the name of WEST, Polytechnic Institute of NYU nor the names
//     of its contributors may be used to endorse or promote products derived
//     from this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

// Author(s): Torsten Suel, Jiangong Zhang, Jinru He
//
// If you have any questions or problems with our code, please contact:
// jhe@cis.poly.edu
//
// PForDelta Coding:
// This coding method is fast but compression efficiency is not as good as Rice
// coding. It is a blockwise coding, so you need to first set the block size to
// either 32, 64, 128, or 256. The default block size is 128. If the input
// buffer to the Compression() function is greater in size than the block size,
// any integers past the block size will be discarded.
// The meta data is stored as an uncompressed integer written at the beginning
// of the buffer; it includes the number of bits used per integer, the block
// size, and the number of integers coded without exceptions.
#ifndef IZENE_UTIL_COMPRESSION_INT_PFORDELTA_COMPRESSOR_H
#define IZENE_UTIL_COMPRESSION_INT_PFORDELTA_COMPRESSOR_H

#include <util/compression/int/coding.h>

namespace izenelib{namespace util{namespace compression{

class pfordelta_compressor:public coding
{
public:
    pfordelta_compressor();

    ~pfordelta_compressor(){}
public:
    int compress(unsigned int* input, unsigned int* output, int size);

    int decompress(unsigned int* input, unsigned int* output, int size);

    void set_blocksize(int blockSize);
private:
    int _compress_block(unsigned int* input, unsigned int* output);
	
    int _decompress_block(unsigned int* input, unsigned int* output);
	
    int pfor_encode(unsigned int** w, unsigned int* p, int num);

    unsigned* pfor_decode(unsigned int* _p, unsigned int* _w, int flag);

    int block_size_;

    float FRAC;  // The percentage of exceptions in the array.
    int cnum_[17];

    int b;
    int start;
    int t;
    int unpack_count;
};

}}}
#endif


