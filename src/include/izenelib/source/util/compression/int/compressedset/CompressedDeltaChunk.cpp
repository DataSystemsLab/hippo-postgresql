#include <util/compression/int/compressedset/CompressedDeltaChunk.h>
#include <util/compression/int/compressedset/Common.h>
#include <util/compression/int/compressedset/Sink.h>
#include <util/compression/int/compressedset/Source.h>

namespace izenelib
{
namespace util
{
namespace compression
{

template <class T>
__attribute__ ((const))
bool needPaddingTo128Bits(const T * inbyte)
{
    return reinterpret_cast<uintptr_t> (inbyte) & 15;
}
CompressedDeltaChunk::CompressedDeltaChunk()
{
    assert(!needPaddingTo128Bits(&data_[0]));
    compressedSize_ = 0;
}

CompressedDeltaChunk::CompressedDeltaChunk(size_t compressedSize):data_(compressedSize)
{
    compressedSize_ = compressedSize;
    //assert(!needPaddingTo128Bits(&data_[0]));
}

CompressedDeltaChunk::CompressedDeltaChunk(istream & in) :compressedSize_(0)
{
    in.read((char*)&compressedSize_,4);
    data_.resize(compressedSize_);
    in.read((char*)&(data_[0]),compressedSize_);
    assert(!needPaddingTo128Bits(&data_[0]));
}

void CompressedDeltaChunk::resize(size_t newsize)
{
    compressedSize_ = newsize;
    data_.resize(newsize);
    vector<uint8,CacheAllocator> tmp(data_);
    swap(data_, tmp);
    assert(!needPaddingTo128Bits(&data_[0]));
}

vector<uint8,CacheAllocator>& CompressedDeltaChunk::getVector()
{
    return data_;
}

CompressedDeltaChunk::~CompressedDeltaChunk()
{
}


size_t CompressedDeltaChunk::getCompressedSize()
{
    return compressedSize_;
}

Sink CompressedDeltaChunk::getSink()
{
    return Sink((char*)&(data_[0]),compressedSize_);
}

Source CompressedDeltaChunk::getSource() const
{
    return Source((char*)&(data_[0]),compressedSize_);
}

void CompressedDeltaChunk::write(ostream & out) const
{
    out.write((char*)&compressedSize_,4);
    out.write((char*)&(data_[0]),compressedSize_);
}

}
}
}
