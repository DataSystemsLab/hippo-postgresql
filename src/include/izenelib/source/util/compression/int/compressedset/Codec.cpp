#include <util/compression/int/compressedset/Codec.h>
#include <util/compression/int/compressedset/Common.h>
#include <vector>
//#include <util/compression/int/compressedset/bitpacking/util.h>

using namespace std;

namespace izenelib
{
namespace util
{
namespace compression
{

Codec::Codec()
{
}


Codec::~Codec()
{
}


//Code below is part of the public interface

bool Codec::findInDeltaArray(const unsigned int* array, size_t size,unsigned int target) const
{
    unsigned int lastId = array[0];
    if (lastId == target) return true;
    // searching while doing prefix sum (to get docIds instead of d-gaps)
    for(unsigned int idx = 1; idx<size; ++idx)
    {
        lastId += (array[idx]+1);
        if (lastId >= target)
            return (lastId == target);
    }
    return false;
}

double Codec::diffclock(clock_t clock1,clock_t clock2) const
{
    double diffticks=clock1-clock2;
    double diffms=(diffticks*1000)/CLOCKS_PER_SEC;
    return diffms;
}

size_t Codec::Uncompress(Source& src, unsigned int* dst,size_t size) const
{
    assert(!needPaddingTo128Bits(dst));


    size_t sourceSize;
    const uint8* srcptr = src.Peek(&sourceSize);
    const uint32_t* srcptr2= (const uint32_t*)srcptr;
    assert(!needPaddingTo128Bits(srcptr2));
    size_t memavailable = size;
    codec.decodeArray(srcptr2, sourceSize/4,dst,memavailable);
    return memavailable*4;
}

}
}
}

