#ifndef  IZENELIB_UTIL_COMPRESSION_COMPRESSED_SET_DELTA_CHUNK_STORE_H__
#define  IZENELIB_UTIL_COMPRESSION_COMPRESSED_SET_DELTA_CHUNK_STORE_H__
#include <vector>
#include <iostream>
#include <boost/shared_ptr.hpp>
#include "CompressedDeltaChunk.h"
using namespace std;

namespace izenelib
{
namespace util
{
namespace compression
{

class DeltaChunkStore
{
    vector<boost::shared_ptr<CompressedDeltaChunk> > data2;
public:
    DeltaChunkStore()
    {
    }

    boost::shared_ptr<CompressedDeltaChunk> allocateBlock(size_t compressedSize)
    {
        boost::shared_ptr<CompressedDeltaChunk> compblock(new CompressedDeltaChunk(compressedSize));
        return compblock;
    }

    void add(const boost::shared_ptr<CompressedDeltaChunk>& val)
    {
        data2.push_back(val);
    }

    const CompressedDeltaChunk& get(int index) const
    {
        return *data2[index];
    }

    void compact()
    {
        if (data2.size() != data2.capacity())
        {
            vector<boost::shared_ptr<CompressedDeltaChunk> > tmp = data2;
            swap(data2, tmp);
        }
    }

    size_t size() const
    {
        return  data2.size();
    }

    int getSerialIntNum() const
    {
        int num = 1; // _len
        for(size_t i=0; i<data2.size(); i++)
        {
            num += 1 + (*data2[i]).getCompressedSize(); // 1 is the int to record the length of the array
        }
        return num;
    }

    void write(ostream & out) const
    {
        int size = data2.size();
        out.write((char*)&size,4);

        for(size_t i=0; i<data2.size(); i++)
        {
            (*data2[i]).write(out);
        }
    }

    void read(istream & in)
    {
        int size = 0;
        in.read((char*)&size,4);
        data2.clear();
        for(int i = 0; i<size; i++)
        {
            boost::shared_ptr<CompressedDeltaChunk> compblock(new CompressedDeltaChunk(in));
            data2.push_back(compblock);
        }
    }
};
}
}
}
#endif // DELTA_CHUNK_STORE_H__
