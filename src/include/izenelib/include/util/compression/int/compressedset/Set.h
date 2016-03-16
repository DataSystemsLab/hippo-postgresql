#ifndef IZENELIB_UTIL_COMPRESSION_COMPRESSED_SET_ABSTRACT_SET_H__
#define IZENELIB_UTIL_COMPRESSION_COMPRESSED_SET_ABSTRACT_SET_H__

#include <boost/shared_ptr.hpp>
#include <limits>
#include <iostream>

namespace izenelib
{
namespace util
{
namespace compression
{

const unsigned int NO_MORE_DOCS = std::numeric_limits<unsigned int>::max();

class Set
{
public:
    class Iterator
    {
    public:
        virtual unsigned int docID() = 0;
        virtual unsigned int nextDoc() = 0;
        // very efficient Advance method implemented with Skip list
        virtual unsigned int Advance(unsigned int target) = 0;
        virtual ~Iterator() {};
    };
    virtual boost::shared_ptr<Iterator> iterator()  const = 0;
    virtual void addDocs(unsigned int* docids,size_t start,size_t len) {};
    virtual void addDoc(unsigned int docId) {};
    // Free up unused memory in dynamic collection
    virtual void compact() {};
    // Force all docId to be part of a compressed chunk.
    virtual void flush() {};
    virtual bool find(unsigned int target) const = 0;
    virtual ~Set() {}
    virtual void write(std::ostream & out) {}
    virtual void read(std::istream & in) {}
    virtual void size(std::istream & in) {}
    virtual unsigned int size() const =0;
};
}
}
}
#endif  // IZENELIB_UTIL_COMPRESSION_COMPRESSED_SET_ABSTRACT_SET_H__
