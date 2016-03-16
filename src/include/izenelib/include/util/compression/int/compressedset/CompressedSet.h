/*
* Effectively compressing sorted integer arrays and performing highly efficient operations on the compressed arrays.
* Operation include : 
*   -Fast sequential scanning (iteration) speed over the compressed lists. 
*   -Fast random lookup of docIds on the compressed lists.
*   -Fast finding intersections of the compressed lists. (lookup in combination of several bitmaps index)
*  Based on
*  SIMD-Based Decoding of Posting Lists  CIKM 2011
*  Accelerating Search and Recognition Workloads with SSE 4.2 String and Text Processing Instructions
*       Performance Analysis of Systems and Software (ISPASS), 2011 IEEE
*  Fast Sorted-Set Intersection using SIMD Instructions ADMS 2011
*/
#ifndef IZENELIB_UTIL_COMPRESSION_COMPRESSED_SET_COMPRESSED_SET_H__
#define IZENELIB_UTIL_COMPRESSION_COMPRESSED_SET_COMPRESSED_SET_H__

#define  BLOCK_SIZE_BIT 11
// should be a 2 ^ BLOCK_SIZE_BIT
// 256 should give good cache alignement
#define  DEFAULT_BATCH_SIZE 2048
// should be DEFAULT_BATCH_SIZE -1
#define  BLOCK_SIZE_MODULO 2047
//int i=-1;
//for(int x=DEFAULT_BATCH_SIZE; x>0; ++i, x = x>> 1) { }
//BLOCK_INDEX_SHIFT_BITS = i;
#define BLOCK_INDEX_SHIFT_BITS 11
#include "Common.h"
#include "Set.h"
#include "DeltaChunkStore.h"
#include "CompressedDeltaChunk.h"
#include "Codec.h"

using namespace std;

namespace izenelib
{
namespace util
{
namespace compression
{

class CompressedSet;

class CompressedSet : public Set
{
public:
    class Iterator : public Set::Iterator
    {
        unsigned int lastAccessedDocId;
        int cursor; // the current pointer of the input
        int totalDocIdNum;

        int compBlockNum; // the number of compressed blocks
        // unsigned int*  iterDecompBlock; // temporary storage for the decompressed data
        // unsigned int* currentNoCompBlock;
        vector<uint32_t,AlignedSTLAllocator<uint32_t, 64> > iterDecompBlock;
        vector<uint32_t,AlignedSTLAllocator<uint32_t, 64> > currentNoCompBlock;

        //parent
        const CompressedSet* set;
        //int BLOCK_INDEX_SHIFT_BITS; // floor(log(blockSize))
        int advanceToTargetInTheFollowingCompBlocksNoPostProcessing(unsigned int target, int startBlockIndex);
        int getBlockIndex(int docIdIndex);
    public:
        Iterator(const CompressedSet* parentSet);
        Iterator(const CompressedSet::Iterator& other);
        // assignator operator disabled for now
        CompressedSet::Iterator& operator=(const CompressedSet::Iterator& rhs);
        ~Iterator();

        unsigned int docID();
        unsigned int nextDoc();
        unsigned  int Advance(unsigned int target);
    };
private:
    int sizeOfCurrentNoCompBlock; // the number of uncompressed elements that is hold in the currentNoCompBlock
    // Two separate arrays containing
    // the last docID
    // of each block in words in uncompressed form.
    vector<unsigned int> baseListForOnlyCompBlocks;


    const CompressedSet& operator=(const CompressedSet& other);


public:
    unsigned int lastAdded; // recently inserted/accessed element
    static Codec codec; // varint encoding codec
    unsigned int totalDocIdNum; // the total number of elemnts that have been inserted/accessed so far
    // unsigned int* currentNoCompBlock;
    vector<uint32_t,AlignedSTLAllocator<uint32_t, 64> > currentNoCompBlock;  // the memory used to store the uncompressed elements. Once the block is full, all its elements are compressed into sequencOfCompBlock and the block is cleared.
    DeltaChunkStore sequenceOfCompBlocks; // Store for list compressed delta chunk


    CompressedSet(const CompressedSet& other);


    /**
     * Swap the content of this bitmap with another bitmap.
     * No copying is done. (Running time complexity is constant.)
     */
    void swap(CompressedSet & x);


    CompressedSet();

    ~CompressedSet();

    /**
     *  Flush the data left in the currentNoCompBlock into the compressed data
     */
    void flush();

    void write(ostream & out);

    void read(istream & in);

    boost::shared_ptr<Set::Iterator>  iterator() const;

    /**
     * Add an array of sorted docIds to the set
     */
    void addDocs(unsigned int docids[],size_t start,size_t len);

    /**
     * Add document to this set
     * Note that you must set the bits in increasing order:
     * addDoc(1), addDoc(2) is ok;
     * addDoc(2), addDoc(1) is not ok.
     */
    void addDoc(unsigned int docId);

    CompressedSet unorderedAdd(unsigned int docId);

    CompressedSet removeDoc(unsigned int docId);

    void compactBaseListForOnlyCompBlocks();

    void compact();

    void initSet();

    /**
     * Prefix Sum
     *
     */
    void preProcessBlock(unsigned int block[], size_t size);

    const boost::shared_ptr<CompressedDeltaChunk> PForDeltaCompressOneBlock(unsigned int* block,size_t blocksize);

    const boost::shared_ptr<CompressedDeltaChunk> PForDeltaCompressCurrentBlock();

    /**
     * Gets the number of ids in the set
     * @return docset size
     */
    unsigned int size() const;

    /**
     * if more then 1/8 of bit are set to 1 in range [minSetValue,maxSetvalue]
     * you should use EWAHBoolArray compression instead
     * because this compression will take at least 8 bits by positions
     */
    bool isDense();

    //This method will not work after a call to flush()
    bool find(unsigned int target) const;
};

}
}
}
#endif  // IZENELIB_UTIL_COMPRESSION_COMPRESSED_SET_COMPRESSED_SET_H__
