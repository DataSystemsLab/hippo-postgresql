#ifndef SF1COMMON__SCD__PARSER__H__
#define SF1COMMON__SCD__PARSER__H__

#include "ScdParserTraits.h"
#include <util/streambuf.h>
#include <am/3rdparty/rde_hash.h>

#include <boost/shared_ptr.hpp>
#include <boost/iterator/iterator_facade.hpp>
#include <boost/unordered_set.hpp>
#include <string>
#include <iostream>
#include <fstream>
#include <vector>

namespace izenelib {
enum SCD_TYPE
{
    NOT_SCD = 0,
    INSERT_SCD,
    UPDATE_SCD,
    RTYPE_SCD,
    DELETE_SCD
};

namespace scd {
    class ScdIndexIterator;
}

class ScdParser
{
    friend class scd::ScdIndexIterator;

    /// @brief  Reads a document from the loaded SCD file, when given a DOCID value.
    //          prerequisites: SCD file must be loaded by load(), and getDocIdList() must be called.
    bool getDoc(const ScdPropertyValueType & docId, SCDDoc& doc);

public:
    ScdParser();
    ScdParser(const izenelib::util::UString::EncodingType & encodingType);
    ScdParser(const izenelib::util::UString::EncodingType & encodingType, const char* docDelimiter);
    virtual ~ScdParser();

    static const std::string SCD_TYPE_FLAGS[];
    static const std::string SCD_TYPE_NAMES[];

    static bool checkSCDFormat(const string & file);
    static unsigned checkSCDDate(const string & file);
    static unsigned checkSCDTime(const string & file);
    static SCD_TYPE checkSCDType(const string & file);
    static bool compareSCD(const string & file1, const string & file2);
    static void getScdList(const std::string& path, std::vector<std::string>& scd_list);
    static std::size_t getScdDocCount(const std::string& path);


    /// @brief Release allocated memory and make it as initial state.
    void clear();

    /// @brief Read a SCD file and load data into memory.
    bool load(const std::string& path);

    long getFileSize()
    {
        return size_;
    }

    /// @brief  A utility function to get all the DOCID values from an SCD
    bool getDocIdList(std::vector<ScdPropertyValueType> & list);

    bool getDocIdList(std::vector<DocIdPair > & list);

    class iterator
    {
    public:
        iterator(long offset);

        iterator(ScdParser* pScdParser, unsigned int start_doc);

        iterator(ScdParser* pScdParser, unsigned int start_doc, const std::vector<string>& propertyNameList);

        iterator(const iterator& other);

        ~iterator();

        const iterator& operator=(const iterator& other);

        bool operator==(const iterator& other) const;

        bool operator!=(const iterator& other) const;

        iterator& operator++();

        iterator operator++(int);

        iterator& operator+=(unsigned int offset);

        const SCDDocPtr& operator*();

        long getOffset();

    private:
        SCDDoc* getDoc();

        bool isValid() const;

        /// @brief
        /// It's recommended to handle this processing in application by which SCD is created
        void preProcessDoc(string& strDoc);

        void parseDoc(std::string& str, SCDDoc* doc);

    private:
        std::ifstream* pfs_;

        long prevOffset_;

        long offset_;

        SCDDocPtr doc_;

        boost::shared_ptr<izenelib::util::izene_streambuf> buffer_;

        std::string docDelimiter_;

        std::vector<string> propertyNameList_;

        boost::unordered_set<std::string> pname_set_;
    };  // class iterator

    class cached_iterator : public boost::iterator_facade<cached_iterator, SCDDocPtr const, boost::forward_traversal_tag>
    {
    public:
        typedef std::vector<std::pair<SCDDocPtr, long> > cache_type; //the cached offset is the end position of that doc
        cached_iterator(long offset);
        cached_iterator(ScdParser* pParser, uint32_t start_doc);
        cached_iterator(ScdParser* pParser, uint32_t start_doc, const std::vector<std::string>& pname_list);
        //cached_iterator(const cached_iterator& other);
        ~cached_iterator();
        long getOffset();
        iterator get_iterator() const {return it_;}

    private:
        friend class boost::iterator_core_access;
        void increment();
        const SCDDocPtr dereference() const;
        bool equal(const cached_iterator& other) const;

    private:
        iterator it_;
        long offset_;
        cache_type cache_;
        uint32_t cache_index_;
        static const uint32_t MAX_CACHE_NUM = 1000;

    }; //class cached_iterator

    iterator begin(unsigned int start_doc = 0);
    iterator begin(const std::vector<string>& propertyNameList, unsigned int start_doc = 0);
    iterator end();

    cached_iterator cbegin(unsigned int start_doc = 0);
    cached_iterator cbegin(const std::vector<string>& propertyNameList, unsigned int start_doc = 0);
    cached_iterator cend();

    std::ifstream& fs() { return fs_; }

private:
    std::ifstream fs_;

    long size_;

    izenelib::am::rde_hash<ScdPropertyValueType, long> docOffsetList_;

    const std::string docDelimiter_; /// the boundary between each docs
};
}

#endif
