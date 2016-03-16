#ifndef SF1COMMON_JSONDOCUMENT_H
#define SF1COMMON_JSONDOCUMENT_H
//#include "Document.h"
#include <3rdparty/json/json.h>
#include <boost/algorithm/string/trim.hpp>
#include <boost/variant.hpp>
namespace izenelib
{
class JsonDocument
{
    class Visitor : public boost::static_visitor<void>
    {
    public:
        Visitor(const std::string& k, Json::Value& j):key(k), json(j)
        {
        }

        template <typename T>
        void operator()(const T& value) const
        {
            json[key] = value;
        }

        void operator()(const izenelib::util::UString& value) const
        {
            std::string str;
            value.convertString(str, izenelib::util::UString::UTF_8);
            json[key]=str;
        }
        void operator()(const int32_t& value) const
        {
            json[key] = Json::Value::Int(value);
        }
        void operator()(const int64_t& value) const
        {
            json[key] = Json::Value::Int(value);
        }
        void operator()(const std::vector<izenelib::util::UString>& value) const
        {
        }
        void operator()(const std::vector<uint32_t>& value) const
        {
        }
    private:
        const std::string& key;
        Json::Value& json;
    };
public:
    static void JsonToText(const Json::Value& json, izenelib::util::UString& text)
    {
        Json::FastWriter writer;
        std::string str_value = writer.write(json);
        boost::algorithm::trim(str_value);
        text = izenelib::util::UString(str_value, izenelib::util::UString::UTF_8);
    }
    static void JsonToString(const Json::Value& json, std::string& text)
    {
        Json::FastWriter writer;
        text = writer.write(json);
        boost::algorithm::trim(text);
    }
    template <class Document>
    static void ToJson(const Document& doc, Json::Value& json)
    {
        for(typename Document::property_const_iterator it=doc.propertyBegin();it!=doc.propertyEnd();++it)
        {
            boost::apply_visitor( Visitor(it->first, json), it->second.getVariant());
        }
    }
    template <class Document>
    static void ToJsonText(const Document& doc, izenelib::util::UString& text)
    {
        Json::Value json;
        ToJson(doc, json);
        JsonToText(json, text);
    }
    template <class Document>
    static void ToJson(const std::vector<Document>& docs, Json::Value& json)
    {
        //json.reset<Json::Value::ArrayType>();
        json.resize(docs.size());
        for(uint32_t i=0;i<docs.size();i++)
        {
            const Document& doc = docs[i];
            for(typename Document::property_const_iterator it=doc.propertyBegin();it!=doc.propertyEnd();++it)
            {
                boost::apply_visitor( Visitor(it->first, json[i]), it->second.getVariant());
            }
        }
    }
    template <class Document>
    static void ToJsonText(const std::vector<Document>& docs, izenelib::util::UString& text)
    {
        Json::Value json(Json::arrayValue);
        ToJson(docs, json);
        JsonToText(json, text);
    }
    template <class Document>
    static void ToDocument(const Json::Value& json, Document& doc)
    {
        for (Json::Value::iterator it = json.begin(), it_end = json.end(); it != it_end; ++it)
        {
            std::string key = it.key().asCString();
            Json::Value& item = (*it);
            if(item.type()==Json::intValue)
            {
                doc.property(key) = (int64_t)item.asInt();
            }
            else if(item.type()==Json::stringValue)
            {
                doc.property(key) = str_to_propstr(item.asCString());
            }
            else if(item.type()==Json::realValue)
            {
                doc.property(key) = item.asDouble();
            }
        }
    }
};
}

#endif

