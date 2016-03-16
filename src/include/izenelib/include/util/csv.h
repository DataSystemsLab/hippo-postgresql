#ifndef IZENELIB_UTIL_CSV_H
#define IZENELIB_UTIL_CSV_H

#include <iterator>
#include <vector>
#include <string>
#include <stdexcept>

#include <boost/shared_ptr.hpp>

#include <types.h>

NS_IZENELIB_UTIL_BEGIN

class csv_iterator;

class csv_parser
{
    friend class csv_iterator;

private:
    class iterator_handle;

public:
    typedef std::vector<const char*> row_type;
    typedef iterator_handle iterator;

    template <class Iterator>
    csv_parser(Iterator b, Iterator e)
    {
        init(b, e);
    }

    explicit csv_parser(std::istream &ifs)
    {
        init(std::istreambuf_iterator<char>(ifs),
             std::istreambuf_iterator<char>());
    }

    explicit csv_parser(const std::string &str)
    {
        init(str.begin(), str.end());
    }

    bool next()
    {
        has_next=p->next();
        return has_next;
    }

    bool end() const
    {
        return !has_next;
    }

    const row_type &get() const
    {
        return p->get();
    }

private:
    template <class Iterator>
    void init(Iterator b, Iterator e)
    {
        p=boost::shared_ptr<parser>(new parser_impl<Iterator>(b, e));
        has_next=true;
    }

    void copy(row_type &row, std::vector<char> &buf) const
    {
        return p->copy(row, buf);
    }

    class parser
    {
    public:
        virtual ~parser() {}
        virtual bool next() = 0;
        virtual row_type &get() = 0;
        virtual const row_type &get() const = 0;
        virtual void copy(row_type &row, std::vector<char> &buf) const = 0;
    };

    template <class Iterator>
    class parser_impl : public parser
    {
    public:
        parser_impl(Iterator p, Iterator q)
            : p(p), q(q), buf(1024, '\0') {}

        bool next()
        {
            cur.clear();
            if (p==q) return false;

            for (size_t bufcnt=0; ; )
            {
                size_t start=bufcnt;

                if (*p=='\"')
                {
                    p++;
                    for (;;)
                    {
                        if (*p=='\"')
                        {
                            p++;
                            if (*p!='\"') break;
                        }
                        if (bufcnt>=buf.size()) extend();
                        buf[bufcnt++]=*p++;
                    }
                    while(*p && *p!=',' && *p!='\n') p++;
                }
                else
                {
                    while(*p && *p!=',' && *p!='\r' && *p!='\n')
                    {
                        if (bufcnt>=buf.size()) extend();
                        buf[bufcnt++]=*p++;
                    }
                }

                buf[bufcnt++]='\0';
                cur.push_back(&buf[start]);
                if (!*p) break;
                if (*p=='\r') p++;
                if (*p=='\n')
                {
                    p++;
                    break;
                }
                p++;
            }

            return true;
        }

        row_type &get()
        {
            return cur;
        }

        const row_type &get() const
        {
            return cur;
        }

        void copy(row_type &row, std::vector<char> &buf) const
        {
            buf=this->buf;
            row.resize(this->cur.size());
            for (size_t i=0; i<this->cur.size(); i++)
                row[i]=(&buf[0]+(this->cur[i]-&(this->buf[0])));
        }

    private:
        void extend()
        {
            const char *o=&buf[0];
            buf.resize(buf.size()*2);
            for (size_t i=0; i<cur.size(); i++)
                cur[i]=&buf[0]+(cur[i]-o);
        }

        Iterator p, q;
        row_type cur;
        std::vector<char> buf;
    };

    boost::shared_ptr<parser> p;
    bool has_next;
};

class csv_iterator
{
public:
    typedef std::vector<const char*> row_type;

    explicit csv_iterator(csv_parser &p): p(&p), cached(false)
    {
        ++(*this);
    }

    csv_iterator(): p(NULL), cached(false)
    {
    }

    csv_iterator(const csv_iterator &r)
        : p(r.p), cached(false)
    {
        if (p)
        {
            cached=true;
            p->copy(cur, buf);
        }
    }

    bool operator==(const csv_iterator &r) const
    {
        if (!p && !r.p) return true;
        return false;
    }
    bool operator!=(const csv_iterator &r) const
    {
        return !((*this)==r);
    }

    const row_type &operator*() const
    {
        if (cached) return cur;
        if (!p) throw std::runtime_error("no more rows");
        return p->get();
    }

    const row_type *operator->() const
    {
        if (cached) return &cur;
        if (!p) return NULL;
        return &p->get();
    }

    csv_iterator &operator++()
    {
        cached=false;
        if (!p->next()) p=NULL;
        return *this;
    }

    csv_iterator operator++(int)
    {
        csv_iterator ret(*this);
        ++(*this);
        return ret;
    }

private:
    csv_parser *p;

    bool cached;
    row_type cur;
    std::vector<char> buf;
};

void parse_csv(const std::string &str, std::vector<std::vector<std::string> > &ret);


NS_IZENELIB_UTIL_END

#endif // IZENELIB_UTIL_CSV_H
