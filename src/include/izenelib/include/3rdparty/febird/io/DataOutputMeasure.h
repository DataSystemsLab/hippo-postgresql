/* vim: set tabstop=4 : */

#include "DataOutput.h"

#include <boost/type_traits/detail/bool_trait_def.hpp>

namespace febird {

class TestFixedSizeOutput
{
public:
	bool isFixed;

	TestFixedSizeOutput() : isFixed(true) {}

	template<class T> TestFixedSizeOutput& operator<<(const T& x)
	{
		DataIO_saveObject(*this, x);
		return *this;
	}
	template<class T> TestFixedSizeOutput& operator &(const T& x) {	return *this << x; }

#define DATA_IO_GEN_IsFixedSize(type) \
	TestFixedSizeOutput& operator &(const type&) { return *this; } \
	TestFixedSizeOutput& operator<<(const type&) { return *this; }

	DATA_IO_GEN_IsFixedSize(float)
	DATA_IO_GEN_IsFixedSize(double)
	DATA_IO_GEN_IsFixedSize(long double)

	DATA_IO_GEN_IsFixedSize(char)
	DATA_IO_GEN_IsFixedSize(unsigned char)
	DATA_IO_GEN_IsFixedSize(signed char)
	DATA_IO_GEN_IsFixedSize(wchar_t)

	DATA_IO_GEN_IsFixedSize(int)
	DATA_IO_GEN_IsFixedSize(unsigned int)
	DATA_IO_GEN_IsFixedSize(short)
	DATA_IO_GEN_IsFixedSize(unsigned short)
	DATA_IO_GEN_IsFixedSize(long)
	DATA_IO_GEN_IsFixedSize(unsigned long)

#if defined(BOOST_HAS_LONG_LONG)
	DATA_IO_GEN_IsFixedSize(long long)
	DATA_IO_GEN_IsFixedSize(unsigned long long)
#elif defined(BOOST_HAS_MS_INT64)
	DATA_IO_GEN_IsFixedSize(__int64)
	DATA_IO_GEN_IsFixedSize(unsigned __int64)
#endif

	template<class T> TestFixedSizeOutput& operator&(pass_by_value<T> x) { return *this << x.val; }
	template<class T> TestFixedSizeOutput& operator&(boost::reference_wrapper<T> x) { return *this << x.get(); }

	template<class T> TestFixedSizeOutput& operator<<(pass_by_value<T> x) { return *this << x.val; }
	template<class T> TestFixedSizeOutput& operator<<(boost::reference_wrapper<T> x) { return *this << x.get(); }

	template<class T> TestFixedSizeOutput& operator&(const T* x) { return *this << *x; }
	template<class T> TestFixedSizeOutput& operator&(T* x) { return *this << *x; }

	template<class T> TestFixedSizeOutput& operator<<(const T* x) { return *this << *x; }
	template<class T> TestFixedSizeOutput& operator<<(T* x) { return *this << *x; }

	template<class T, int Dim>
	Final_Output& operator<<(const T (&x)[Dim]) { return *this << x[0]; }

	Final_Output& operator<<(const char* s) { isFixed = false; return *this; }
	Final_Output& operator<<(      char* s) { isFixed = false; return *this; }
	Final_Output& operator<<(const signed char* s) { isFixed = false; return *this; }
	Final_Output& operator<<(      signed char* s) { isFixed = false; return *this; }
	Final_Output& operator<<(const unsigned char* s) { isFixed = false; return *this; }
	Final_Output& operator<<(      unsigned char* s) { isFixed = false; return *this; }

	Final_Output& operator<<(const wchar_t* s){ isFixed = false; return *this; }
	Final_Output& operator<<(      wchar_t* s){ isFixed = false; return *this; }

	Final_Output& operator<<(const std::string& x){ isFixed = false; return *this; }
	Final_Output& operator<<(const std::wstring& x){ isFixed = false; return *this; }

	template<class Elem, class Alloc>
	Final_Output& operator<<(const std::vector<Elem, Alloc>& x){ isFixed = false; return *this; }

	template<class Elem, class Alloc>
	Final_Output& operator<<(const std::list<Elem, Alloc>& x){ isFixed = false; return *this; }
	
	template<class Elem, class Alloc>
	Final_Output& operator<<(const std::deque<Elem, Alloc>& x){ isFixed = false; return *this; }

	template<class Elem, class Compare, class Alloc>
	Final_Output& operator<<(const std::set<Elem, Compare, Alloc>& x){ isFixed = false; return *this; }
	
	template<class Elem, class Compare, class Alloc>
	Final_Output& operator<<(const std::multiset<Elem, Compare, Alloc>& x){ isFixed = false; return *this; }
	
	template<class Key, class Val, class Compare, class Alloc>
	Final_Output& operator<<(const std::map<Key, Val, Compare, Alloc>& x){ isFixed = false; return *this; }
	
	template<class Key, class Val, class Compare, class Alloc>
	Final_Output& operator<<(const std::multimap<Key, Val, Compare, Alloc>& x){ isFixed = false; return *this; }
};

template<class T> bool IsFixedSize(const T& x)
{
	TestFixedSizeOutput test;
	test & x;
	return test.isFixed;
}

template<class Final_Output> class DataOutputMeasureBase
{
protected:
	size_t m_size;

public:
	BOOST_STATIC_CONSTANT(bool, value = false);
	typedef boost::mpl::true_ type;

	DataOutputMeasureBase() { this->m_size = 0; }

	size_t size() const { return this->m_size(); }

	void ensureWrite(void* , size_t length) { this->m_size += length; }

	Final_Output& operator<<(serialize_version_t x) { return static_cast<Final_Output&>(*this) << var_uint32_t(x.t); }

#define FEBIRD_GEN_MEASURE_SIZE(type, size) \
	Final_Output& operator<<(type x) { this->m_size += size; return static_cast<Final_Output&>(*this); }

#define FEBIRD_GEN_MEASURE_SIZE_FUN(type) \
	Final_Output& operator<<(type x) { this->m_size += sizeof(type); return static_cast<Final_Output&>(*this); }

#define FEBIRD_GEN_MEASURE_VAR_INT(type) \
	Final_Output& operator<<(type x) { this->m_size += sizeof_int(x); return static_cast<Final_Output&>(*this); }

	FEBIRD_GEN_MEASURE_VAR_INT(var_int16_t)
	FEBIRD_GEN_MEASURE_VAR_INT(var_uint16_t)
	FEBIRD_GEN_MEASURE_VAR_INT(var_int32_t)
	FEBIRD_GEN_MEASURE_VAR_INT(var_uint32_t)

#if !defined(BOOST_NO_INT64_T)
	FEBIRD_GEN_MEASURE_VAR_INT(var_int64_t)
	FEBIRD_GEN_MEASURE_VAR_INT(var_uint64_t)
#endif

	FEBIRD_GEN_MEASURE_SIZE_FUN(char)
	FEBIRD_GEN_MEASURE_SIZE_FUN(unsigned char)
	FEBIRD_GEN_MEASURE_SIZE_FUN(signed char)

	FEBIRD_GEN_MEASURE_SIZE_FUN(short)
	FEBIRD_GEN_MEASURE_SIZE_FUN(unsigned short)

	FEBIRD_GEN_MEASURE_SIZE_FUN(int)
	FEBIRD_GEN_MEASURE_SIZE_FUN(unsigned int)
	FEBIRD_GEN_MEASURE_SIZE_FUN(long)
	FEBIRD_GEN_MEASURE_SIZE_FUN(unsigned long)

#if defined(BOOST_HAS_LONG_LONG)
	FEBIRD_GEN_MEASURE_SIZE_FUN(long long)
	FEBIRD_GEN_MEASURE_SIZE_FUN(unsigned long long)
#elif defined(BOOST_HAS_MS_INT64)
	FEBIRD_GEN_MEASURE_SIZE_FUN(__int64)
	FEBIRD_GEN_MEASURE_SIZE_FUN(unsigned __int64)
#endif

	FEBIRD_GEN_MEASURE_SIZE_FUN(float)
	FEBIRD_GEN_MEASURE_SIZE_FUN(double)
	FEBIRD_GEN_MEASURE_SIZE_FUN(long double)

	Final_Output& operator<<(const char* s)
	{
		var_uint32_t n(strlen(s));
		this->m_size += sizeof_int(n) + n;
		return static_cast<Final_Output&>(*this);
	}
	Final_Output& operator<<(const wchar_t* s)
	{
		var_uint32_t n(wcslen(s));
		this->m_size += sizeof_int(n) + n * sizeof(wchar_t);
		return static_cast<Final_Output&>(*this);
	}
	Final_Output& operator<<(const std::string& x)
	{
		var_uint32_t n(x.size());
		this->m_size += sizeof_int(n) + n.t;
		return static_cast<Final_Output&>(*this);
	}
	Final_Output& operator<<(const std::wstring& x)
	{
		var_uint32_t n(x.size());
		this->m_size += sizeof_int(n) + n.t * sizeof(wchar_t);
		return static_cast<Final_Output&>(*this);
	}

	template<class Elem, class Alloc>
	Final_Output& operator<<(const std::vector<Elem, Alloc>& x)
	{
		return measure_seq(x);
	}
	template<class Elem, class Alloc>
	Final_Output& operator<<(const std::list<Elem, Alloc>& x)
	{
		return measure_seq(x);
	}
	template<class Elem, class Alloc>
	Final_Output& operator<<(const std::deque<Elem, Alloc>& x)
	{
		return measure_seq(x);
	}

	template<class Elem, class Compare, class Alloc>
	Final_Output& operator<<(const std::set<Elem, Compare, Alloc>& x)
	{
		return measure_seq(x);
	}
	template<class Elem, class Compare, class Alloc>
	Final_Output& operator<<(const std::multiset<Elem, Compare, Alloc>& x)
	{
		return measure_seq(x);
	}
	template<class Key, class Val, class Compare, class Alloc>
	Final_Output& operator<<(const std::map<Key, Val, Compare, Alloc>& x)
	{
		return measure_seq(x);
	}
	template<class Key, class Val, class Compare, class Alloc>
	Final_Output& operator<<(const std::multimap<Key, Val, Compare, Alloc>& x)
	{
		return measure_seq(x);
	}

protected:
	template<class Seq>
	Final_Output& measure_seq(const Seq& x)
	{
		if (x.empty())
			return 1; // 0 byte store var_uint32(x.size()=0)
		if (IsFixedSize(*x.begin()))
			measure_seq_fixed_elem(x);
		else
			measure_seq_var_elem(x);
		return static_cast<Final_Output&>(*this);
	}
	template<class Seq>
	void measure_seq_fixed_elem(const Seq& x)
	{
		var_uint32_t n(x.size());
		this->m_size += sizeof_int(x) + sizeof(typename Seq::value_type) * n.t;
	}
	template<class Seq>
	void measure_seq_var_elem(const Seq& x)
	{
		var_uint32_t n(x.size());
		static_cast<Final_Output&>(*this) << n;
		for (typename Seq::const_iterator i = x.begin(); i != x.end(); ++i)
			static_cast<Final_Output&>(*this) << *i;
	}
};

class DataOutputMeasure
	: public DataOutput<DataOutputMeasureBase, DataOutputMeasure>
{
public:
};

} // namespace febird


