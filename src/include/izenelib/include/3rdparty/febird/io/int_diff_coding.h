/* vim: set tabstop=4 : */
#ifndef __febird_int_diff_coding_h__
#define __febird_int_diff_coding_h__

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
#endif

//#include <febird/io/var_int.h>
//#include <febird/io/DataIO.h>

namespace febird {

template<class FirstIntType, class DiffIntType = FirstIntType>
class DecodeIntDiff
{
	FirstIntType m_cur;

public:
	template<class Input>
	explicit DecodeIntDiff(Input& input, bool firstIsVarInt=true)
	{
		init(input, firstIsVarInt);
	}
	DecodeIntDiff() {}

	template<class Input>
	void init(Input& input, bool firstIsVarInt=true)
	{
		if (firstIsVarInt) {
			typename febird::var_int<FirstIntType>::type x;
			input >> x;
			m_cur = x.t;
		} else
			input >> m_cur;
	}

	FirstIntType value() const { return m_cur; }
	operator FirstIntType() const { return m_cur; }

	template<class Input>
	friend void DataIO_loadObject(Input& in, DecodeIntDiff<FirstIntType, DiffIntType>& x)
	{
		typename febird::var_int<DiffIntType>::type diff;
		in >> diff;
		x.m_cur += diff.t;
	}
	template<class Output>
	friend void DataIO_saveObject(Output&, const DecodeIntDiff<FirstIntType, DiffIntType>& x)
	{
		Output::do_not_support_serialize_this_class(x);
	}
};

template<class FirstIntType, class DiffIntType = FirstIntType>
class EncodeIntDiff
{
	FirstIntType m_cur;

public:
	template<class Output>
	EncodeIntDiff(Output& output, FirstIntType first, bool firstIsVarInt=true)
	{
		init(output, first, firstIsVarInt);
	}
	EncodeIntDiff() {}

	template<class Output>
	void init(Output& output, FirstIntType first, bool firstIsVarInt=true)
	{
		m_cur = first;
		if (firstIsVarInt)
			output << febird::as_var_int(first);
		else
			output << first;
	}

//	FirstIntType value() const { return m_cur; }
//	operator FirstIntType() const { return m_cur; }

	//! used as:
	//! @code
	//!   EncodeIntDiff<uint32_t, int32_t> encode_diff(output, iVal);
	//!   for (...)
	//!   {
	//!      iVal = get_next_value(...);
	//!      output << encode_diff(iVal);
	//!   }
	//! @endcode
	typename febird::var_int<DiffIntType>::type operator()(FirstIntType next)
	{
		DiffIntType diff = next - m_cur;
		m_cur = next;
		return typename febird::var_int<DiffIntType>::type(diff);
	}
};

} // namespace febird

#endif // __febird_int_diff_coding_h__

