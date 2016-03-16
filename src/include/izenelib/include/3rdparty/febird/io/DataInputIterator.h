/* vim: set tabstop=4 : */
#ifndef __febird_io_DataInputIterator_h__
#define __febird_io_DataInputIterator_h__

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
#endif

#ifndef __febird_io_DataInput_h__
#include "DataInput.h"
#endif

namespace febird {

template<class StreamClass>
LittleEndianDataInput<StreamClass*> LittleEndianDataInputer(StreamClass* stream)
{
	return LittleEndianDataInput<StreamClass*>(stream);
}

template<class StreamClass>
PortableDataInput<StreamClass*> PortableDataInputer(StreamClass* stream)
{
	return PortableDataInput<StreamClass*>(stream);
}
//////////////////////////////////////////////////////////////////////////

template<class DataInput, class T>
class DataInputIterator :
	public boost::input_iterator_helper<DataInputIterator<DataInput, T>, T>
{
	DataInput m_input;
	size_t m_count;

public:
	//! ���е� count ��֪������������� iterator
	DataInputIterator(DataInput input, size_t count)
		: m_input(input), m_count(count)
	{
		assert(m_count > 0);
	}

	//! ���е� count ���� stream �У�����ʱ��ȡ��(var_uint32_t �� count)
	DataInputIterator(DataInput input)
		: m_input(input)
	{
		var_uint32_t x;  input >> x;
		m_count = x.t;
	}

	DataInputIterator()
		: m_count(0) {}

	//! ��ȡ֮��������ǰ�ߣ����ԣ�ͬһ��λ��ֻ�ܶ�ȡһ��
	T operator*()
	{
		assert(m_count > 0);
		--m_count;

		T x; m_input >> x;
		return x;
	}

	//! �޲���
	DataInputIterator& operator++()
	{
		assert(m_count >= 0);
		return *this;
	}

	bool operator==(const DataInputIterator& r) const
	{
		return r.m_count == this->m_count;
	}

	bool is_end() const { return 0 == m_count; }

	size_t count() const { return m_count; }
};

//////////////////////////////////////////////////////////////////////////

}

#endif // __febird_io_DataInputIterator_h__

