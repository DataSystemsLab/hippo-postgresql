/* vim: set tabstop=4 : */
#ifndef DataBuffer_h__
#define DataBuffer_h__

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
# pragma warning(disable: 4819)
#endif

#include <cstddef>
#include <boost/detail/atomic_count.hpp>
#include <boost/smart_ptr.hpp>

namespace febird {

//! forward declaration
class FEBIRD_DLL_EXPORT DataBufferPtr;

/**
 @brief ֻ��ͨ�� DataBufferPtr ʹ�øö���
 */
class FEBIRD_DLL_EXPORT DataBuffer
{
	boost::detail::atomic_count m_refcount;
	size_t m_size;

private:
	DECLARE_NONE_COPYABLE_CLASS(DataBuffer)
	DataBuffer(size_t size) : m_refcount(0) { m_size = size; }
	~DataBuffer(); // disable

	static DataBuffer* create(size_t size)
	{
		DataBuffer* p = (DataBuffer*)new char[sizeof(DataBuffer) + size];
		new (p) DataBuffer(size); // placement new...
		return p;
	}
	static void destroy(DataBuffer* p)
	{
		char* pb = (char*)p;
		delete [] pb;
	}

	friend inline void intrusive_ptr_add_ref(DataBuffer* p) { ++p->m_refcount; }
	friend inline void intrusive_ptr_release(DataBuffer* p)
	{
		if (0 == --p->m_refcount) DataBuffer::destroy(p);
	}
	friend class DataBufferPtr;

public:
	size_t size() const { return m_size; }
	byte*  data() const { return (byte*)(this + 1); }
//	byte*  end()  const { return (byte*)(this + 1) + m_size; }
};

/**
 @brief ����ӵ���Լ��� buffer

 -# DataBufferPtr ��ռһ��ָ��Ŀռ�
 -# DataBufferPtr ָ���Ŀ����һ���������ڴ�
 */
class FEBIRD_DLL_EXPORT DataBufferPtr : public boost::intrusive_ptr<DataBuffer>
{
	typedef boost::intrusive_ptr<DataBuffer> MyBase;
public:
	DataBufferPtr() {}
	explicit DataBufferPtr(size_t size) : MyBase(DataBuffer::create(size)) {}
};


/**
 @brief ���Ը�Ч�ر���Լ�ӵ�еĻ��߷��Լ����е� buffer

 -# ������Լ�ӵ�е� buffer�������ö���ʹ�����ü��������� buffer
 -# ��������Լ�ӵ�е� buffer����������ָ��
 -# ֱ��ʹ�øö��󼴿ɣ�����ҪΪ��Ч��ʹ��ָ��ö��������ָ��
 */
class FEBIRD_DLL_EXPORT SmartBuffer
{
public:
	explicit SmartBuffer(size_t size = 0)
	{
		m_data = size ? new byte[size] : 0;
		m_size = size;
		m_refCount = new boost::detail::atomic_count(1);
	}
	SmartBuffer(void* vbuf, size_t size)
	{
		m_data = (byte*)vbuf;
		m_size = size;
		m_refCount = 0;
	}
	SmartBuffer(void* pBeg, void* pEnd)
	{
		m_data = (byte*)pBeg;
		m_size = (byte*)pEnd - (byte*)pBeg;
		m_refCount = 0;
	}
	~SmartBuffer()
	{
		if (m_refCount && 0 == --*m_refCount)
		{
			delete m_refCount;
			delete [] m_data;
		}
	}

	SmartBuffer(const SmartBuffer& rhs)
		: m_data(rhs.m_data)
		, m_size(rhs.m_size)
		, m_refCount(rhs.m_refCount)
	{
		if (m_refCount)
			++*m_refCount;
	}

	const SmartBuffer& operator=(const SmartBuffer& rhs)
	{
		SmartBuffer(rhs).swap(*this);
		return *this;
	}

	void swap(SmartBuffer& y)
	{
		std::swap(m_data, y.m_data);
		std::swap(m_size, y.m_size);
		std::swap(m_refCount, y.m_refCount);
	}

	long refcount() const { return m_refCount ? *m_refCount : 0; }

	size_t size() const { return m_size; }
	byte*  data() const { return m_data; }

private:
	byte*  m_data;
	size_t m_size;
	boost::detail::atomic_count* m_refCount;
};

class FEBIRD_DLL_EXPORT AutoFreeMem
{
	void* m_ptr;
public:
	void* get() const { return m_ptr; }
	AutoFreeMem(void* ptr) : m_ptr(ptr) { assert(NULL != ptr); }
	~AutoFreeMem() { ::free(m_ptr); }
};


} // namespace febird

#endif // DataBuffer_h__
