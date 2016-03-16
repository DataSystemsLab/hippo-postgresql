/* vim: set tabstop=4 : */
#ifndef __febird_io_BzipStream_h__
#define __febird_io_BzipStream_h__

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
#endif

#include <stdio.h>

#include "../stdtypes.h"
#include "../refcount.h"
#include "IOException.h"
#include "IStream.h"

namespace febird {

class FEBIRD_DLL_EXPORT BzipInputStream	: public RefCounter, public IInputStream
{
	DECLARE_NONE_COPYABLE_CLASS(BzipInputStream)
	void* m_fp;
	FILE* m_cf;

public:
	explicit BzipInputStream(const char* fpath, const char* mode = "rb");
	explicit BzipInputStream(int fd, const char* mode = "rb");
	BzipInputStream() : m_fp(0), m_cf(0) {}
	~BzipInputStream();

	void open(const char* fpath, const char* mode = "rb");
	void dopen(int fd, const char* mode = "rb");

	void close();

	bool isOpen() const { return 0 != m_fp; }
	bool eof() const;

	void ensureRead(void* vbuf, size_t length);
	size_t read(void* buf, size_t size);
};

class FEBIRD_DLL_EXPORT BzipOutputStream : public RefCounter, public IOutputStream
{
	DECLARE_NONE_COPYABLE_CLASS(BzipOutputStream)
	void* m_fp;
	FILE* m_cf;

public:
	explicit BzipOutputStream(const char* fpath, const char* mode = "wb");
	explicit BzipOutputStream(int fd, const char* mode = "wb");
	BzipOutputStream() : m_fp(0), m_cf(0) {}
	~BzipOutputStream();

	void close();

	void open(const char* fpath, const char* mode = "wb");
	void dopen(int fd, const char* mode = "wb");
	bool isOpen() const { return 0 != m_fp; }

	void ensureWrite(const void* vbuf, size_t length);
	size_t write(const void* buf, size_t size);
	void flush();
};

} // namespace febird

#endif

