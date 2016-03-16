/* vim: set tabstop=4 : */
#ifndef __febird_stdtypes_h__
#define __febird_stdtypes_h__

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
# pragma warning(disable: 4819)
# pragma warning(disable: 4290)
# pragma warning(disable: 4267) // conversion from 'size_t' to 'uint', possible loss of data
# pragma warning(disable: 4244) // conversion from 'difference_type' to 'int', possible loss of data
#endif

#include "config.h"

#if BOOST_VERSION < 103301
# include <boost/limits.hpp>
#else
# include <boost/detail/endian.hpp>
#endif

#include <boost/cstdint.hpp>
#include <boost/current_function.hpp>

#include <limits.h>

namespace febird {

namespace integer_types
{
	using boost::int8_t;
	using boost::int16_t;
	using boost::int32_t;

	using boost::uint8_t;
	using boost::uint16_t;
	using boost::uint32_t;

#if !defined(BOOST_NO_INT64_T)
	using boost::int64_t;
	using boost::uint64_t;
#endif

	typedef unsigned int uint_t;
	typedef unsigned char byte;

	typedef unsigned long  ulong;
	typedef unsigned short ushort;
}
using namespace integer_types;

#if !defined(BOOST_NO_INT64_T)
typedef uint64_t stream_position_t;
typedef int64_t  stream_offset_t;
#else
typedef uint32_t stream_position_t;
typedef int32_t  stream_offset_t;
#endif

typedef uint32_t ip_addr_t;

//! ���࣬�����ڲ������̳�ʱ�Ļ���ռλ��
class EmptyClass{};

/**
 @name ʹ�� boost::multi_index ʱ������ index ��ʶ
 @{
 */
struct TagID{};
struct TagKey{};
struct TagName{};
struct TagTime{};
struct TagMru{};
//@}

#define TT_PAIR(T) std::pair<T,T>

template<class SizeT, class AlignT>
inline SizeT align_up(SizeT size, AlignT align_size)
{
	size = (size + align_size - 1);
	return size - size % align_size;
}

template<class SizeT, class AlignT>
inline SizeT align_down(SizeT size, AlignT align_size)
{
	return size - size % align_size;
}

#define FEBIRD_HAS_BSET(set, subset) (((set) & (subset)) == (subset))

/*
 * iter = s.end();
 * ibeg = s.begin();
 * if (iter != ibeg) do { --iter;
 *     //
 *     // do something with iter
 *     //
 * } while (iter != ibeg);else;
 * //
 * // this 'else' is intend for use REVERSE_FOR_EACH
 * // within an if-else-while sub sentence
 *
 * // this is faster than using reverse_iterator
 *
 */
//#define REVERSE_FOR_EACH_BEGIN(iter, ibeg)  if (iter != ibeg) do { --iter
//#define REVERSE_FOR_EACH_END(iter, ibeg)    } while (iter != ibeg); else


/////////////////////////////////////////////////////////////
//
//! @note Need declare public/protected after call this macro!!
//
#define DECLARE_NONE_COPYABLE_CLASS(ThisClassName)	\
private:											\
	ThisClassName(const ThisClassName& rhs);		\
	ThisClassName& operator=(const ThisClassName& rhs);
/////////////////////////////////////////////////////////////

#define CURRENT_SRC_CODE_POSTION  \
	__FILE__ ":" BOOST_STRINGIZE(__LINE__) ", in function: " BOOST_CURRENT_FUNCTION

#if defined(_DEBUG) || defined(DEBUG) || !defined(NDEBUG)
#	define DEBUG_only(S) S
#	define DEBUG_perror		perror
#	define DEBUG_printf		printf
#	define DEBUG_fprintf	fprintf
#	define DEBUG_fflush		fflush
#	define FEBIRD_IF_DEBUG(Then, Else)  Then
#	define FEBIRD_RT_assert(exp, ExceptionT)  assert(exp)
#else
#	define DEBUG_only(S)
#	define DEBUG_perror(Msg)
#	define DEBUG_printf		1 ? 0 : printf
#	define DEBUG_fprintf	1 ? 0 : fprintf
#	define DEBUG_fflush(fp)
#	define FEBIRD_IF_DEBUG(Then, Else)  Else
#	define FEBIRD_RT_assert(exp, ExceptionT)  \
	if (!(exp)) { \
		std::ostringstream oss;\
		oss << "expression=\"" << #exp << "\", exception=\"" << #ExceptionT << "\"\n" \
			<< __FILE__ ":" BOOST_STRINGIZE(__LINE__) ", in function: " \
			<< BOOST_CURRENT_FUNCTION; \
		throw ExceptionT(oss.str().c_str()); \
	}
#endif

} // namespace febird

#endif // __febird_stdtypes_h__
