/* vim: set tabstop=4 : */
#ifndef __febird_io_DataIO_Version_h__
#define __febird_io_DataIO_Version_h__

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
#endif

#include "../stdtypes.h"
//#include "var_int.h"
#include <boost/serialization/strong_typedef.hpp>
#include "../pass_by_value.h"

namespace febird {

BOOST_STRONG_TYPEDEF(uint32_t, serialize_version_t)

//! ������ DATA_IO_VERSIONED_LOAD_SAVE ����ʹ��
//!
//! ��������ֻ���л����� version �Ķ����Ա
//! @see DataIO_version_manager::since
//!
template<class Class>
class DataIO_since_version_proxy
{
	unsigned int m_min_owner_version;
	unsigned int m_version;
	Class& m_object;

public:
	DataIO_since_version_proxy(unsigned int min_owner_version, unsigned int loaded_owner_version, Class& object)
		: m_min_owner_version(min_owner_version)
		, m_version(loaded_owner_version)
		, m_object(object)
	{
	}
	template<class Input> friend void
		DataIO_loadObject(Input& input, DataIO_since_version_proxy x)
	{
		if (x.m_version >= x.m_min_owner_version)
		{
			input >> x.m_object;
		}
	}
	template<class Output> friend void 
		DataIO_saveObject(Output& output, const DataIO_since_version_proxy& x)
	{
		if (x.m_version >= x.m_min_owner_version)
		{
			output << x.m_object;
		}
	}
};

//! ������ DATA_IO_VERSIONED_LOAD_SAVE ����ʹ��
//!
//! ���������ʱ�������� version ��������Ա
//! @see DataIO_version_manager::get_version
//!
class DataIO_copy_version_proxy
{
	uint32_t& m_version;
	uint32_t  m_loadedVersion;
public:
	DataIO_copy_version_proxy(uint32_t& version, uint32_t loadedVersion)
		: m_version(version), m_loadedVersion(loadedVersion) {}

	template<class Input> friend void
		DataIO_loadObject(Input& input, DataIO_copy_version_proxy x)
	{
		x.m_version = x.m_loadedVersion;
	}

	template<class Output> friend void 
		DataIO_saveObject(Output& output, const DataIO_copy_version_proxy& x)
	{
		// ignore
	}
};

//! ������ DATA_IO_REG_LOAD_SAVE_V ����ʹ��
//!
//! �������������е��û���� load(object, version)/save(object, version)
//! 
//! @see DataIO_version_manager::base_object
template<class BaseClassPtr>
class DataIO_base_class_version_load_save
{
	BaseClassPtr m_base;
	unsigned int m_version;

public:
	DataIO_base_class_version_load_save(BaseClassPtr pbase, unsigned int version)
		: m_base(pbase), m_version(version) {}

	template<class Input> friend void
		DataIO_loadObject(Input& input, DataIO_base_class_version_load_save x)
	{
		x.m_base->load(input, x.m_version);
	}
	template<class Output> friend void 
		DataIO_saveObject(Output& output, const DataIO_base_class_version_load_save& x)
	{
		x.m_base->save(output, x.m_version);
	}
};

//! �汾����
//!
//! @see DATA_IO_REG_VERSION_SERIALIZE/DATA_IO_REG_LOAD_SAVE_V
//!
template<class ThisClass> class DataIO_version_manager
{
	unsigned int m_version;

public:
	DataIO_version_manager(unsigned int loaded_owner_version)
		: m_version(loaded_owner_version)
	{
	}

	template<class Class>
	pass_by_value<DataIO_since_version_proxy<Class> >
	since(unsigned int min_owner_version, Class& object)
	{
		return pass_by_value<DataIO_since_version_proxy<Class> >
			(DataIO_since_version_proxy<Class>(min_owner_version, m_version, object));
	}
	template<class Class>
	DataIO_since_version_proxy<const Class>
	since(unsigned int min_owner_version, const Class& object)
	{
		return DataIO_since_version_proxy<const Class>(min_owner_version, m_version, object);
	}

	pass_by_value<DataIO_copy_version_proxy>
	get_version(uint32_t& version)
	{
		return pass_by_value<DataIO_copy_version_proxy>
				(DataIO_copy_version_proxy(version, m_version));
	}

	//! version is const when save()
	DataIO_copy_version_proxy
	get_version(const uint32_t& version)
	{
		static uint32_t v2;
		return DataIO_copy_version_proxy(v2, m_version);
	}

	//! for loading
	template<class BaseClass>
	pass_by_value<DataIO_base_class_version_load_save<BaseClass*> >
	base_object(BaseClass* self)
	{
		return pass_by_value<DataIO_base_class_version_load_save<BaseClass*> >
			(DataIO_base_class_version_load_save<BaseClass*>(self, m_version));
	}

	//! for saving
	template<class BaseClass>
	DataIO_base_class_version_load_save<const BaseClass*>
	base_object(const BaseClass* self)
	{
		return DataIO_base_class_version_load_save<const BaseClass*>(self, m_version);
	}
};

} // namespace febird

#endif // __febird_io_DataIO_Version_h__
