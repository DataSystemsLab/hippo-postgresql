/* vim: set tabstop=4 : */
#ifndef __febird_pass_by_value_h__
#define __febird_pass_by_value_h__

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
#endif

namespace febird {

//! �� T ��һ�����������ʱ��ʹ���������ת������
//! 
//!	input >> t ʵ�ʵ��õ��� void DataIO_loadObject(Input& input, T t)
//! ���� pass_by_value �� T ���Ǵ�ֵ���õ�
//!
//! T �а���һ����ʵ�����ã����統 T �� load_as_var_int_proxy<IntT> ʱ
//! �������Ͳ���Ҫ��ÿ������ load_as_var_int_proxy<IntT> �� Class ��д�� DataInput �ӿ���
//! �Ӷ� DataInput �ӿ�ֻ��Ҫһ�� pass_by_value
//!
//! ��ˣ�ʵ������ʹ���������м��һ���� load_as_var_int_proxy<IntT>����������ʵ�� proxy
//! ��һ������ pass_by_value �ˣ�ֻ�������� DataInput �ӿڣ�
//! ��Ϊ��Ϊ T& ���ܰ󶨵���ʱ����
template<class T> class pass_by_value
{
public:
	T val;

    typedef T type;

	pass_by_value(const T& val) : val(val) {}

	T& operator=(const T& y) { val = y; return val; }

	operator T&() { return val; }

	T& get() { return val; }
};

}

#endif // __febird_pass_by_value_h__

