//
// msgpack::rpc::exception_impl - MessagePack-RPC for C++
//
// Copyright (C) 2010 FURUHASHI Sadayuki
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//        http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//
#ifndef MSGPACK_RPC_EXCEPTION_IMPL_H__
#define MSGPACK_RPC_EXCEPTION_IMPL_H__

#include "exception.h"
#include "future_impl.h"

namespace msgpack {
namespace rpc {


extern const object TIMEOUT_ERROR;
extern const object CONNECT_ERROR;

void throw_exception(future_impl* f);


}  // namespace rpc
}  // namespace msgpack

#endif /* msgpack/rpc/exception_impl.h */

