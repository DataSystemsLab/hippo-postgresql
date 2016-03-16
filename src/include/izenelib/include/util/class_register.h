//
// Defines several helper macros for registering class by a string name and
// creating them later per the registered name.
// The motivation is to help implement the factory class. C++ doesn't support
// reflection so we defines several macros to do this.
//
// All macros defined here are NOT used by final user directly, they are used
// to create register macros for a specific base class. Here is an example:
/*
   mapper.h (the interface definition):
   #include "class_register.h"
   class Mapper {
   };

   CLASS_REGISTER_DEFINE_REGISTRY(mapper_register, Mapper);

   #define REGISTER_MAPPER(mapper_name) \
       CLASS_REGISTER_OBJECT_CREATOR( \
           mapper_register, Mapper, #mapper_name, mapper_name) \

   #define CREATE_MAPPER(mapper_name_as_string) \
       CLASS_REGISTER_CREATE_OBJECT(mapper_register, mapper_name_as_string)

   hello_mapper.cc (an implementation of Mapper):
   #include "mapper.h"
   class HelloMapper : public Mapper {
   };
   REGISTER_MAPPER(HelloMapper);

   mapper_user.cc (the final user of all registered mappers):
   #include "mapper.h"
   Mapper* mapper = CREATE_MAPPER("HelloMapper");
*/
// Another usage is to register by class by an arbitrary string instead of its
// class name, and register a default class when no registerd name is matched.
// Here is an example:
/*
   file_impl.h (the interface definition):
   class FileImpl {
   };

   CLASS_REGISTER_DEFINE_REGISTRY(file_impl_register, FileImpl);

   #define REGISTER_DEFAULT_FILE_IMPL(file_impl_name) \
       CLASS_REGISTER_DEFAULT_OBJECT_CREATOR( \
           file_impl_register, FileImpl, file_impl_name)

   #define REGISTER_FILE_IMPL(path_prefix_as_string, file_impl_name) \
       CLASS_REGISTER_OBJECT_CREATOR( \
           file_impl_register, FileImpl, path_prefix_as_string, file_impl_name)

   #define CREATE_FILE_IMPL(path_prefix_as_string) \
       CLASS_REGISTER_CREATE_OBJECT(file_impl_register, path_prefix_as_string)

   local_file.cc (an implementation of FileImpl):
   #include "file.h"
   class LocalFileImpl : public FileImpl {
   };
   REGISTER_DEFAULT_FILE_IMPL(LocalFileImpl);
   REGISTER_FILE_IMPL("/local", LocalFileImpl);

   file_user.cc (the final user of all registered file implementations):
   #include "file_impl.h"
   FileImpl* file_impl = CREATE_FILE_IMPL("/local");
*/

#ifndef UTIL_CLASS_REGISTER_H_
#define UTIL_CLASS_REGISTER_H_

#include <map>
#include <string>

// The first parameter, register_name, should be unique globally.
// Another approach for this is to define a template for base class. It would
// make the code more readable, but the only issue of using template is that
// each base class could have only one register then. It doesn't sound very
// likely that a user wants to have multiple registers for one base class,
// but we keep it as a possibility.
// We would switch to using template class if necessary.
#define CLASS_REGISTER_DEFINE_REGISTRY(register_name, base_class_name)  \
  class ObjectCreatorRegistry_##register_name {                         \
   public:                                                              \
   typedef base_class_name* (*Creator)();                               \
                                                                        \
   ObjectCreatorRegistry_##register_name()                              \
   : default_creator_(NULL) {}                                         \
   ~ObjectCreatorRegistry_##register_name() {}                          \
                                                                        \
   void SetDefaultCreator(Creator creator) {                            \
     default_creator_ = creator;                                       \
   }                                                                    \
                                                                        \
   void AddCreator(std::string entry_name, Creator creator) {           \
     creator_registry_[entry_name] = creator;                          \
   }                                                                    \
                                                                        \
   base_class_name* CreateObject(const std::string& entry_name);        \
                                                                        \
   private:                                                             \
   typedef std::map<std::string, Creator> CreatorRegistry;              \
   Creator default_creator_;                                           \
   CreatorRegistry creator_registry_;                                  \
  };                                                                    \
                                                                        \
  inline ObjectCreatorRegistry_##register_name&                         \
  GetRegistry_##register_name() {                                       \
    static ObjectCreatorRegistry_##register_name registry;              \
    return registry;                                                    \
  }                                                                     \
                                                                        \
  class DefaultObjectCreatorRegister_##register_name {                  \
   public:                                                              \
   DefaultObjectCreatorRegister_##register_name(                        \
       ObjectCreatorRegistry_##register_name::Creator creator) {        \
     GetRegistry_##register_name().SetDefaultCreator(creator);          \
   }                                                                    \
   ~DefaultObjectCreatorRegister_##register_name() {}                   \
  };                                                                    \
                                                                        \
  class ObjectCreatorRegister_##register_name {                         \
   public:                                                              \
   ObjectCreatorRegister_##register_name(                               \
       const std::string& entry_name,                                   \
       ObjectCreatorRegistry_##register_name::Creator creator) {        \
     GetRegistry_##register_name().AddCreator(entry_name,               \
                                              creator);                 \
   }                                                                    \
   ~ObjectCreatorRegister_##register_name() {}                          \
  }

#define CLASS_REGISTER_IMPLEMENT_REGISTRY(register_name, base_class_name) \
  base_class_name* ObjectCreatorRegistry_##register_name::CreateObject( \
      const std::string& entry_name) {                                  \
    Creator creator = default_creator_;                                \
    CreatorRegistry::const_iterator it =                                \
        creator_registry_.find(entry_name);                            \
    if (it != creator_registry_.end()) {                               \
      creator = it->second;                                             \
    }                                                                   \
                                                                        \
    if (creator != NULL) {                                              \
      return (*creator)();                                              \
     } else {                                                           \
      return NULL;                                                      \
    }                                                                   \
  }

#define CLASS_REGISTER_DEFAULT_OBJECT_CREATOR(register_name,            \
                                              base_class_name,          \
                                              class_name)               \
  base_class_name* DefaultObjectCreator_##register_name##class_name() { \
    return new class_name;                                              \
  }                                                                     \
  DefaultObjectCreatorRegister_##register_name                          \
  g_default_object_creator_register_##register_name##class_name(        \
      DefaultObjectCreator_##register_name##class_name)

#define CLASS_REGISTER_OBJECT_CREATOR(register_name,                    \
                                      base_class_name,                  \
                                      entry_name_as_string,             \
                                      class_name)                       \
  base_class_name* ObjectCreator_##register_name##class_name() {        \
    return new class_name;                                              \
  }                                                                     \
  ObjectCreatorRegister_##register_name                                 \
  g_object_creator_register_##register_name##class_name(                \
      entry_name_as_string,                                             \
      ObjectCreator_##register_name##class_name)

#define CLASS_REGISTER_CREATE_OBJECT(register_name, entry_name_as_string) \
  GetRegistry_##register_name().CreateObject(entry_name_as_string)

#endif  // UTIL_CLASS_REGISTER_H_
