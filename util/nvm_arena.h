//
// Created by hxy on 24-3-29.
//

#ifndef LEVELDB_NVM_ARENA_H
#define LEVELDB_NVM_ARENA_H

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <vector>
#include <libpmem.h>
#include "nvm_module.h"
namespace  leveldb{

class NvmArena {
 public:
  NvmArena(PmLogHead *pm_log_head,bool force=false);

  NvmArena(const NvmArena&) = delete;
  NvmArena& operator=(const NvmArena&) = delete;

  ~NvmArena();

  // Return a pointer to a newly allocated memory block of "bytes" bytes.
  char* Allocate(size_t bytes);

  // Allocate memory with the normal alignment guarantees provided by malloc.
  char* AllocateAligned(size_t bytes);

  // Returns an estimate of the total memory usage of data allocated
  // by the arena.
  size_t MemoryUsage() const {
    return memory_usage_;
  }
  void Persist();
  void PersistKV();
  void PersistHead();

 private:

  // Allocation state
  PmLogHead *pm_log_start_;//start
  bool force_;//是否每次强制压缩
  size_t memory_usage_;//使用量
  char *kv_start_;//kv开始存储的位置
  char *kv_alloc_ptr_;//当前分配的地址
  char *last_persist_point_;//上一次持久化的地址


  // Total memory usage of the arena.
  //
  // TODO(costan): This member is accessed via atomics, but the others are
  //               accessed without any locking. Is this OK?

};



}
#endif  // LEVELDB_NVM_ARENA_H
