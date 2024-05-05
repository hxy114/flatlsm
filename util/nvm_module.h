//
// Created by hxy on 24-3-29.
//

#ifndef LEVELDB_PM_MODULE_H
#define LEVELDB_PM_MODULE_H
#include <cstdint>
#include <mutex>
#include <libpmem.h>
#include <map>
#include <vector>
#include "port/port.h"


namespace leveldb {
extern const size_t PM_SIZE;

extern const size_t PM_LOG_HEAD_SIZE;//pm log大小
extern const size_t PM_LOG_SIZE;
extern const size_t PERSIST_SIZE;//非强制刷写大小

extern const size_t PM_LOG_NUMBER;//TODO
extern const char * PM_FILE_NAME;

extern const uint32_t PM_LOG_MAGIC;
extern const uint32_t INVALID;



extern const bool IS_FLUSH;

extern const uint64_t L0_THREAD_NUMBER;

extern const std::string MIN_KEY;
extern const std::string MAX_KEY;
typedef  struct PmLogHead{//size=24B
  uint32_t magic_number;
  uint64_t file_size;
  uint64_t used_size;
}PmLogHead;
class NvmManager {
 public:
  NvmManager (bool is_recover_);
  ~NvmManager();

  PmLogHead * get_pm_log();

  void free_pm_log(PmLogHead * pm_log);

  char *get_base();
  size_t get_free_pm_log_number();
  size_t get_free_meta_node_number();
  uint64_t  L0_wait_;
  uint64_t  L0_stop_;
 private:
  size_t pm_size_;//pm大小

  port::Mutex mutex_;
  char* base_;
  char *pm_log_base_;

  std::vector<PmLogHead *>free_pm_log_list_;


};

void reset(PmLogHead *pm_log_head);
void set(PmLogHead *pm_log_head);

extern NvmManager *nvmManager;
}
#endif  // LEVELDB_PM_MODULE_H
