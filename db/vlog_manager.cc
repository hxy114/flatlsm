
#include "db/vlog_manager.h"



#include "util/coding.h"

#include "filename.h"

namespace leveldb {
namespace vlog {



VlogManager::~VlogManager() {
  for (auto& it : manager_) {
    /*if (it.first == cur_vlog_) {
      it.second->vlog_write_->dest_->Append(
          Slice(it.second->buffer_, it.second->size_));
    }*/
    delete it.second->vlog_fetch_;
    delete it.second->vlog_write_->dest_;
    delete it.second->vlog_write_;
    delete it.second;
  }
}

VWriter* VlogManager::AddVlog(const std::string& dbname, const Options& options,
                          uint64_t vlog_numb) {
  /*VlogInfo* old = manager_[vlog_numb];
  if (old != nullptr) {
    old->vlog_write_->dest_->Append(Slice(old->buffer_, old->size_));
  }*/

  VlogInfo* v = new VlogInfo;
  v->vlog_write_ = new VWriter;
  v->vlog_write_->size_=0;
  Status s = options.env->NewAppendableFile(LogFileName(dbname, vlog_numb),
                                            &v->vlog_write_->dest_);
  assert(s.ok());
  // VlogFetcher must initialize after WritableFile is created;
  v->vlog_fetch_ = new VlogFetcher(dbname, options, vlog_numb);
  v->vlog_write_->my_info_ = v;
  v->vlog_fetch_->my_info_ = v;
  v->count_ = 0;
  mutex.Lock();
  manager_[vlog_numb] = v;
  mutex.Unlock();
  //cur_vlog_ = vlog_numb;
  return v->vlog_write_;
}

//void VlogManager::SetCurrentVlog(uint64_t vlog_numb) { cur_vlog_ = vlog_numb; }

Status VlogManager::FetchValueFromVlog(Slice addr, std::string* value) {
  Status s;
  uint64_t file_numb, offset, size;
  // address is <vlog_number, vlog_offset, size>
  if (!GetVarint64(&addr, &file_numb))
    return Status::Corruption("parse size false in RealValue");
  if (!GetVarint64(&addr, &offset))
    return Status::Corruption("parse file_numb false in RealValue");
  if (!GetVarint64(&addr, &size))
    return Status::Corruption("parse pos false in RealValue");

  std::map<uint64_t, VlogInfo*>::const_iterator iter = manager_.find(file_numb);
  if (iter == manager_.end() || iter->second->vlog_fetch_ == nullptr) {
    s = Status::Corruption("can not find vlog");
  } else {
    VlogFetcher* cache = iter->second->vlog_fetch_;
    s = cache->Get(offset, size, value);
  }

  return s;
}
Status VlogManager::AddRecord(uint64_t file_numb,const Slice& slice) {
  std::map<uint64_t, VlogInfo*>::const_iterator iter = manager_.find(file_numb);
  assert(iter != manager_.end());
  assert(iter->second != nullptr);
  return iter->second->vlog_write_->AddRecord(slice)!=UINT64_MAX ? Status::OK():Status::IOError("append error");
}
Status VlogManager::Sync(uint64_t file_numb) {
  std::map<uint64_t, VlogInfo*>::const_iterator iter = manager_.find(file_numb);
  assert(iter != manager_.end());
  assert(iter->second != nullptr);
  return iter->second->vlog_write_->dest_->Sync();
}

/*Status VlogManager::SetHead(size_t offset) {
  std::map<uint64_t, VlogInfo*>::const_iterator iter = manager_.find(cur_vlog_);
  if (iter == manager_.end() || iter->second->vlog_fetch_ == nullptr) {
    return Status::Corruption("can not find vlog");
  } else {
    iter->second->head_ = offset;
    return Status::OK();
  }
}*/

}  // namespace vlog
}  // namespace leveldb
