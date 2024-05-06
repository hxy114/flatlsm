//
// Created by hxy on 24-4-2.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include <unistd.h>

#include "db/pmtable.h"
#include "db/dbformat.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "util/coding.h"


namespace leveldb {

static Slice GetLengthPrefixedSlice(const char* data) {
  uint32_t len;
  const char* p = data;
  p = GetVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted
  return Slice(p, len);
}

PmTable::PmTable(const InternalKeyComparator& comparator,PmLogHead* pmLogHead)
    : comparator_(comparator), refs_(0),
      pmLogHead_(pmLogHead),
      nvmArena_(pmLogHead_),
      table_(comparator_, &arena_),
      count_(0)
      {
  assert(pmLogHead_!= nullptr);
  pmLogHead_->magic_number=PM_LOG_MAGIC;
  pmLogHead_->used_size=PM_LOG_HEAD_SIZE;
  pmLogHead_->file_size=PM_LOG_SIZE;
  pmem_persist(&pmLogHead_,PM_LOG_HEAD_SIZE);

  }

void PmTable::FreePmtable(){
  pmLogHead_->magic_number= INVALID;
  //pmem_persist(pmLogHead_,sizeof(pmLogHead_));
}
PmTable::~PmTable() { assert(refs_ == 0);
  pmem_persist(pmLogHead_,sizeof(PmLogHead));
  nvmManager->free_pm_log(pmLogHead_);
}


size_t PmTable::ApproximateMemoryUsage() { return nvmArena_.MemoryUsage(); }

int PmTable::KeyComparator::operator()(const char* aptr,
                                        const char* bptr) const {
  // Internal keys are encoded as length-prefixed strings.
  Slice a = GetLengthPrefixedSlice(aptr);
  Slice b = GetLengthPrefixedSlice(bptr);
  return comparator.Compare(a, b);
}

// Encode a suitable internal key target for "target" and return it.
// Uses *scratch as scratch space, and the returned pointer will point
// into this scratch space.
static const char* EncodeKey(std::string* scratch, const Slice& target) {
  scratch->clear();
  PutVarint32(scratch, target.size());
  scratch->append(target.data(), target.size());
  return scratch->data();
}

class PmTableIterator : public Iterator {
 public:
  explicit PmTableIterator(PmTable::Table* table) : iter_(table) {}

  PmTableIterator(const PmTableIterator&) = delete;
  PmTableIterator& operator=(const PmTableIterator&) = delete;

  ~PmTableIterator() override = default;

  bool Valid() const override { return iter_.Valid(); }
  void Seek(const Slice& k) override { iter_.Seek(EncodeKey(&tmp_, k)); }
  void SeekToFirst() override { iter_.SeekToFirst(); }
  void SeekToLast() override { iter_.SeekToLast(); }
  void Next() override { iter_.Next(); }
  void Prev() override { iter_.Prev(); }
  Slice key() const override { return GetLengthPrefixedSlice(iter_.key()); }
  Slice value()  override {
    Slice key_slice = GetLengthPrefixedSlice(iter_.key());
    return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
  }

  Status status() const override { return Status::OK(); }

 private:
  PmTable::Table::Iterator iter_;
  std::string tmp_;  // For passing to EncodeKey
};

Iterator* PmTable::NewIterator() { return new PmTableIterator(&table_); }
std::string &PmTable::GetMinKey(){
  return min_key_;
}
std::string &PmTable::GetMaxKey(){
  return max_key_;
}
bool PmTable::Add(SequenceNumber s, ValueType type, const Slice& key,
                   const Slice& value) {
  // Format of an entry is concatenation of:
  //  key_size     : varint32 of internal_key.size()
  //  key bytes    : char[internal_key.size()]
  //  tag          : uint64((sequence << 8) | type)
  //  value_size   : varint32 of value.size()
  //  value bytes  : char[value.size()]
  size_t key_size = key.size();
  size_t val_size = value.size();
  size_t internal_key_size = key_size + 8;
  const size_t encoded_len = VarintLength(internal_key_size) +
                             internal_key_size + VarintLength(val_size) +
                             val_size;
  if(encoded_len+nvmArena_.MemoryUsage()<PM_LOG_SIZE){
    char* buf = nvmArena_.Allocate(encoded_len);
    char* p = EncodeVarint32(buf, internal_key_size);
    std::memcpy(p, key.data(), key_size);
    p += key_size;
    EncodeFixed64(p, (s << 8) | type);
    p += 8;
    p = EncodeVarint32(p, val_size);
    std::memcpy(p, value.data(), val_size);
    assert(p + val_size == buf + encoded_len);
    //if(min_key_.size()==0||comparator_(min_key_.c_str(),buf))
    if(min_key_.empty()||comparator_.comparator.user_comparator()->Compare(min_key_,key)>0){
      min_key_=key.ToString();
    }
    if(max_key_.empty()||comparator_.comparator.user_comparator()->Compare(max_key_,key)<0){
      max_key_=key.ToString();
    }
    table_.Insert(buf);
    count_++;
    return true;
  }
  return false;

}

bool PmTable::Get(const LookupKey& key, std::string* value, Status* s) {
  Slice memkey = key.memtable_key();
  Table::Iterator iter(&table_);
  iter.Seek(memkey.data());
  if (iter.Valid()) {
    // entry format is:
    //    klength  varint32
    //    userkey  char[klength]
    //    tag      uint64
    //    vlength  varint32
    //    value    char[vlength]
    // Check that it belongs to same user key.  We do not check the
    // sequence number since the Seek() call above should have skipped
    // all entries with overly large sequence numbers.
    const char* entry = iter.key();
    uint32_t key_length;
    const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
    if (comparator_.comparator.user_comparator()->Compare(
            Slice(key_ptr, key_length - 8), key.user_key()) == 0) {
      // Correct user key
      const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
      switch (static_cast<ValueType>(tag & 0xff)) {
        case kTypeValue: {
          Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
          value->assign(v.data(), v.size());
          return true;
        }
        case kTypeDeletion:
          *s = Status::NotFound(Slice());
          return true;
      }
    }
  }
  return false;
}
PmtableQueue::ListNode::ListNode(PmTable* pmTable) {
  pmTable_=pmTable;
  pre= nullptr;
  next= nullptr;
}
PmtableQueue::PmtableQueue(){
  head_=new ListNode(nullptr);
  head_->next=head_;
  head_->pre=head_;

}
PmtableQueue::~PmtableQueue() {
  delete head_;
}
PmtableQueue::ListNode * PmtableQueue::GetHead(){
  return head_;
}
void PmtableQueue::InsertPmtable(PmTable *pmtable){
  if(mp_.count(pmtable)==0){
    ListNode *node=new ListNode(pmtable),*pre=head_->pre;
    pre->next=node;
    node->pre=pre;
    node->next=head_;
    head_->pre=node;
    mp_[pmtable]=node;

  }

}
void PmtableQueue::RemovePmtable(PmTable *pmTable) {
  if (mp_.count(pmTable) != 0) {
    ListNode *node = mp_[pmTable], *pre = node->pre, *next = node->next;
    pre->next = next;
    next->pre = pre;
    mp_.erase(pmTable);
    delete node;
  }
}
size_t PmtableQueue:: capacity(){
  return mp_.size();
}
}  // namespace leveldb
