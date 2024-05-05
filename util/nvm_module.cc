//
// Created by hxy on 24-3-29.
//
#include "nvm_module.h"
namespace leveldb {
NvmManager *nvmManager= nullptr;


const size_t PM_SIZE=12*1024*1024*1024UL;
//const size_t PM_SIZE=14*1024*1024*1024UL;

const size_t PM_LOG_HEAD_SIZE=24;//pm log大小
const size_t PM_LOG_SIZE=2*1024*1024*1024UL;
const size_t PERSIST_SIZE=256;//非强制刷写大小

const size_t PM_LOG_NUMBER=6;//TODO
const char * PM_FILE_NAME="/mnt/pmemdir/pm_log";
const uint32_t PM_LOG_MAGIC=0x0101;
const uint32_t INVALID=0x00;


const bool IS_FLUSH=true;

const uint64_t L0_THREAD_NUMBER=5;

const std::string MIN_KEY="";
const std::string MAX_KEY="\xFF\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff";

PmLogHead * NvmManager::get_pm_log() {
  mutex_.Lock();
  PmLogHead *pm_log= nullptr;
  if(!free_pm_log_list_.empty()){
    pm_log=free_pm_log_list_.back();
    free_pm_log_list_.pop_back();
  }
  mutex_.Unlock();
  return pm_log;

}

void NvmManager::free_pm_log(PmLogHead* pm_log) {
  mutex_.Lock();
  free_pm_log_list_.emplace_back(pm_log);
  mutex_.Unlock();
}


NvmManager::NvmManager (bool is_recover_){
  size_t map_len;
  int is_pmem;
  if((base_= (char *)pmem_map_file(PM_FILE_NAME,PM_SIZE,PMEM_FILE_CREATE,0666,&map_len,&is_pmem))==NULL){
    perror("pmem_map_file");
    exit(1);
  }
  if(map_len!=PM_SIZE){
    perror("memory size not enough");
    pmem_unmap(base_,pm_size_);
    exit(1);
  }
  pm_log_base_=base_ ;
  if(is_recover_){
    //TODO recover
    /*for(int i=0;i<PM_META_NODE_NUMBER;i++){
      char *meta_node=base_+i*PM_META_NODE_SIZE;
      if(strncmp(meta_node,META_NODE_MAGIC,4)==0){

      }
      pmem_memcpy_persist(meta_node,"0000",4);
      free_meta_node_list_.emplace_back(meta_node);
    }*/


  }else{

    for(int i=0;i<PM_LOG_NUMBER;i++){
      PmLogHead *pm_log_head=(PmLogHead*)(pm_log_base_+i*PM_LOG_SIZE);
      reset(pm_log_head);
      free_pm_log_list_.emplace_back(pm_log_head);
    }




  }
}
NvmManager::~NvmManager(){
  pmem_drain();
  pmem_unmap(base_,PM_SIZE);
}

char * NvmManager::get_base() {
  return base_;
}
size_t NvmManager::get_free_pm_log_number(){
  return free_pm_log_list_.size();
}

void reset(PmLogHead *pm_log_head){
  pmem_memset_persist(pm_log_head,0,sizeof(PmLogHead));
}

}