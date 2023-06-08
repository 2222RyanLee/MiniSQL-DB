#include "buffer/buffer_pool_manager.h"
#include "glog/logging.h"
#include "page/bitmap_page.h"
 
BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
        : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page: page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  if (page_id == INVALID_PAGE_ID) return nullptr; //page_id有效
  if (page_table_.find(page_id) != page_table_.end()){    //如果page在table中
    frame_id_t frame_id = page_table_.find(page_id)->second; 
    Page * page = &pages_[frame_id];
    page->pin_count_++;  //count自增
    replacer_->Pin(frame_id);  //在replacer中固定frame_id
    return page;
  }

  else {     //如果page不在table当中
    frame_id_t frame_id = -1;
    if (!free_list_.empty()){ //如果空闲页未满
      frame_id = free_list_.front(); //取第一个空闲页
      free_list_.pop_front();   //pop掉空闲页
      Page * page = &pages_[frame_id];
      disk_manager_->ReadPage(page_id, page->data_);  //从磁盘中读page
      page->pin_count_ = 1;  //count置1
      replacer_->Pin(frame_id); //在replacer中固定frame_id
      return page;
    }

    else {   //空闲页已满
      if (replacer_->Victim(&frame_id)){  //存在victim page并返回frame_id
        Page * page = &pages_[frame_id];  
        assert(page->pin_count_ == 0);
        if (page->is_dirty_){          // 为脏页，写回磁盘
          disk_manager_->WritePage(page->page_id_, page->GetData());
          page->is_dirty_ = false;
        }
        page_table_.erase(page->page_id_);   //从table中删除page的id
        if (page_id != INVALID_PAGE_ID) {      //写入page_id
          page_table_.emplace(page_id, frame_id);  
        }
        //page->ResetMemory();
        page->page_id_ = page_id;
        disk_manager_->ReadPage(page_id, page->data_);  //磁盘中读数据
        replacer_->Pin(frame_id);
        page->pin_count_ = 1;    //count置1
        return page;
      }
      else return nullptr;
    }
    
  }
}

Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  frame_id_t frame_id ;   //初始化
  bool can_allocate = 1;      //
  if (!free_list_.empty()){   //空闲页未满
      frame_id = free_list_.front();
      free_list_.pop_front();}
  else if (replacer_->Victim(&frame_id)){}    
  else can_allocate = 0;     //没有页可以替换，不能新增页
  if (can_allocate == 0) return nullptr;  
  else { 
    page_id = AllocatePage();     //新增页
    Page *page = &pages_[frame_id];
    //if (page->pin_count_ != 0) return nullptr;
        if (page->is_dirty_){                    //若为脏页则写回磁盘
          disk_manager_->WritePage(page->page_id_, page->GetData());
          page->is_dirty_ = false;
        }
        page_table_.erase(page->page_id_);        
        if (page_id != INVALID_PAGE_ID) {              
    page_table_.emplace(page_id, frame_id);  
  }
        //page->ResetMemory();
        page->page_id_ = page_id;
    replacer_->Pin(frame_id);
    page->pin_count_ = 1;   //count 置1
    return page;
    
  }
  
}

bool BufferPoolManager::DeletePage(page_id_t page_id) {
  if (page_id == INVALID_PAGE_ID) return false; //id有效  
  if (page_table_.find(page_id) != page_table_.end()){   
    frame_id_t frame_id = page_table_.find(page_id)->second;   //table中有此page_id
    Page * page = &pages_[frame_id];
    if (page->pin_count_ != 0) return false;    //pin_count未归0无法删除
    else {  //否则就可以删除
      if (page->is_dirty_){  //若为脏页则写回
          disk_manager_->WritePage(page->page_id_, page->GetData());
          page->is_dirty_ = false;
        }
      DeallocatePage(page_id);
      page_table_.erase(page->page_id_);
      page->ResetMemory();
      page->page_id_ = INVALID_PAGE_ID;
      free_list_.push_back(frame_id);
      return true;
    }
    
  }
  // 1.   If P does not exist, return true.
  else return true;
}

bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  if (page_id == INVALID_PAGE_ID) return false;  //id有效
  if (page_table_.find(page_id) != page_table_.end()){       //如果在table中则可以unpin
    frame_id_t frame_id = page_table_.find(page_id)->second;
    Page * page = &pages_[frame_id];
    if (is_dirty) page->is_dirty_ = true;
    if (page->pin_count_ > 1){      //代表此页仍被使用，无法unpin
    page->pin_count_--;
    return false;
    }
    else if (page->pin_count_ == 1){   //此页可以被unpin
      page->pin_count_ = 0;
      replacer_->Unpin(frame_id);
      return true;
    }
    else return false;
  }
  else return false;
}

bool BufferPoolManager::FlushPage(page_id_t page_id) {
  if (page_id == INVALID_PAGE_ID) return false;  //确保id有效
  if (page_table_.find(page_id) != page_table_.end()){   
    frame_id_t frame_id = page_table_.find(page_id)->second;
    Page * page = &pages_[frame_id];
    disk_manager_->WritePage(page->page_id_, page->GetData());    //不管是否为dirty，均强制写回
    page->is_dirty_ = false;
    return true;
  }
  else return false;
}

bool BufferPoolManager::FlushAllPages(){
  auto pg=page_table_.begin();
  for(;pg!=page_table_.end();pg++){
    frame_id_t frame_id = pg->second;
    Page * page = &pages_[frame_id];
    disk_manager_->WritePage(page->page_id_, page->GetData());    //不管是否为dirty，均强制写回
    page->is_dirty_ = false;
  }
  return true;
}

page_id_t BufferPoolManager::AllocatePage() {
  page_id_t p = disk_manager_->AllocatePage();
  //page_id_t p = disk_manager_->MapPageId(next_page_id);
  return p;
}

//page_id_t BufferPoolManager::MapPageid(page_id_t logical_page_id) {
//  page_id_t p = disk_manager_->MapPageId(logical_page_id);
//  return p;
//}

void BufferPoolManager::DeallocatePage(page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id);
}

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}