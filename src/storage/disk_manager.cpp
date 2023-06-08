#include <stdexcept>
#include <sys/stat.h>

#include "glog/logging.h"
#include "page/bitmap_page.h"
#include "storage/disk_manager.h"

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);//读入meta_data_
}

void DiskManager::Close() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

page_id_t DiskManager::AllocatePage() {//扫描区块，看看哪个有空闲页，然后转化为逻辑页号，返回
  DiskFileMetaPage *metapage=reinterpret_cast<DiskFileMetaPage *>(meta_data_);
  u_int32_t extents=metapage->GetExtentNums();//所有分区数目
  u_int32_t i,page_off;
  if(extents==0){
    metapage->num_extents_++;
    extents++;
  }
  for(i=0; i<extents; ++i){//扫描所有分区，如果没有就新建分区
    if(metapage->GetExtentUsedPage(i) < BITMAP_SIZE){//进入到bitmap中，知道扫描到有空闲页的再分配
      break;
    }
  }
  // 没有合适的区块，新加区块
  if(metapage->GetExtentUsedPage(i-1)==BITMAP_SIZE && i==extents){
    extents++;
    metapage->num_extents_++;
  }
  char bitmapdata[PAGE_SIZE];//读取bitmap的数据,之后转为bitmap对象
  ReadPhysicalPage(i*(BITMAP_SIZE + 1)+1,bitmapdata);
  BitmapPage<PAGE_SIZE> *bitmap=reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmapdata);
  page_off=0;//数据页的偏移量
  bitmap->AllocatePage(page_off);
  metapage->num_allocated_pages_++;
  metapage->extent_used_page_[i]++;
  WritePhysicalPage((BITMAP_SIZE+1)*i+1, bitmapdata);
  return page_off+BITMAP_SIZE*i;//logic page id
}

void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
  u_int32_t extent=logical_page_id/BITMAP_SIZE;
  u_int32_t page_off=logical_page_id%BITMAP_SIZE;
  DiskFileMetaPage *metapage=reinterpret_cast<DiskFileMetaPage *>(meta_data_);
  char bitmapdata[PAGE_SIZE];//读取bitmap的数据,之后转为bitmap对象
  ReadPhysicalPage(extent*(BITMAP_SIZE+1)+1,bitmapdata);
  BitmapPage<PAGE_SIZE> *bitmap=reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmapdata);
  bitmap->DeAllocatePage(page_off);
  // char *newdata=reinterpret_cast<char *>(bitmap);
  metapage->num_allocated_pages_--;
  metapage->extent_used_page_[extent]--;
  if(metapage->GetExtentUsedPage(extent) == 0)
    metapage->num_extents_--;
  WritePhysicalPage(extent*(BITMAP_SIZE+1)+1, bitmapdata);//logic 转 physical
}

bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  u_int32_t extent=logical_page_id/BITMAP_SIZE;
  u_int32_t page_off=logical_page_id%BITMAP_SIZE;
  char bitmapdata[PAGE_SIZE];//读取bitmap的数据,之后转为bitmap对象
  ReadPhysicalPage(extent*(BITMAP_SIZE +1)+1,bitmapdata);
  BitmapPage<PAGE_SIZE> *bitmap=reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmapdata);
  return bitmap->IsPageFree(page_off);
}

page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
  u_int32_t extent=logical_page_id/BITMAP_SIZE;
  u_int32_t page_off=logical_page_id%BITMAP_SIZE;
  return page_off+(BITMAP_SIZE+1)*extent+2;
}

int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page" << std::endl;
#endif
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}