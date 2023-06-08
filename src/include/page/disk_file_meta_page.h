#ifndef MINISQL_DISK_FILE_META_PAGE_H
#define MINISQL_DISK_FILE_META_PAGE_H

#include <cstdint>

#include "page/bitmap_page.h"
  
static constexpr page_id_t MAX_VALID_PAGE_ID = (PAGE_SIZE - 8) / 4 * BitmapPage<PAGE_SIZE>::GetMaxSupportedSize();//最多管理 (PAGE_SIZE - 8) / 4 个bitmap

class DiskFileMetaPage {
public:
  uint32_t GetExtentNums() {
    return num_extents_;//返回分区数目
  }

  uint32_t GetAllocatedPages() {
    return num_allocated_pages_;//返回已经分配的页数
  }

  uint32_t GetExtentUsedPage(uint32_t extent_id) {
    if (extent_id >= num_extents_) {//如果要取的分区页号大于现有区号，返回0
      return 0;
    }
    return extent_used_page_[extent_id];//正常返回当前分区用了多少页
  }

public:
  uint32_t num_allocated_pages_{0};
  uint32_t num_extents_{0};   // each extent consists with a bit map and BIT_MAP_SIZE pages;所有分区
  uint32_t extent_used_page_[0];//占用四个字节，最多有(PAGE_SIZE - 8) / 4个，即最多1022个分区
};

#endif //MINISQL_DISK_FILE_META_PAGE_H
