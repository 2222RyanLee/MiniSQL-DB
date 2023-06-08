#ifndef MINISQL_BITMAP_PAGE_H
#define MINISQL_BITMAP_PAGE_H

#include <bitset>
 
#include "common/macros.h"
#include "common/config.h"

template<size_t PageSize>
class BitmapPage {
public:
  /**
   * @return The number of pages that the bitmap page can record, i.e. the capacity of an extent.
   */
  static constexpr size_t GetMaxSupportedSize() { return 8 * MAX_CHARS; }

  BitmapPage(){//构造函数
    this->page_allocated_ = 0;//在构造时，已分配的页数为0
    this->next_free_page_ = 0;//下一个空闲的页从0开始
  }

  /**
   * @param page_offset Index in extent of the page allocated.
   * @return true if successfully allocate a page.
   */
  bool AllocatePage(uint32_t &page_offset);//4字节的page_offset 到底是用来干嘛的，我的理解是就是一个单纯的数字，代表着页数

  /**
   * @return true if successfully de-allocate a page.
   */
  bool DeAllocatePage(uint32_t page_offset);

  /**
   * @return whether a page in the extent is free
   */
  bool IsPageFree(uint32_t page_offset) const;// 稍微高层一点的判断某一页是否为空的函数

  bool SetPageStatus(uint32_t page_offset, int flag);//自己设置的，目的是在分配完一个page之后将其bit设定为1

//  bool SetPageFree(uint32_t page_offset);//自己设置的,设置page对应的bit为0
  uint32_t show(){return page_allocated_;};
private:
  /**
   * check a bit(byte_index, bit_index) in bytes is free(value 0).
   *
   * @param byte_index value of page_offset / 8,那就是说page_offset是按照bit为单位的
   * @param bit_index value of page_offset % 8 ,bit_index注定只能是0~7
   * @return true if a bit is 0, false if 1.
   */
  bool IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const; //这个函数将是最基础的函数，用于检测一个块是否是空闲的

  /** Note: need to update if modify page structure. */
  static constexpr size_t MAX_CHARS = PageSize - 2 * sizeof(uint32_t);//###

private:
  /** The space occupied by all members of the class should be equal to the PageSize */
  [[maybe_unused]] uint32_t page_allocated_;
  [[maybe_unused]] uint32_t next_free_page_;
  [[maybe_unused]] unsigned char bytes[MAX_CHARS];//这里面的每一个bit都标记一个page是否被使用
};

#endif //MINISQL_BITMAP_PAGE_H
