#include "page/bitmap_page.h"
// #include <iostream>
// using namespace std;

template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {  //将空闲的offset的数字返回回去
  //  int flag = 0;//作为是否找到空闲块的标志
  if (page_allocated_ < this->GetMaxSupportedSize()) {  //如果还没有满,说明肯定可以进行分配
    page_offset = this->next_free_page_;                //将目前还存在的下一个空闲页分配给offset
    this->page_allocated_++;                            //已分配的页数加一
    SetPageStatus(page_offset, 1);
    //接下来还得对 free page进行维护
    if (IsPageFree(next_free_page_ + 1) && next_free_page_ + 1 < this->GetMaxSupportedSize()) {  //如果下一块页是空闲的
      this->next_free_page_ += 1;
    } else {  //下一块不是空闲的，出现这种情况说明已经到头了，那么不得不检查之前的页中有没有被释放的
      for (size_t i = 0; i < this->GetMaxSupportedSize(); i++) {
        if (IsPageFree(i)) {
          this->next_free_page_ = i;
          break;
        }
      }
    }
    return true;
  } else {
    return false;
  }
}
 
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  if (!IsPageFree(page_offset)) {  //说明这一块确实不是空的
    SetPageStatus(page_offset, 0);
    this->page_allocated_--;
    for (uint32_t i = 0; i < GetMaxSupportedSize(); ++i) {//更新next_free_page
      if (IsPageFree(i)) next_free_page_ = i;
    }
    return true;
  } else
    return false;
  // return false;
}
 
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {  //如果是free的，返回1
  if (IsPageFreeLow(page_offset / 8, page_offset % 8))
    return true;
  else
    return false;
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  if (byte_index < MAX_CHARS) {  //保证在范围内
    unsigned char a = this->bytes[byte_index];
    int t = a & (1 <<(bit_index));  //获得指定的bit位
    if (t == 0)
      return true;
    else
      return false;
  } else
    return false;
}

template <size_t PageSize>
bool BitmapPage<PageSize>::SetPageStatus(uint32_t page_offset, int flag) {  //将指定的offset进行设置
  uint32_t byte_index1 = page_offset / 8;
  uint32_t bit_index1 = page_offset % 8;
  if (flag == 1) {  //设置为1
    unsigned char a = 1 << bit_index1;
    if (IsPageFree(page_offset)) {  //原来为空
      this->bytes[byte_index1] = this->bytes[byte_index1] | a;
      return true;
    } else
      return false;
  } else if (flag == 0) {
    unsigned char a = ~(1 << bit_index1);  //只有指定位为0
    if (!IsPageFree(page_offset)) {        //原来非空,设置为空
      this->bytes[byte_index1] &= a;
      return true;
    } else
      return false;
  } else {
    return false;
  }
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;