#ifndef MINISQL_RID_H
#define MINISQL_RID_H

#include <cstdint>
#include "common/config.h"

/**
 * | page_id(32bit) | slot_num(32bit) |
 */
class RowId {
public:
  RowId() = default;//必须携带参数构造

  RowId(page_id_t page_id, uint32_t slot_num) : page_id_(page_id), slot_num_(slot_num) {}//数据页的逻辑值，当前数据页中的记录号

  explicit RowId(int64_t rid)//强制拆分64位整数
          : page_id_(static_cast<page_id_t>(rid >> 32)),
            slot_num_(static_cast<uint32_t>(rid)) {}

  inline int64_t Get() const {//拼接为64位整数
    return (static_cast<int64_t>(page_id_)) << 32 | slot_num_;
  }

  inline page_id_t GetPageId() const { return page_id_; }//得到页号

  inline uint32_t GetSlotNum() const { return slot_num_; }//得到记录号

  inline void Set(page_id_t page_id, uint32_t slot_num) {//重置页号和记录号
    page_id_ = page_id;
    slot_num_ = slot_num;
  }

  bool operator<(const RowId &other)const{
    if(this->page_id_<other.page_id_ || (this->page_id_==other.page_id_&&this->slot_num_<other.slot_num_)){
      return true;
    }
    else return false;
  }

  bool operator>(const RowId & other)const{
    if(this->page_id_>other.page_id_||(this->page_id_==other.page_id_&&this->slot_num_>other.slot_num_)) return true;
    else return false;
  }

  bool operator>=(const RowId& other)const{
    if(!((*this)<other)) return true;
    else return false;
  }

  bool operator<=(const RowId& other)const{
    if(!((*this)>other)) return true;
    else return false;
  }

  bool operator!=(const RowId& other)const{
    if(!((*this)==other)) return true;
    else return false;
  }
 
  bool operator==(const RowId &other) const {//判断当前记录和other记录是不是一条
    return page_id_ == other.page_id_ && slot_num_ == other.slot_num_;
  }

private:
  page_id_t page_id_{INVALID_PAGE_ID};//初始不指向物理页
  uint32_t slot_num_{0};  // logical offset of the record in page, starts from 0. eg:0, 1, 2...
};

static const RowId INVALID_ROWID = RowId(INVALID_PAGE_ID, 0);//无效物理页和记录号

#endif //MINISQL_RID_H
