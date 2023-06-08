#ifndef MINISQL_ROW_H
#define MINISQL_ROW_H

#include <memory>
#include <vector>
#include "common/macros.h"
#include "common/rowid.h"
#include "record/field.h"
#include "record/schema.h"
#include "utils/mem_heap.h"

/**
 *  Row format:
 * -------------------------------------------
 * | Header | Field-1 | ... | Field-N |
 * -------------------------------------------
 *  Header format:
 * --------------------------------------------
 * | Field Nums | Null bitmap |
 * -------------------------------------------
 *  
 *
 */
class Row {
public:
  /**
   * Row used for insert
   * Field integrity should check by upper level
   */
  explicit Row(std::vector<Field> &fields) : heap_(new SimpleMemHeap) {
    // deep copy
    for (auto &field : fields) {
      void *buf = heap_->Allocate(sizeof(Field));
      fields_.push_back(new(buf)Field(field));
    }
  }

  /**
   * Row used for deserialize
   */
  Row() = delete;

  /**
   * Row used for deserialize and update
   */
  Row(RowId rid) : rid_(rid), heap_(new SimpleMemHeap) {}

  /**
   * Row copy function
   */
  Row(const Row &other) : heap_(new SimpleMemHeap) {
    if (!fields_.empty()) {
      for (auto &field : fields_) {
        heap_->Free(field);
      }
      fields_.clear();
    }
    rid_ = other.rid_;
    for (auto &field : other.fields_) {
      void *buf = heap_->Allocate(sizeof(Field));
      fields_.push_back(new(buf)Field(*field));
    }
  }

  virtual ~Row() {
    delete heap_;
  }

  /**
   * Note: Make sure that bytes write to buf is equal to GetSerializedSize()
   */
  uint32_t SerializeTo(char *buf, Schema *schema) const;

  uint32_t DeserializeFrom(char *buf, Schema *schema);

  /**
   * For empty row, return 0
   * For non-empty row with null fields, eg: |null|null|null|, return header size only
   * @return
   */
  uint32_t GetSerializedSize(Schema *schema) const;

  inline const RowId GetRowId() const { return rid_; }

  inline void SetRowId(RowId rid) { rid_ = rid; }

  inline std::vector<Field *> &GetFields() { return fields_; }

  inline Field *GetField(uint32_t idx) const {
    ASSERT(idx < fields_.size(), "Failed to access field");
    return fields_[idx];
  }

  inline size_t GetFieldCount() const { return fields_.size(); }



private:
  Row &operator=(const Row &other) = delete;

private:
  RowId rid_{};
  std::vector<Field *> fields_;   /** Make sure that all fields are created by mem heap */
  MemHeap *heap_{nullptr};
};

class null_bitmap
{
 public:
  null_bitmap(int num)//初始化，根据传入的数量决定要开辟的数组的大小
  {

    _v.resize((num >> 3) + 1); // 相当于num/32 + 1
  }

  void Set(int num) //set 1
  {
    size_t index = num >> 3; // 相当于num/32
    size_t pos = num % 8;
    _v[index] |= (1 << pos);
  }

  void ReSet(int num) //set 0
  {
    size_t index = num >> 3; // 相当于num/32
    size_t pos = num % 8;
    _v[index] &= ~(1 << pos);
  }

  bool check_0(int num){//用以判断第num位是否为0
 //   int flag = 0;
    size_t index = num >> 3; // 相当于num/32
    size_t pos = num % 8;
    if((_v[index] & (1<<pos))==0){
      return true;
    }
    else{
      return false;
    }
  }

  std::vector<char> getVector(){
    return this->_v;
  }

  void setVector(std::vector<char> other){
    this->_v = other;
  }
  //  bool HasExisted(int num)//check whether it exists
  //  {
  //    size_t index = num >> 5;
  //    size_t pos = num % 32;
  //    bool flag = false;
  //    if (_v[index] & (1 << pos))
  //      flag = true;
  //    return flag;
  //
  //  }

// private:
  std::vector<char> _v;
};
#endif //MINISQL_TUPLE_H
