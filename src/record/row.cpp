#include "record/row.h"
#include <iostream>

uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  char *buffer = buf;
  bool isNull;
  uint32_t flen, offset, size;

  size = 0;
  flen = this->GetFieldCount();

  memcpy(buffer, &flen, sizeof(uint32_t));
  buffer += sizeof(uint32_t);
  size += sizeof(uint32_t);

  for(uint32_t i = 0; i < flen; i++){
    isNull = ((fields_[i])->IsNull());
    memcpy(buffer, &isNull, sizeof(bool));
    buffer += sizeof(bool);
    size += sizeof(bool);
    //给每个field序列化并返回偏移量
    if(isNull == false){
      offset = fields_[i]->SerializeTo(buffer);
      buffer += offset;
      size += offset;
    }
  }
  return size;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  char *buffer = buf;
  bool isNull;
  uint32_t flen, size, offset;
  
  size = 0;
  //如果为空，直接返回0
  if(buffer == nullptr)
    return 0;
  //读入field num
  flen = MACH_READ_FROM(uint32_t, buffer);
  //判断是否存在错误
  if(flen != schema->GetColumnCount())
    std::cerr << "Field Count has something wrong" << std::endl;
  buffer += sizeof(uint32_t);
  size += sizeof(uint32_t);

  std::vector<Column *> Column = schema->GetColumns();
  //读入isnull和field
  for(uint32_t i = 0; i < flen; i++){
    isNull = MACH_READ_FROM(bool, buffer);
    buffer += sizeof(bool);
    size += sizeof(bool);
    //如果isNull代表为空
    if(isNull == '\1'){
      Field *temp = nullptr;
      void *mem = heap_->Allocate(sizeof(Field));
      temp = new(mem) Field(Column[i]->GetType());
      this->fields_.push_back(temp);
    }
    else{
      Field *temp = nullptr;
      offset = Field::DeserializeFrom(buffer, Column[i]->GetType(), &temp, false, heap_);
      buffer += offset;
      size += offset;
      fields_.push_back(temp);
    }
  }

  return size;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  uint32_t flen, size;
  //初始化
  size = 0;
  flen = this->GetFieldCount();
  //计算flen大小
  size += sizeof(uint32_t);
  //计算isnull大小
  size += this->GetFieldCount();
  //计算field的大小
  for(uint32_t i = 0; i < flen; i++){
    if(fields_[i]->IsNull() == false)
      size += fields_[i]->GetSerializedSize();
  }
  
  return size;
}
