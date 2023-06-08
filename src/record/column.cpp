#include "record/column.h"
Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
        : name_(std::move(column_name)), type_(type), table_ind_(index),
          nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt :
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat :
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
        : name_(std::move(column_name)), type_(type), len_(length),
          table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other) : name_(other->name_), type_(other->type_), len_(other->len_),
                                      table_ind_(other->table_ind_), nullable_(other->nullable_),
                                      unique_(other->unique_) {}

uint32_t Column::SerializeTo(char *buf) const {
    u_int32_t namelen = name_.length();
    int total = sizeof(u_int32_t);//计算返回值
    MACH_WRITE_TO(uint32_t,buf,namelen); //先序列化name的长度
    for (int i=0;i<(int)name_.length();i++){  //序列化name_数组
      char c = name_[i];
      MACH_WRITE_TO(char,buf+total,c); 
      total+=sizeof(char);
    }
    u_int32_t ischar;
    MACH_WRITE_TO(TypeId,buf+total,type_);
    total+=sizeof(TypeId);
    if (type_ == kTypeChar) ischar = 1; //如果是char类型，多序列化一个值
    else ischar = 0;
    if (ischar) {MACH_WRITE_TO(uint32_t,buf+total,len_);//如果是char类型序列化char的长度
    total+=sizeof(u_int32_t);}
    MACH_WRITE_TO(uint32_t,buf+total,table_ind_);
    total+=sizeof(u_int32_t);
    MACH_WRITE_TO(bool,buf+total,nullable_);
    total+=sizeof(bool);
    MACH_WRITE_TO(bool,buf+total,unique_);
    total+=sizeof(bool);
    return total;
}

uint32_t Column::GetSerializedSize() const {//序列化后的字节数，分两种情况来考虑

  if (type_ == kTypeChar) return name_.length()*sizeof(char) + 2*sizeof(bool) + 4*sizeof(u_int32_t)+sizeof(TypeId);
  else return name_.length()*sizeof(char) + 2*sizeof(bool) + 3*sizeof(u_int32_t)+sizeof(TypeId);
}

uint32_t Column::DeserializeFrom(char *buf, Column *&column, MemHeap *heap) {
  char *t = new char[100];//创建一个临时的指针
  memcpy(t,buf,sizeof(u_int32_t)); //拷贝buf的值
  u_int32_t namelen = MACH_READ_UINT32(t); 
  int ofs = sizeof(u_int32_t);
  std::string column_name;
  //char *c = new char[100];
  for (int i = 0;i<(int)namelen ;i++){//反序列化name
    //char c = t[ofs];
    //memcpy(c,t+ofs,sizeof(char));
    memset(t,0,sizeof(*t));
    memcpy(t,buf+ofs,sizeof(char));
    ofs+=sizeof(char);
    column_name.push_back(*t);}
    memset(t,0,sizeof(*t));
    memcpy(t,buf+ofs,sizeof(TypeId));
    TypeId type = MACH_READ_FROM(TypeId, t);
    ofs+=sizeof(TypeId);
    if (type == TypeId::kTypeChar) { //如果是keychar类型反序列化出长度
      memset(t,0,sizeof(*t));
    memcpy(t,buf+ofs,sizeof(u_int32_t));
      u_int32_t length = MACH_READ_UINT32(t);
      ofs+=sizeof(u_int32_t);
      memset(t,0,sizeof(*t));
    memcpy(t,buf+ofs,sizeof(u_int32_t));
      u_int32_t col_ind = MACH_READ_UINT32(t);
      ofs+=sizeof(u_int32_t);
      memset(t,0,sizeof(*t));
    memcpy(t,buf+ofs,sizeof(bool));
      bool nullable = MACH_READ_FROM(bool,t);
      ofs+=sizeof(bool);
      memset(t,0,sizeof(*t));
    memcpy(t,buf+ofs,sizeof(bool));
      bool unique = MACH_READ_FROM(bool,t);
      ofs+=sizeof(bool);
      void *mem = heap->Allocate(sizeof(Column));  //memheap上分配内存
      column = new(mem)Column(column_name, type, length, col_ind, nullable, unique); //column存储在memheap上
    }
    else{ //其它类型则不需要反序列化length，通过另一种方式在memheap上分配内存
    memset(t,0,sizeof(*t));
    memcpy(t,buf+ofs,sizeof(u_int32_t));
      u_int32_t col_ind = MACH_READ_UINT32(t);
      ofs+=sizeof(u_int32_t);
      memset(t,0,sizeof(*t));
    memcpy(t,buf+ofs,sizeof(bool));
      bool nullable = MACH_READ_FROM(bool,t);
      ofs+=sizeof(bool);
      memset(t,0,sizeof(*t));
    memcpy(t,buf+ofs,sizeof(bool));
      bool unique = MACH_READ_FROM(bool,t);
      ofs+=sizeof(bool);
    void *mem = heap->Allocate(sizeof(Column));  //memheap上分配内存
    column = new(mem)Column(column_name, type, col_ind, nullable, unique); 
    }//column存储在memheap上
    delete []t;
  return ofs;
}
