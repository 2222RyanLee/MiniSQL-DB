#include "catalog/indexes.h"
#include "common/macros.h"
IndexMetadata *IndexMetadata::Create(const index_id_t index_id, const string &index_name,
                                     const table_id_t table_id, const vector<uint32_t> &key_map,
                                     MemHeap *heap) {
  void *buf = heap->Allocate(sizeof(IndexMetadata));
  return new(buf)IndexMetadata(index_id, index_name, table_id, key_map);
}

uint32_t IndexMetadata::SerializeTo(char *buf) const {
  int offset = 0;
  MACH_WRITE_TO(index_id_t,buf+offset,index_id_);
  offset+=sizeof(index_id_);
  MACH_WRITE_TO(string,buf+offset,index_name_);
  offset+=sizeof(index_name_);
  MACH_WRITE_TO(table_id_t,buf+offset,table_id_);
  offset+=sizeof(table_id_);
  int num = key_map_.size();
  MACH_WRITE_TO(int,buf+offset,num);
  offset+=sizeof(int);
  for(size_t i=0;i<key_map_.size();i++){
    MACH_WRITE_TO(uint32_t,buf+offset,key_map_[i]);
    offset+=sizeof(key_map_[i]);
  }

  return offset;
}

uint32_t IndexMetadata::GetSerializedSize() const {
  int sum = sizeof(index_id_)+sizeof(index_name_)+sizeof(table_id_)+sizeof(uint32_t)*this->key_map_.size()+sizeof(int);
  return sum;
}

uint32_t IndexMetadata::DeserializeFrom(char *buf, IndexMetadata *&index_meta, MemHeap *heap) {
  int offset = 0;
  char *t = new char[100];//创建一个临时的指针，指向开辟的空间
  memcpy(t,buf+offset,sizeof(index_id_t));
  index_id_t index_id = MACH_READ_FROM(index_id_t,t);
  offset+=sizeof(index_id_t);

  memset(t,0,sizeof(*t));//先对内容进行重置
  memcpy(t,buf+offset,sizeof(string));
  string index_name = MACH_READ_FROM(string,t);
  offset+=sizeof(string);

  memset(t,0,sizeof(*t));//先对内容进行重置
  memcpy(t,buf+offset,sizeof(table_id_t));
  table_id_t table_id = MACH_READ_FROM(table_id_t,t);
  offset+=sizeof(table_id_t);

  vector< uint32_t> key_map;
  memset(t,0,sizeof(*t));//先对内容进行重置
  memcpy(t,buf+offset,sizeof(int));
  int num = MACH_READ_FROM(int,t);//读出vector里的元素个数
  offset+=sizeof(int);
  for(int i=0;i<num;i++){
    memset(t,0,sizeof(*t));//先对内容进行重置
    memcpy(t,buf+offset,sizeof(uint32_t));
    uint32_t a = MACH_READ_FROM(uint32_t,t);
    key_map.push_back(a);
    offset+=sizeof(uint32_t);
  }

  index_meta = IndexMetadata::Create(index_id,index_name,table_id,key_map,heap);
  //IndexMetadata* memory = IndexMetadata::Create(index_id,index_name,table_id,key_map,heap);
  // index_meta = memory;
  delete []t;

  return offset;
}