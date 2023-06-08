#include "catalog/table.h"
#include "common/macros.h"
uint32_t TableMetadata::SerializeTo(char *buf) const {
  int offset = 0;
  MACH_WRITE_TO(table_id_t,buf+offset,table_id_);
  offset+=sizeof(table_id_t);
  u_int32_t namelen = table_name_.length();
    MACH_WRITE_TO(uint32_t,buf+offset,namelen); //先序列化name的长度
        offset+= sizeof(u_int32_t);//计算返回值
    for (int i=0;i<(int)table_name_.length();i++){  //序列化name_数组
      char c = table_name_[i];
      MACH_WRITE_TO(char,buf+offset,c);
      offset+=sizeof(char);
    }
  //offset+=sizeof(string);
  MACH_WRITE_TO(page_id_t,buf+offset,root_page_id_);
  offset+=sizeof(page_id_t);
  offset+=this->schema_->SerializeTo(buf+offset);
  return offset;
}

uint32_t TableMetadata::GetSerializedSize() const {
  int sum = sizeof(table_id_t)+sizeof(u_int32_t)+this->table_name_.length()*sizeof(char)+sizeof(page_id_t)+this->schema_->GetSerializedSize();
  return sum;
}

/**
 * @param heap Memory heap passed by TableInfo
 */
uint32_t TableMetadata::DeserializeFrom(char *buf, TableMetadata *&table_meta, MemHeap *heap) {
  int offset = 0;
  char *t = new char[100];//创建一个临时的指针，指向开辟的空间
  memcpy(t,buf+offset,sizeof(table_id_t));
  table_id_t table_id = MACH_READ_FROM(table_id_t,t);
  offset+=sizeof(table_id_t);

  memset(t,0,sizeof(*t));//先对内容进行重置
  memcpy(t,buf+offset,sizeof(u_int32_t)); //拷贝buf的值
  u_int32_t namelen = MACH_READ_UINT32(t); 
  offset += sizeof(u_int32_t);
  std::string table_name;
  //char *c = new char[100];
  for (int i = 0;i<(int)namelen ;i++){//反序列化name
    //char c = t[ofs];
    //memcpy(c,t+ofs,sizeof(char));
    memset(t,0,sizeof(*t));
    memcpy(t,buf+offset,sizeof(char));
    offset+=sizeof(char);
    table_name.push_back(*t);}

  memset(t,0,sizeof(*t));//先对内容进行重置
  memcpy(t,buf+offset,sizeof(page_id_t));
  page_id_t root_page_id = MACH_READ_FROM(page_id_t,t);
  offset+=sizeof(page_id_t);

  Schema *a = nullptr ;
  offset+=a->DeserializeFrom(buf+offset,a,heap);
  table_meta = table_meta->Create(table_id,table_name,root_page_id,a,heap);

  delete []t;
  return offset;
}

/**
 * Only called by create table
 *
 * @param heap Memory heap passed by TableInfo
 */
TableMetadata *TableMetadata::Create(table_id_t table_id, std::string table_name,
                                     page_id_t root_page_id, TableSchema *schema, MemHeap *heap) {
  // allocate space for table metadata
  void *buf = heap->Allocate(sizeof(TableMetadata));
  return new(buf)TableMetadata(table_id, table_name, root_page_id, schema);
}

TableMetadata::TableMetadata(table_id_t table_id, std::string table_name, page_id_t root_page_id, TableSchema *schema)
        : table_id_(table_id), table_name_(table_name), root_page_id_(root_page_id), schema_(schema) {}
