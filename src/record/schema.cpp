#include "record/schema.h"

uint32_t Schema::SerializeTo(char *buf) const {
  u_int32_t size = GetColumns().size(); //先传一个column数量
  MACH_WRITE_UINT32(buf,size);
  u_int32_t total = sizeof(u_int32_t);
  for(int i=0; i<(int)size;i++){//schema is std::vector<Column *> columns_;
    u_int32_t s = GetColumn(i)->SerializeTo(buf+total);
    total+=s;//逐个传入column并序列化
  }
  return total;
}

uint32_t Schema::GetSerializedSize() const {
  uint32_t t = sizeof(u_int32_t);
  for(int i =0;i<(int)GetColumns().size();i++){
  t+=GetColumn(i)->GetSerializedSize();   //获取每一个column的serializedsize()
  }
  return t;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema, MemHeap *heap) {
  char *t = new char[100];//创建一个临时的指针
  memcpy(t,buf,sizeof(u_int32_t)); //拷贝buf值
  u_int32_t ofs = sizeof(u_int32_t);
  u_int32_t size =  MACH_READ_UINT32(t);
  std::vector<Column *> columns_; //实际上是初始化schema，因为columns vector用于后续heap分配
  for (int i=0;i<(int)size;i++){//重新还原逐个column
      Column *p;
      memset(t,0,sizeof(*t));//将内存块重置
      memcpy(t,buf+ofs,sizeof(Column));
      ofs+=p->DeserializeFrom(t,p,heap);
      columns_.push_back(p);
    }
  void *mem = heap->Allocate(sizeof(Schema));  //memheap上分配内存
  schema = new(mem)Schema(columns_); 
  delete []t;
  return ofs;
}
