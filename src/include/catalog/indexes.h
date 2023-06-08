#ifndef MINISQL_INDEXES_H
#define MINISQL_INDEXES_H

#include <memory>

#include "catalog/table.h"
#include "index/generic_key.h"
#include "index/b_plus_tree_index.h"
#include "record/schema.h"

class IndexMetadata {
  friend class IndexInfo;

public:
  static IndexMetadata *Create(const index_id_t index_id, const std::string &index_name,
                               const table_id_t table_id, const std::vector<uint32_t> &key_map,
                               MemHeap *heap);

  uint32_t SerializeTo(char *buf) const;

  uint32_t GetSerializedSize() const;

  static uint32_t DeserializeFrom(char *buf, IndexMetadata *&index_meta, MemHeap *heap);

  inline std::string GetIndexName() const { return index_name_; }

  inline table_id_t GetTableId() const { return table_id_; }

  uint32_t GetIndexColumnCount() const { return key_map_.size(); }

  inline const std::vector<uint32_t> &GetKeyMapping() const { return key_map_; }

  inline index_id_t GetIndexId() const { return index_id_; }

private:
  IndexMetadata() = delete;

  explicit IndexMetadata(const index_id_t index_id, const std::string &index_name,
                         const table_id_t table_id, const std::vector<uint32_t> &key_map) {
    this->index_id_ = index_id;
    this->index_name_ = index_name;
    this->table_id_ = table_id;
    this->key_map_ = key_map;
  }

private:
  static constexpr uint32_t INDEX_METADATA_MAGIC_NUM = 344528;
  index_id_t index_id_;
  std::string index_name_;//这个应该可以理解为建立在某一张表上的某一个索引，拥有自己的id 和name，同时标注了table的id
  table_id_t table_id_;
  std::vector<uint32_t> key_map_;  /** The mapping of index key to tuple key */
};//这个key_map感觉应该是每一个索引对应一个tuple，但里面具体存储的是什么不太清楚,可能是索引所对应的tuple的下标号
  //因为索引可能不是对应每一条的tuple，而是间断的对应
  //上面的理解都有问题，这里的key——map实际上应该是这张表的哪一些列建立了索引，这里存储的是相应的列对饮的下标号

/**
 * The IndexInfo class maintains metadata about a index.
 */
class IndexInfo {
public:
  static IndexInfo *Create(MemHeap *heap) {
    void *buf = heap->Allocate(sizeof(IndexInfo));
    return new(buf)IndexInfo();
  }

  ~IndexInfo() {
    delete heap_;
  }

  void Init(IndexMetadata *meta_data, TableInfo *table_info, BufferPoolManager *buffer_pool_manager) {
    // Step1: init index metadata and table info
    this->meta_data_ = meta_data;
    this->table_info_ = table_info;
    // Step2: mapping index key to key schema
    this->key_schema_ = key_schema_->ShallowCopySchema(this->table_info_->GetSchema(),this->meta_data_->key_map_,this->heap_);
    //通过这一步，从table里将建立索引的列浅拷贝到key——schema里
    // Step3: call CreateIndex to create the index
    this->index_ = CreateIndex(buffer_pool_manager);

    //ASSERT(false, "Not Implemented yet.");
  }

  inline Index *GetIndex() { return index_; }

  inline std::string GetIndexName() { return meta_data_->GetIndexName(); }

  inline IndexSchema *GetIndexKeySchema() { return key_schema_; }

  inline MemHeap *GetMemHeap() const { return heap_; }

  inline TableInfo *GetTableInfo() const { return table_info_; }

  inline IndexMetadata *GetMetaData()const{return meta_data_;}

private:
  explicit IndexInfo() : meta_data_{nullptr}, index_{nullptr}, table_info_{nullptr},
                         key_schema_{nullptr}, heap_(new SimpleMemHeap()) {}

  //INDEX_TEMPLATE_ARGUMENTS
  Index *CreateIndex(BufferPoolManager *buffer_pool_manager) {
   // GenericComparator <64> t(this->key_schema_);//不确定这个大小是否合适

   //GenericComparator<64> (this->key_schema_);
   int length = this->key_schema_->GetSerializedSize();
   Index * a;
   if(length<=4){
     void * buf = this->heap_->Allocate(sizeof(BPlusTreeIndex <GenericKey<4>,RowId,GenericComparator<4> >));//这里是按照leaf node进行传的，不知道是否正确
     a = new (buf) BPlusTreeIndex <GenericKey<4>,RowId,GenericComparator<4>>(this->meta_data_->index_id_,this->key_schema_,buffer_pool_manager);
   }
   else if(length<=8){
     void * buf = this->heap_->Allocate(sizeof(BPlusTreeIndex <GenericKey<8>,RowId,GenericComparator<8> >));//这里是按照leaf node进行传的，不知道是否正确
     a = new (buf) BPlusTreeIndex <GenericKey<8>,RowId,GenericComparator<8>>(this->meta_data_->index_id_,this->key_schema_,buffer_pool_manager);
   }
   else if(length<=16) {
     void * buf = this->heap_->Allocate(sizeof(BPlusTreeIndex <GenericKey<16>,RowId,GenericComparator<16> >));//这里是按照leaf node进行传的，不知道是否正确
     a = new (buf) BPlusTreeIndex <GenericKey<16>,RowId,GenericComparator<16>>(this->meta_data_->index_id_,this->key_schema_,buffer_pool_manager);

   } else if (length<=32) {
     void * buf = this->heap_->Allocate(sizeof(BPlusTreeIndex <GenericKey<32>,RowId,GenericComparator<32> >));//这里是按照leaf node进行传的，不知道是否正确
     a = new (buf) BPlusTreeIndex <GenericKey<32>,RowId,GenericComparator<32>>(this->meta_data_->index_id_,this->key_schema_,buffer_pool_manager);

   } else if (length<=64) {
     void * buf = this->heap_->Allocate(sizeof(BPlusTreeIndex <GenericKey<64>,RowId,GenericComparator<64> >));//这里是按照leaf node进行传的，不知道是否正确
     a = new (buf) BPlusTreeIndex <GenericKey<64>,RowId,GenericComparator<64>>(this->meta_data_->index_id_,this->key_schema_,buffer_pool_manager);

   } else {
     void * buf = this->heap_->Allocate(sizeof(BPlusTreeIndex <GenericKey<64>,RowId,GenericComparator<64> >));//这里是按照leaf node进行传的，不知道是否正确
     a = new (buf) BPlusTreeIndex <GenericKey<64>,RowId,GenericComparator<64>>(this->meta_data_->index_id_,this->key_schema_,buffer_pool_manager);
   }//理论上支持的最大的就是64个字节的key，再大了就处理不了了

   //void * buf = this->heap_->Allocate(sizeof(BPlusTreeIndex <GenericKey<64>,RowId,GenericComparator<64> >));//这里是按照leaf node进行传的，不知道是否正确
   //Index* a = new (buf) BPlusTreeIndex <GenericKey<64>,RowId,GenericComparator<64>>(this->meta_data_->index_id_,this->key_schema_,buffer_pool_manager);
    //ASSERT(false, "Not Implemented yet.");
    //TableHeap *myTableHeap = this->table_info_->GetTableHeap();
    //for(auto iter = myTableHeap->Begin();iter!=myTableHeap->End();iter++){
      //vector<uint32_t> mapping = this->meta_data_->GetKeyMapping();
      //vector<Field> myField;
      //Row row(iter->GetRowId());
      //myTableHeap->GetTuple(&row,nullptr);
      //for(auto i:mapping){
        // myField.push_back(*(iter->GetField(i)));
        // std::cout<<i<<endl;
        //myField.push_back(*(row.GetField(i)));
      //}
      //Row a1(myField);
      //a->InsertEntry(a1, iter.GetRowId(), nullptr);
   //}
    return a;
    //这里就存在两个问题。一个是进行比较的field的数量的问题，另一个是keyType 和keyValue类型的问题，目前是按照叶子节点进行创建的
   // return nullptr;
  }

private:
  IndexMetadata *meta_data_;//这里是一个table对应的索引的基本信息
  Index *index_;//这里面也有一个 Key—schema
  TableInfo *table_info_;//这里存放的是index对应的table包含的information
  IndexSchema *key_schema_;//这里又是一个key——schema，schema里包含有很多的column，这里的key——schema是建立索引的column的集合
  MemHeap *heap_;
};

#endif //MINISQL_INDEXES_H
