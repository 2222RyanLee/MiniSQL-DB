#include "catalog/catalog.h"
#include "common/macros.h"
#include "index/b_plus_tree.h"
void CatalogMeta::SerializeTo(char *buf) const {
  // ASSERT(false, "Not Implemented yet");
  int offset = 0;//偏移量
  int num1 = table_meta_pages_.size();//第一个map的成员数量
  int num2 = index_meta_pages_.size();//第二个map的成员数量
    MACH_WRITE_TO(int,buf+offset,num1);
    offset+=sizeof(int);
    for(auto i:this->table_meta_pages_){
      MACH_WRITE_TO(table_id_t,buf+offset,i.first);
      offset+=sizeof(table_id_t);
      MACH_WRITE_TO(page_id_t,buf+offset,i.second);
      offset+=sizeof(page_id_t);
    }

    MACH_WRITE_TO(int,buf+offset,num2);
    offset+=sizeof(int);
    for(auto i:index_meta_pages_){
      MACH_WRITE_TO(index_id_t,buf+offset,i.first);
      offset+=sizeof(index_id_t);
      MACH_WRITE_TO(page_id_t,buf+offset,i.second);
      offset+=sizeof(page_id_t);
    }

}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf, MemHeap *heap) {
  // ASSERT(false, "Not Implemented yet");
  int offset = 0;
  char *t = new char[100];//创建一个临时的指针，指向开辟的空间
  memcpy(t,buf,sizeof(int));
  int num1 = MACH_READ_FROM(int,t);
  offset+=sizeof(int);
//  void * memory = heap->Allocate(sizeof(CatalogMeta));
//  CatalogMeta* p = new(memory)CatalogMeta();
  CatalogMeta* memory = CatalogMeta::NewInstance(heap);//用一个指针来接住这块分配出的空间
  for(int i=0;i<num1;i++){
    table_id_t a = 0;
    page_id_t  b = 0;
    memset(t,0,sizeof(*t));//先对内容进行重置
    memcpy(t,buf+offset,sizeof(table_id_t));
    a = MACH_READ_FROM(table_id_t,t);
    offset+=sizeof(table_id_t);
    memset(t,0,sizeof(*t));//先对内容进行重置
    memcpy(t,buf+offset,sizeof(page_id_t));
    b = MACH_READ_FROM(page_id_t,t);
    offset+=sizeof(page_id_t);

    memory->table_meta_pages_.emplace(a,b);//将读出来的内容放置到创建好的catalog中去
  }

  memset(t,0,sizeof(*t));//先对内容进行重置
  memcpy(t,buf+offset,sizeof(int));
  int num2 = MACH_READ_FROM(int,t);
  offset+=sizeof(int);
  for(int i = 0;i<num2;i++){
    index_id_t a = 0;
    page_id_t  b = 0;
    memset(t,0,sizeof(*t));//先对内容进行重置
    memcpy(t,buf+offset,sizeof(index_id_t));
    a = MACH_READ_FROM(index_id_t,t);
    offset+=sizeof(index_id_t);
    memset(t,0,sizeof(*t));//先对内容进行重置
    memcpy(t,buf+offset,sizeof(page_id_t));
    b = MACH_READ_FROM(page_id_t,t);
    offset+=sizeof(page_id_t);

    memory->index_meta_pages_.emplace(a,b);//将读出来的内容放到创建好的index 的map中去
  }

  delete []t;
  return memory;
//  return nullptr;
}

uint32_t CatalogMeta::GetSerializedSize() const {
  // ASSERT(false, "Not Implemented yet");
  int sum = 2 * sizeof(int) + (sizeof(table_id_t) + sizeof(page_id_t))*this->table_meta_pages_.size() +
            (sizeof(index_id_t) + sizeof(page_id_t))*this->index_meta_pages_.size();
  return sum;
}

CatalogMeta::CatalogMeta() {}


CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
        : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager),
          log_manager_(log_manager), heap_(new SimpleMemHeap()) {
  next_table_id_.store(0,std::memory_order_relaxed); //初始化tableid为0，在这里都是自己定义的
  next_index_id_.store(0,std::memory_order_relaxed); //初始化indexid为0          
  if(init){//如果init=true，则创建一个新的，初始化元数据
  catalog_meta_ = new CatalogMeta();//初始化catalog meta
  }
  else {//加载catalog meta page上面所有的表和索引信息，load table 和load index
  auto * page = buffer_pool_manager->FetchPage(CATALOG_META_PAGE_ID);
  ASSERT(page != nullptr,"error! the page is null");  //确保page有效
  char * buf = page->GetData();
  catalog_meta_ =catalog_meta_->DeserializeFrom(buf, heap_);//反序列化出一个catalog meta
  for(std::pair<table_id_t, page_id_t>instance:catalog_meta_->table_meta_pages_){//遍历每一个table_meta_pages并load到catalogmanager上去
    LoadTable(instance.first,instance.second);
  }
  for(std::pair<index_id_t, page_id_t>instance:catalog_meta_->index_meta_pages_){//遍历每一个table_meta_pages并load到catalogmanager上去
    LoadIndex(instance.first,instance.second);
  }
  }
}

CatalogManager::~CatalogManager() {
  FlushCatalogMetaPage();//把修改过的catalogmeta写回
  delete heap_;
}

dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema,
                                    Transaction *txn, TableInfo *&table_info) {//catalog中新建一个table，table_id在table_heap当中分配问题以及flush之后重新fetch丢数据
  if(table_names_.find(table_name)==table_names_.end()){//如果找不到table，则可以创建
  table_id_t tid = next_table_id_.fetch_add(1);//tableid在atomic当中+1
  //table_n++;
  //next_table_id_.store(table_n,std::memory_order_relaxed);
  table_names_.insert({table_name,tid});
  TableHeap *table_heap = nullptr;
  TableMetadata *table_meta = nullptr;
  table_heap = table_heap->Create(buffer_pool_manager_,schema,txn,log_manager_,lock_manager_,heap_); //新增一个table_heap
  page_id_t root_page_id = table_heap->GetFirstPageId(); //this table 的page id 是tableheap分配的first page id
  //*********************************table_heap如何分配first_page_id还有待完善
  table_meta = table_meta->Create(tid,table_name,root_page_id,schema,heap_);//新增一个table_meta
  table_info = table_info->Create(heap_);
  table_info->Init(table_meta,table_heap); //初始化table_info
  tables_.insert({tid,table_info});
  auto * page = buffer_pool_manager_->FetchPage(root_page_id);//把table meta写入到root_page_id当中
  ASSERT(page != nullptr,"error! the page is null");  //确保page有效
  table_meta->SerializeTo(page->GetData());//反序列化
  buffer_pool_manager_->UnpinPage(root_page_id,true);
  buffer_pool_manager_->FlushPage(root_page_id);//要把新增的table写进磁盘
  catalog_meta_->table_meta_pages_.insert({tid,root_page_id});
  return DB_SUCCESS;
  }
  else return DB_TABLE_ALREADY_EXIST; //找到了则返回已经创建
}


dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {//通过查找tablename返回一个table_info
  if(table_names_.find(table_name)==table_names_.end())//如果找不到table
  return DB_TABLE_NOT_EXIST;
  else {
    table_id_t tid = table_names_[table_name];//找到了table则从Map中传给table_info
    table_info = tables_[tid];
    return DB_SUCCESS;
  }
}

dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  //table_id_t table_n = next_table_id_.load(); //取table总数
  //if (table_n>0){//如果table不为1，遍历每一个table
    //TableInfo * tableinfo = nullptr;
  //  for(int i=0;i<(int)table_n;i++){
      //table_id_t tid = i;
      for(std::pair<table_id_t, TableInfo *> i:tables_){
      //tableinfo = this->tables_.find(tid)->second;
      tables.push_back(i.second); //每个tableinfo* push到vector当中
  }
  return DB_SUCCESS;
  }
  //else return DB_FAILED;
//}

dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info) {//根据index root page找到所需b+树的root page id，然后根据id取出root page，然后把这个page转换成B+树page
  if(table_names_.find(table_name)!=table_names_.end()){//如果table存在
  index_id_t iid;
  table_id_t tid;
  vector<uint32_t> key_map; //*************************这张表的哪一些列建立了索引，这里存储的是相应的列对饮的下标号
    tid = table_names_.find(table_name)->second;
    Schema * tableschema = tables_.find(tid)->second->GetSchema();
    u_int32_t index = 0;
    for(string i:index_keys){//遍历每一个index
      if (tableschema->GetColumnIndex(i,index)== DB_COLUMN_NAME_NOT_EXIST) return DB_COLUMN_NAME_NOT_EXIST; //只要有一个不存在就返回DB_COLUMN_NAME_NOT_EXIST
      else key_map.push_back(index); //存储列的下标号
    }
  iid = next_index_id_.fetch_add(1);//indexid在atomic当中+1
  if(index_names_.find(table_name)==index_names_.end()){//table存在且table中不存在任何index
  std::unordered_map<std::string, index_id_t> indexmap;//初始化这个table的indexmap
  indexmap.insert({index_name,iid});  //该table的indexmap插入这一条index记录
  //index_names_.erase(table_name);
  index_names_.insert({table_name,indexmap});//替换为修改过的      
  }
  else {//table存在且table中含有index
  //tid = table_names_.find(table_name)->second;
  //Schema * tableschema = tables_.find(tid)->second->GetSchema();
  //u_int32_t index = 0;
  //for(string i:index_keys){//遍历每一个index
  //  if (tableschema->GetColumnIndex(i,index)== DB_COLUMN_NAME_NOT_EXIST) return DB_COLUMN_NAME_NOT_EXIST; //只要有一个不存在就返回DB_COLUMN_NAME_NOT_EXIST
  //}
  //iid = next_index_id_.fetch_add(1);//indexid在atomic当中+1
  std::unordered_map<std::string, index_id_t> indexmap = index_names_.find(table_name)->second;//取出这个table的indexmap
  if (indexmap.find(index_name)!=indexmap.end()) return DB_INDEX_ALREADY_EXIST; //如果找到了index则返回DB_INDEX_ALREADY_EXIST
  indexmap.insert({index_name,iid});  //该table的indexmap插入这一条index记录
  index_names_.erase(table_name);
  index_names_.insert({table_name,indexmap});//替换为修改过的 
  }
  //************************INDEX_ROOTS_PAGE_ID=1
  IndexMetadata *indexmetadata = nullptr;
  indexmetadata = indexmetadata->Create(iid,index_name,tid, key_map,heap_);//indexmetadata
  TableInfo * table_info = tables_.find(tid)->second; //获取当前table的tableinfo
  index_info = index_info->Create(heap_);//
  index_info->Init(indexmetadata,table_info,buffer_pool_manager_);
  page_id_t page_id;
  buffer_pool_manager_->NewPage(page_id);//取一个新的页，并获得其page_id
  auto * page = buffer_pool_manager_->FetchPage(page_id);//把INDEX meta写入到page_id当中
  ASSERT(page != nullptr,"error! the page is null");  //确保page有效
  //IndexRootsPage *root_page = reinterpret_cast<IndexRootsPage *>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID));
  
  indexmetadata->SerializeTo(page->GetData());//序列化到page当中
  buffer_pool_manager_->UnpinPage(page_id,true);
  buffer_pool_manager_->FlushPage(page_id);//要把修改过的写进磁盘
  indexes_.insert({iid,index_info});
  catalog_meta_->index_meta_pages_.insert({tid,page_id});
  return DB_SUCCESS;
  }
  else return DB_TABLE_NOT_EXIST; //table不存在
}

dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  if(index_names_.find(table_name)==index_names_.end())//如果找不到table
  return DB_TABLE_NOT_EXIST;
  else {
    std::unordered_map<std::string, index_id_t> findind = index_names_.find(table_name)->second;
    if(findind.find(index_name)==findind.end()) 
    return DB_INDEX_NOT_FOUND;
    else{
    //table_id_t tid = index_names_.find(table_name).second;//找到了table则从Map中传给table_info
      index_id_t iid = findind.find(index_name)->second;
      index_info = indexes_.find(iid)->second;
      return DB_SUCCESS;
    }
  }
}

dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {//返回一个给定table的所有index的vector
  if(index_names_.find(table_name)==index_names_.end())//如果找不到table
  return DB_TABLE_NOT_EXIST;
  else {
    std::unordered_map<std::string, index_id_t> findind = index_names_.find(table_name)->second;//找到属于table_name的map
    //Set<std::string> s = findind.keySet();
     for(unordered_map<std::string, index_id_t>::iterator iter=findind.begin();iter!=findind.end();iter++){//遍历findind中
      indexes.push_back(indexes_.find(iter->second)->second); 
      //iter second作为indexid，在indexes_ map当中查找indexid所对应的indexinfo*并pushback到vector当中
    }
    return DB_SUCCESS;
  }
}

dberr_t CatalogManager::DropTable(const string &table_name) {
  if(table_names_.find(table_name)!=table_names_.end()){
    table_id_t tid = table_names_.find(table_name)->second;
    table_names_.erase(table_name);
    tables_.erase(tid);
    catalog_meta_->table_meta_pages_.erase(tid);
    return DB_SUCCESS;
  }
  else return DB_TABLE_NOT_EXIST;
}

dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  if(table_names_.find(table_name)!=table_names_.end()){
    std::unordered_map<std::string, index_id_t> findind = index_names_.find(table_name)->second;
    if(findind.find(index_name)==findind.end()) return DB_INDEX_NOT_FOUND;
    else{
      index_id_t iid = findind.find(index_name)->second;
      findind.erase(index_name);
      index_names_.erase(table_name);
      index_names_.insert({table_name,findind});
      indexes_.erase(iid);
      catalog_meta_->index_meta_pages_.erase(iid);
      return DB_SUCCESS;
    }    
  }
  else return DB_TABLE_NOT_EXIST;
}


dberr_t CatalogManager::FlushCatalogMetaPage() const {//通过buffer pool manager重新序列化catalog meta写入catalog meta page并强制它马上刷到磁盘 
  auto * page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  ASSERT(page != nullptr,"error! the page is null");  //确保page有效
  char * buf = page->GetData();
  catalog_meta_->SerializeTo(buf);//catalog meta 序列化到catalog meta page上
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID,true);
  buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID);
  return DB_SUCCESS;
}

dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {//通过传入参数来从catalog meta载入table
    auto * page = buffer_pool_manager_->FetchPage(page_id);
    if (page==nullptr) return DB_TABLE_NOT_EXIST;
    else{
      auto *table_info = TableInfo::Create(heap_);
      TableMetadata *table_meta = nullptr;
      TableMetadata::DeserializeFrom(page->GetData(),table_meta,table_info->GetMemHeap());
      if (table_meta != nullptr) {
        auto *table_heap = TableHeap::Create(buffer_pool_manager_,page_id,table_meta->GetSchema(),log_manager_,lock_manager_,table_info->GetMemHeap());
        table_info->Init(table_meta, table_heap);
      }
      //TableMetadata *table_meta = nullptr;//(TableMetadata*)malloc(sizeof(TableMetadata));
      //table_meta->DeserializeFrom(page->GetData(),table_meta,heap_);//反序列化
      //TableHeap *table_heap = nullptr;
      //table_info->Init(table_meta,table_heap); //初始化table_info
      next_table_id_.fetch_add(1);
      table_names_.insert({table_meta->GetTableName(),table_id});
      tables_.insert({table_id,table_info});
      buffer_pool_manager_->UnpinPage(page_id,false);
      return DB_SUCCESS;
    }
}

dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {//通过传入参数从catalog meta载入index
  auto * page = buffer_pool_manager_->FetchPage(page_id);//取indexmeta所对应的page_id
  if (page==nullptr) return DB_INDEX_NOT_FOUND;
  else{
    auto * index_info = IndexInfo::Create(heap_);
    IndexMetadata * index_meta = nullptr;
    index_meta->DeserializeFrom(page->GetData(),index_meta,index_info->GetMemHeap());
    TableInfo * table_info = nullptr;
    this->GetTable(index_meta->GetTableId(),table_info); //载入table_info
    index_info->Init(index_meta,table_info,buffer_pool_manager_);
    //IndexInfo * index_info = nullptr;
    //index_info = index_info->Create(heap_);
    //indexes_.insert({index_meta->GetTableName ,index_info});
    //if (index_meta!=nullptr){
    //}
    table_id_t tid = index_meta->GetTableId();
    std::string tnames;
    for(std::unordered_map<std::string,table_id_t>::iterator it = table_names_.begin();it!=table_names_.end();it++) 
	  {
		if(it->second==tid)  {tnames = it->first; break;} //根据tid找到map中的table_names_
	  }
    if(index_names_.find(tnames)==index_names_.end()){//table存在且table中不存在任何index
    std::unordered_map<std::string, index_id_t> indexmap;//初始化这个table的indexmap
    indexmap.insert({index_meta->GetIndexName(),index_id});  //该table的indexmap插入这一条index记录
    //index_names_.erase(table_name);
    index_names_.insert({tnames,indexmap});//替换为修改过的      
    }
    else {
    std::unordered_map<std::string, index_id_t> indexmap = index_names_.find(tnames)->second;//取出这个table的indexmap
    if (indexmap.find(index_meta->GetIndexName())!=indexmap.end()) return DB_INDEX_ALREADY_EXIST; //如果找到了index则返回DB_INDEX_ALREADY_EXIST
    indexmap.insert({index_meta->GetIndexName(),index_id});  //该table的indexmap插入这一条index记录
    index_names_.erase(tnames);
    index_names_.insert({tnames,indexmap});//替换为修改过的 
    }
    next_index_id_.fetch_add(1);
    indexes_.insert({index_id,index_info});
    buffer_pool_manager_->UnpinPage(page_id,false);
    return DB_SUCCESS;
  }
}

dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  if(tables_.find(table_id)!=tables_.end()){
    table_info = tables_.find(table_id)->second;
    return DB_SUCCESS;
  }
  else return DB_TABLE_NOT_EXIST;
}