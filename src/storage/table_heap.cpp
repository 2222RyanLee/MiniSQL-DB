#include "storage/table_heap.h"
// 读取每页，检查每页的剩余空间，找到偏移位置，插入，写回
bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
  if(row.GetSerializedSize(schema_)>PAGE_SIZE)
    return false;
  auto now_page=reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  if(now_page==nullptr){
    return false;
  }
  page_id_t next_page_id=0;
  while(!now_page->InsertTuple(row,schema_,txn,lock_manager_,log_manager_)){
    next_page_id=now_page->GetNextPageId();
    if(next_page_id!=INVALID_PAGE_ID){
      buffer_pool_manager_->UnpinPage(now_page->GetPageId(),false);
      now_page=reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(next_page_id));
    }
    else{
      auto new_page=reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(next_page_id));
      if(new_page==nullptr){
        buffer_pool_manager_->UnpinPage(now_page->GetPageId(),true);
        return false;
      }
      now_page->SetNextPageId(next_page_id);
      new_page->Init(next_page_id,now_page->GetPageId(),log_manager_,txn);
      buffer_pool_manager_->UnpinPage(now_page->GetPageId(),true);
      now_page=new_page;
    }
  }
  buffer_pool_manager_->UnpinPage(now_page->GetPageId(),true);
  return true;
}
 
bool TableHeap::MarkDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    return false;
  } 
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}
// 通过返回值区分错误类型：
//  
bool TableHeap::UpdateTuple(const Row &row, const RowId &rid, Transaction *txn) {
  int response;
  TablePage *now_page=reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // page_id_t next_page_id=now_page->GetNextPageId(); //下一页
  // 页号错误
  if(now_page==nullptr)
    return false;
  // 新纪录超过页大小
  if(row.GetSerializedSize(schema_)>PAGE_SIZE)
    return false;
  Row old_row(rid);
  if((response=now_page->UpdateTuple(row,&old_row,schema_,txn,lock_manager_,log_manager_))!=666){
    switch(response){
      case 1://记录位置错误
        return false;
        break;
      case 2://记录标记删除
        return false;
        break;
      case 3://记录标记删除
        return false;
        break;
      case 4://大小不够，删除、寻页、插入
        MarkDelete(rid,txn);
        Row *p=(Row*)&row;
        InsertTuple(*p,txn);
        // 返回rowid？
        break;

    }
  }
  return true;
  
}

void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
  // Step1: Find the page which contains the tuple.
  // Step2: Delete the tuple from the page.
  page_id_t now_page_id=rid.GetPageId();
  TablePage *now_page=reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(now_page_id));
  now_page->ApplyDelete(rid,txn,log_manager_);
  buffer_pool_manager_->UnpinPage(now_page->GetPageId(),true);
}

void TableHeap::RollbackDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback the delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::FreeHeap() {
  // 删除页 是否删除schema
  buffer_pool_manager_->FlushAllPages();
  TablePage *now_page=reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  page_id_t next_page_id=now_page->GetNextPageId();
  while(next_page_id!=INVALID_PAGE_ID){
    buffer_pool_manager_->UnpinPage(now_page->GetPageId(),false);
    buffer_pool_manager_->DeletePage(now_page->GetPageId());
    now_page=reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(next_page_id));
    next_page_id=now_page->GetNextPageId();
  }
  // 需不需要把指针指向空
}

bool TableHeap::GetTuple(Row *row, Transaction *txn) {
//  if(row->GetSerializedSize(schema_)>PAGE_SIZE)
//     return false;
  RowId id(row->GetRowId().GetPageId(),row->GetRowId().GetSlotNum());
  auto now_page=reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
  if(now_page==nullptr){
    return false;
  }
  now_page->GetTuple(row,schema_,txn,lock_manager_);
  return true;
}

TableIterator TableHeap::Begin() {
  return TableIterator(this, first_page_id_);
}

TableIterator TableHeap::End() {
  return TableIterator(nullptr,INVALID_PAGE_ID);
}
