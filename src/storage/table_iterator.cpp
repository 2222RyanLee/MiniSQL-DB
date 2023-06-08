#include "common/macros.h"
#include "storage/table_iterator.h"
#include "storage/table_heap.h"

TableIterator::TableIterator(TableHeap* table_heap, page_id_t page_id):
  cur_page_id(page_id),cur_rowid((RowId*)&INVALID_ROWID){
  //初始化操作
  this->table_heap = table_heap;
  //找到page_id所在页设置为p
  if(page_id==INVALID_PAGE_ID || table_heap==nullptr){
    cur_page_id=page_id;
    *cur_rowid=INVALID_ROWID;
    this->row->SetRowId(INVALID_ROWID);
  }
  else{
    TablePage *table_page = reinterpret_cast<TablePage *>(this->table_heap->buffer_pool_manager_->FetchPage(page_id));
    this->row->SetRowId(*cur_rowid);
    
    //如果p为空页
    if(table_page->GetPageId() == INVALID_PAGE_ID)
      *cur_rowid = INVALID_ROWID;
      //如果非空页但没有记录
    else if(table_page->GetPageId() != INVALID_PAGE_ID&&table_page->GetFirstTupleRid(cur_rowid) == false){
        *cur_rowid = INVALID_ROWID;
    }
    else{
      row->SetRowId(*cur_rowid); //给row赋值
      table_heap->GetTuple(row,nullptr);
    }
  }
}

TableIterator::TableIterator(const TableIterator &other) {
  //拷贝函数拷贝过程
  this->table_heap = other.table_heap;
  this->cur_rowid = other.cur_rowid;
  this->row = new Row(*cur_rowid);
  table_heap->GetTuple(row,nullptr);
}

TableIterator::~TableIterator() {
  //析构时删除row
  // delete row;
}

bool TableIterator::operator==(const TableIterator &itr) const {
  //判断操作，判断row_id是否相等
  return cur_rowid==itr.cur_rowid;
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  //与等于的bool值相反
  return cur_rowid==itr.cur_rowid;
}

const Row &TableIterator::operator*() {
  // assert(*this != table_heap->End());
  //如果不在heap末尾则返回row*
  return *row;
}

Row *TableIterator::operator->() {
  // assert(*this != table_heap->End());
  //如果不在heap末尾返回row
  return row;
}

TableIterator &TableIterator::operator++() {
  // assert(*this != table_heap->End()); //assert this is not at the end of the table heap
  //变量定义
  RowId temp;
  page_id_t next_page_id;
  bool flag = false;
  TablePage *table_page = reinterpret_cast<TablePage *>(this->table_heap->buffer_pool_manager_->FetchPage(cur_page_id));
  //if can, make temp the next tuple of p
  if(table_page->GetNextTupleRid(*cur_rowid, &temp) == true){
    //transfer the value
    *cur_rowid = temp;
    row->SetRowId(*cur_rowid);
    table_heap->GetTuple(row,nullptr);
    flag = true; //mark that the next tuple has been found
  }
  //if can't be found next tuple in this page
  else{
    //get the next page and found if it has the next tuple
    next_page_id = table_page->GetNextPageId();
    while(next_page_id != INVALID_PAGE_ID){
      //let p be the next page
      table_page = reinterpret_cast<TablePage *>(this->table_heap->buffer_pool_manager_->FetchPage(next_page_id));
      //if can find the first tuple, then it's valid
      if(table_page->GetFirstTupleRid(&temp) == true){
        *cur_rowid = temp;
        row->SetRowId(*cur_rowid);
        table_heap->GetTuple(row,nullptr);
        flag = true; //mark that the next tuple has been found 
        break;
      }
      else
        next_page_id = table_page->GetNextPageId(); //continue the loop
    }
  }
  //if not found next valid tuple
  if(!flag){
    *cur_rowid = INVALID_ROWID;
    row->SetRowId(INVALID_ROWID);
    table_heap->GetTuple(row,nullptr);
  }
  //return the next tuple value
  return *this;
}

TableIterator &TableIterator::operator++(int) {
  // assert(*this != table_heap->End()); //assert this is not at the end of the table heap
  //变量定义
  RowId temp;
  page_id_t next_page_id;
  bool flag = false;
  TablePage *table_page = reinterpret_cast<TablePage *>(this->table_heap->buffer_pool_manager_->FetchPage(cur_page_id));
  //if can, make temp the next tuple of p
  if(table_page->GetNextTupleRid(*cur_rowid, &temp) == true){
    //transfer the value
    *cur_rowid = temp;
    row->SetRowId(*cur_rowid);
    table_heap->GetTuple(row,nullptr);
    flag = true; //mark that the next tuple has been found
  }
  //if can't be found next tuple in this page
  else{
    //get the next page and found if it has the next tuple
    next_page_id = table_page->GetNextPageId();
    while(next_page_id != INVALID_PAGE_ID){
      //let p be the next page
      table_page = reinterpret_cast<TablePage *>(this->table_heap->buffer_pool_manager_->FetchPage(next_page_id));
      //if can find the first tuple, then it's valid
      if(table_page->GetFirstTupleRid(&temp) == true){
        *cur_rowid = temp;
        row->SetRowId(*cur_rowid);
        table_heap->GetTuple(row,nullptr);
        flag = true; //mark that the next tuple has been found 
        break;
      }
      else
        next_page_id = table_page->GetNextPageId(); //continue the loop
    }
  }
  //if not found next valid tuple
  if(!flag){
    *cur_rowid = INVALID_ROWID;
    row->SetRowId(INVALID_ROWID);
    table_heap->GetTuple(row,nullptr);
  }
  //return the next tuple value
  return *this;
}
