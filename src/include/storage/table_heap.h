#ifndef MINISQL_TABLE_HEAP_H
#define MINISQL_TABLE_HEAP_H

#include "buffer/buffer_pool_manager.h"
#include "page/table_page.h"
#include "storage/table_iterator.h"
#include "transaction/log_manager.h"
#include "transaction/lock_manager.h"
#include "storage/table_iterator.h"
class TableIterator;
class TableHeap {
  friend class TableIterator;//定义迭代器

public:
  static TableHeap *Create(BufferPoolManager *buffer_pool_manager, Schema *schema, Transaction *txn,
                           LogManager *log_manager, LockManager *lock_manager, MemHeap *heap) {
    void *buf = heap->Allocate(sizeof(TableHeap));
    return new(buf) TableHeap(buffer_pool_manager, schema, txn, log_manager, lock_manager);
  }//用schma 和 txn创建

  static TableHeap *Create(BufferPoolManager *buffer_pool_manager, page_id_t first_page_id, Schema *schema,
                           LogManager *log_manager, LockManager *lock_manager, MemHeap *heap) {
    void *buf = heap->Allocate(sizeof(TableHeap));
    return new(buf) TableHeap(buffer_pool_manager, first_page_id, schema, log_manager, lock_manager);
  }//用logic pageid 和 schema创建

  ~TableHeap() {}

  /**
   * Insert a tuple into the table. If the tuple is too large (>= page_size), return false.
   * @param[in/out] row Tuple Row to insert, the rid of the inserted tuple is wrapped in object row
   * @param[in] txn The transaction performing the insert
   * @return true iff the insert is successful
   */
  // 
  bool InsertTuple(Row &row, Transaction *txn);//插入一条记录

  /**
   * Mark the tuple as deleted. The actual delete will occur when ApplyDelete is called.
   * @param[in] rid Resource id of the tuple of delete
   * @param[in] txn Transaction performing the delete
   * @return true iff the delete is successful (i.e the tuple exists)
   */
  bool MarkDelete(const RowId &rid, Transaction *txn);//按照rowid标注删除

  /**
   * if the new tuple is too large to fit in the old page, return false (will delete and insert)
   * @param[in] row Tuple of new row
   * @param[in] rid Rid of the old tuple
   * @param[in] txn Transaction performing the update
   * @return true is update is successful.
   * 用int区分不同错误的类型
   */
  bool UpdateTuple(const Row &row, const RowId &rid, Transaction *txn);//true 是更新成功

  /**
   * Called on Commit/Abort to actually delete a tuple or rollback an insert.
   * @param rid Rid of the tuple to delete
   * @param txn Transaction performing the delete.
   */
  void ApplyDelete(const RowId &rid, Transaction *txn);//删除标注的删除记录

  /**
   * Called on abort to rollback a delete.
   * @param[in] rid Rid of the deleted tuple.
   * @param[in] txn Transaction performing the rollback
   */
  void RollbackDelete(const RowId &rid, Transaction *txn);//恢复标注删除的记录

  /**
   * Read a tuple from the table.
   * @param[in/out] row Output variable for the tuple, row id of the tuple is wrapped in row
   * @param[in] txn transaction performing the read
   * @return true if the read was successful (i.e. the tuple exists)
   */
  bool GetTuple(Row *row, Transaction *txn);//读取记录

  /**
   * Free table heap and release storage in disk file
   */
  void FreeHeap();//删除堆结构，释放磁盘内存

  /**
   * @return the begin iterator of this table
   */
  TableIterator Begin();//迭代器头

  /**
   * @return the end iterator of this table
   */
  TableIterator End();//迭代器尾

  /**
   * @return the id of the first page of this table
   */
  inline page_id_t GetFirstPageId() const { return first_page_id_; }//堆中有多个数据页，返回第一个页
 
private: 
  /**
   * create table heap and initialize first page
   */
  explicit TableHeap(BufferPoolManager *buffer_pool_manager, Schema *schema, Transaction *txn,
                     LogManager *log_manager, LockManager *lock_manager) :
          buffer_pool_manager_(buffer_pool_manager),
          schema_(schema),
          log_manager_(log_manager),
          lock_manager_(lock_manager) {
    buffer_pool_manager->NewPage(first_page_id_);//从buffer pool manager里面分配一个并返回一个干净的逻辑页号
    auto page=reinterpret_cast<TablePage *>(buffer_pool_manager->FetchPage(first_page_id_));
    page->Init(first_page_id_, INVALID_PAGE_ID,log_manager,txn);
    buffer_pool_manager_->UnpinPage(first_page_id_,true);
  };

  /**
   * load existing table heap by first_page_id
   */
  explicit TableHeap(BufferPoolManager *buffer_pool_manager, page_id_t first_page_id, Schema *schema,
                     LogManager *log_manager, LockManager *lock_manager)
          : buffer_pool_manager_(buffer_pool_manager),
            first_page_id_(first_page_id),
            schema_(schema),
            log_manager_(log_manager),
            lock_manager_(lock_manager) {}

private:
  BufferPoolManager *buffer_pool_manager_;//调用缓存区管理模块
  page_id_t first_page_id_;//第一页的逻辑号
  Schema *schema_;//Row里面的信息结合Schema信息可以打ASSERT做一些调试验证
  [[maybe_unused]] LogManager *log_manager_;//
  [[maybe_unused]] LockManager *lock_manager_;
};

#endif  // MINISQL_TABLE_HEAP_H
