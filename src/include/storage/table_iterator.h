#ifndef MINISQL_TABLE_ITERATOR_H
#define MINISQL_TABLE_ITERATOR_H

#include "common/rowid.h"
#include "record/row.h"
#include "transaction/transaction.h"
#include "storage/table_heap.h"


class TableHeap;
 
class TableIterator {
  friend class TableHeap;
public:
  // you may define your own constructor based on your member variables
  TableIterator();

  TableIterator(TableHeap *table_heap,page_id_t page_id);
  
  // explicit TableIterator(page_id_t &pageid, RowId &rowid);

  TableIterator(const TableIterator &other);

  virtual ~TableIterator();

  inline bool operator==(const TableIterator &itr) const;

   bool operator!=(const TableIterator &itr) const;

  const Row &operator*();

  Row *operator->();

  TableIterator &operator++();

  TableIterator &operator++(int);

  RowId GetRowId(){return *cur_rowid;};

  TableHeap *table_heap;
private:
  // add your own private member variables here
  page_id_t cur_page_id;
  RowId *cur_rowid;
  Row *row;
};

#endif //MINISQL_TABLE_ITERATOR_H
