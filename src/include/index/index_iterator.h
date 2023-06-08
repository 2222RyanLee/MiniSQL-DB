#ifndef MINISQL_INDEX_ITERATOR_H
#define MINISQL_INDEX_ITERATOR_H

#include "page/b_plus_tree_leaf_page.h"

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
public:
  // you may define your own constructor based on your member variables
  explicit IndexIterator(int index,BufferPoolManager* bufferPoolManager,Page* page);
  explicit IndexIterator();

  ~IndexIterator();

  /** Return the key/value pair this iterator is currently pointing at. */
  const MappingType &operator*();

  /** Move to the next key/value pair.*/
  IndexIterator &operator++();

  /** Return whether two iterators are equal */
  bool operator==(const IndexIterator &itr) const;

  /** Return whether two iterators are not equal. */
  bool operator!=(const IndexIterator &itr) const;


  void SetIndex(int index){index_ = index;}
  void SetBuffer(BufferPoolManager *buffer){bufferPoolManager_ = buffer;}
  void SetLeaf(B_PLUS_TREE_LEAF_PAGE_TYPE *leaf){ leaf_page = leaf;}
  B_PLUS_TREE_LEAF_PAGE_TYPE *GetLeaf(){ return leaf_page;}

private:
  // add your own private member variables here
      Page* page_;//首先取出page，然后对page进行强制类型转换为leaf_page然后进行操作
      BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *leaf_page;//强制类型转换完的leaf_page
      BufferPoolManager *bufferPoolManager_;
      int index_;
      // B_PLUS_TREE_LEAF_PAGE_TYPE *this_leaf;
};


#endif //MINISQL_INDEX_ITERATOR_H