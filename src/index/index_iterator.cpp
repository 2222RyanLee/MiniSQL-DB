// #include "index/basic_comparator.h"
// #include "index/generic_key.h"
// #include "index/index_iterator.h"

// INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::IndexIterator(int index,BufferPoolManager* bufferPoolManager,Page* page) {
//     //在构造函数里首先进行初始化，然后强转生成leaf—page
//     this->index_ = index;
//     this->bufferPoolManager_ = bufferPoolManager;
//     this->page_ = page;
//     this->leaf_page = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *> (this->page_->GetData());
//     //在这一步强转之后，leaf——page里就可以调用具体的BPlusLeafPage进行迭代了
// }

// INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::~IndexIterator() {
//     this->bufferPoolManager_->UnpinPage(this->page_->GetPageId(), false);
//     //问题：析构时，这块page是脏的吗
// }

// INDEX_TEMPLATE_ARGUMENTS const MappingType &INDEXITERATOR_TYPE::operator*() {//const意味着无法通过迭代器进行属性的变更
//   return leaf_page->GetItem(this->index_);//返回指定叶子节点下标的pair
//   //ASSERT(false, "Not implemented yet.");
// }

// INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
//   //分情况进行讨论，如果该叶子节点已经顺序遍历完了，就跳到next page，且将下标置为0，要不然就继续遍历
//   if(this->index_==this->leaf_page->GetSize()-1 && this->leaf_page->GetNextPageId()!=INVALID_PAGE_ID){//也就是当前节点遍历完了且下一个page是存在的
//     auto * page = this->bufferPoolManager_->FetchPage(this->leaf_page->GetNextPageId());
//     if(page!= nullptr){//其实上面的判断已经保证了不会是空的
//       auto* next_page = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(page->GetData());
//       this->bufferPoolManager_->UnpinPage(this->page_->GetPageId(),false);//将现在这块解除pin状态
//       this->page_ = page;
//       this->leaf_page = next_page;//分别进行赋值
//       this->index_ = 0;
//     }
//   }
//   else this->index_++;
//   return *this;
//   //ASSERT(false, "Not implemented yet.");
// }

// INDEX_TEMPLATE_ARGUMENTS
// bool INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const {
//   if(this->leaf_page->GetPageId()==itr.leaf_page->GetPageId()&&this->index_==itr.index_)//如果这两个属性都相同，那么说明是同一个
//     return true;
//   else
//     return false;
// }

// INDEX_TEMPLATE_ARGUMENTS
// bool INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const {
//   if(*this ==itr)
//     return false;
//   else
//     return true;
// }

// template
// class IndexIterator<int, int, BasicComparator<int>>;

// template
// class IndexIterator<GenericKey<4>, RowId, GenericComparator<4>>;

// template
// class IndexIterator<GenericKey<8>, RowId, GenericComparator<8>>;

// template
// class IndexIterator<GenericKey<16>, RowId, GenericComparator<16>>;

// template
// class IndexIterator<GenericKey<32>, RowId, GenericComparator<32>>;

// template
// class IndexIterator<GenericKey<64>, RowId, GenericComparator<64>>;





#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "index/index_iterator.h"

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::IndexIterator() {
  index_ = 0;
  leaf_page = nullptr;
};
INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::IndexIterator(int index,BufferPoolManager* bufferPoolManager,Page* page) {
    //在构造函数里首先进行初始化，然后强转生成leaf—page
    this->index_ = index;
    this->bufferPoolManager_ = bufferPoolManager;
    this->page_ = page;
    this->leaf_page = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *> (this->page_->GetData());
    //在这一步强转之后，leaf——page里就可以调用具体的BPlusLeafPage进行迭代了
}
INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::~IndexIterator() {
  index_ = 0;
  leaf_page = nullptr;
}

INDEX_TEMPLATE_ARGUMENTS const MappingType &INDEXITERATOR_TYPE::operator*() {
  return leaf_page->GetItem(index_);
}

INDEX_TEMPLATE_ARGUMENTS 
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  if(index_ == leaf_page->GetSize())
  {
    index_ = 1;
    auto *page = bufferPoolManager_->FetchPage(leaf_page->GetNextPageId());
    if(page != nullptr)
    {
      B_PLUS_TREE_LEAF_PAGE_TYPE *next = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *> (page->GetData());
      leaf_page = next;
      bufferPoolManager_->UnpinPage(leaf_page->GetPageId(),true);
    }
  }
  else
  {
    index_++;
  }
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const {
  if(leaf_page == itr.leaf_page)
    if(index_ == itr.index_)
      return true;
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const {
  if(leaf_page == itr.leaf_page)
    if(index_ == itr.index_)
      return false;
  return true;
}

template
class IndexIterator<int, int, BasicComparator<int>>;

template
class IndexIterator<GenericKey<4>, RowId, GenericComparator<4>>;

template
class IndexIterator<GenericKey<8>, RowId, GenericComparator<8>>;

template
class IndexIterator<GenericKey<16>, RowId, GenericComparator<16>>;

template
class IndexIterator<GenericKey<32>, RowId, GenericComparator<32>>;

template
class IndexIterator<GenericKey<64>, RowId, GenericComparator<64>>;
