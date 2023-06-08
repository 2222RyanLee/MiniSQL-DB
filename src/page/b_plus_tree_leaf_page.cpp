#include <algorithm>
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/b_plus_tree_leaf_page.h"

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type,-------------------ok
 * set current size to zero, -----------------ok
 * set page id/parent id, --------------------ok
 * set max size-------------------------------ok
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id-------------ok
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const {
  return next_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
  //return;
}

/**
 * Helper method to find the first index i so that array_[i].first >= key---------------ok
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const {
  int i = 1;
  for(; i <= GetSize(); i++)
  {
    if(comparator(array_[i].first,key) >= 0)break;
  }
  return i;
}

/*
 * Helper method to find and return the key associated with input "index"----------ok
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  KeyType key{};
  key = array_[index].first;
  return key;
}

/*
 * Helper method to find and return the key & value pair associated with input----------ok
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
  return array_[index];
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key----------------ok
 * @return page size after insertion------------------------------------ok
 */
//--------------------------------perfect-------------------------------
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
  if(GetSize() == 0)
    array_[1] = make_pair(key,value);
  else
    for(int i = 1; i <= GetSize(); i++)
    {
      if(comparator(key,array_[i].first) < 0)//找到了目标节点
      {
        for(int j = GetSize()+1; j > i; j--) array_[j] = array_[j-1];
        array_[i] = make_pair(key,value);
        break;
      }
      else if(comparator(key,array_[i].first) > 0)
      {
        if(i == GetSize())
          array_[i+1] = make_pair(key,value);
      } 
      else//该处已经有了插入
        return GetSize();
    }
  IncreaseSize(1);
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page------ok
 */
//---------------------------------------------perfect----------------------------
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient) {
  int min = (GetMaxSize()+2)/2;
  int size = GetSize();
  for(int i = min+1; i <= size; i++)
  {
    recipient->array_[i-min] = array_[i];
    recipient->IncreaseSize(1);
    IncreaseSize(-1);
  }
  recipient->next_page_id_ = next_page_id_;
  next_page_id_ = recipient->GetPageId();
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *items, int size) {

    for(int i=this->GetSize()+1, t = 0;i<=this->GetSize()+size;i++,t++){//复制size个pair到本地的array中
      this->array_[i] = *(items + t) ;
    }
    IncreaseSize(size);//本地array的数量增加了size个
    //疑问，这里要把自身的移走的item删除吗
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. ----------------ok
 * If it does, then store its corresponding value in input "value" and return true.----ok
 * If the key does not exist, then return false----------------------------------------ok
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType &value, const KeyComparator &comparator) const {
  for(int i = 1; i <= GetSize(); i++)
    if(comparator(key,array_[i].first) > 0 || comparator(key,array_[i].first) < 0)
    {}else
    {
      value = array_[i].second;
      return true;
    }
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not.---------ok
 * If exist, perform deletion, otherwise return immediately.--------------------ok
 * NOTE: store key&value pair continuously after deletion-----------------------ok
 * @return  page size after deletion--------------------------------------------ok
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator) {
  int size = GetSize();
  for(int i = 1; i <= size; i++)
  {
    if(comparator(key,array_[i].first) == 0)
    {
      for(int j = i; j < GetSize(); j++)
        array_[j] = array_[j+1];
      IncreaseSize(-1);
      return GetSize();
    }
  }
  return GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.--------ok
 * Don't forget to update the next_page id in the sibling page.---------------ok
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient) {
  for(int i = 1; i <= GetSize(); i++)
  {
    recipient->array_[recipient->GetSize()+1] = array_[i];
    recipient->IncreaseSize(1);
  }
  SetSize(0);
  recipient->next_page_id_ = next_page_id_;
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.------ok
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient) {
  
  MappingType pair = array_[1];
  
  for(int i = 1; i < GetSize(); i++)
    array_[i] = array_[i+1];
  IncreaseSize(-1);

  recipient->array_[recipient->GetSize()+1] = pair;
  recipient->SetSize(recipient->GetSize()+1);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {//当使用&引用时，是一个具体的pair，当使用指针*时
    array_[this->GetSize()+1] = item;                                     //可能是某一个地址的开始
    this->IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.---------ok
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient) {
  MappingType pair = array_[GetSize()];
  
  IncreaseSize(-1);

  for(int i = recipient->GetSize()+1; i > 1; i-- )
    recipient->array_[i] = recipient->array_[i-1];
  recipient->array_[1] = pair;
  recipient->SetSize(recipient->GetSize()+1);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 *
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item) {
    for(int i = this->GetSize() +1;i>1;i--){
        this->array_[i] = this->array_[i-1];
    }
    this->array_[1] = item;
    this->IncreaseSize(1);
}

template
class BPlusTreeLeafPage<int, int, BasicComparator<int>>;

template
class BPlusTreeLeafPage<GenericKey<4>, RowId, GenericComparator<4>>;

template
class BPlusTreeLeafPage<GenericKey<8>, RowId, GenericComparator<8>>;

template
class BPlusTreeLeafPage<GenericKey<16>, RowId, GenericComparator<16>>;

template
class BPlusTreeLeafPage<GenericKey<32>, RowId, GenericComparator<32>>;

template
class BPlusTreeLeafPage<GenericKey<64>, RowId, GenericComparator<64>>;