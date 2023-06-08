// #include "index/basic_comparator.h"
// #include "index/generic_key.h"
// #include "page/b_plus_tree_internal_page.h"


// /*****************************************************************************
//  * HELPER METHODS AND UTILITIES
//  *****************************************************************************/
// /*
//  * Init method after creating a new internal page
//  * Including set page type, set current size, set page id, set parent id and set
//  * max page size
//  */
// INDEX_TEMPLATE_ARGUMENTS
// void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
//   SetPageType(IndexPageType::INTERNAL_PAGE);
//   SetPageId(page_id);
//   SetParentPageId(parent_id);
//   SetMaxSize(max_size);
//   SetSize(0);
// }
// /*
//  * Helper method to get/set the key associated with input "index"(a.k.a
//  * array offset)
//  */
// INDEX_TEMPLATE_ARGUMENTS
// KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
//   KeyType key = array_[index].first;
//   return key;
// }

// INDEX_TEMPLATE_ARGUMENTS
// void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
//   array_[index].first = key;
// }

// /*
//  * Helper method to find and return array index(or offset), so that its value
//  * equals to input "value"
//  */
// INDEX_TEMPLATE_ARGUMENTS
// int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {//寻找值为value所对应的下标
//   int i = 0;
//   while (i<GetSize() && value != array_[i].second){
//     i++;
//   }
//   return i;
// }

// /*
//  * Helper method to get the value associated with input "index"(a.k.a array
//  * offset)
//  */
// INDEX_TEMPLATE_ARGUMENTS
// ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const {//寻找数组下标对应的value
//   return array_[index].second;
// }

// /*****************************************************************************
//  * LOOKUP
//  *****************************************************************************/
// /*
//  * Find and return the child pointer(page_id) which points to the child page
//  * that contains input "key"
//  * Start the search from the second key(the first key should always be invalid)
//  */
// //----------------------------perfect now-------------------------------
// INDEX_TEMPLATE_ARGUMENTS
// ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
//   ValueType val{};
//   int i = 2;
//   for(; i < GetSize()+1; i++)
//     if(comparator(key,array_[i].first) < 0)
//     {
//       val = array_[i-1].second;
//       break;
//     }
//   if(i == GetSize()+1)
//     val = array_[GetSize()].second;
//   return val;
// }

// /*****************************************************************************
//  * INSERTION
//  *****************************************************************************/
// /*
//  * Populate new root page with old_value + new_key & new_value
//  * When the insertion cause overflow from leaf page all the way upto the root
//  * page, you should create a new root page and populate its elements.
//  * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
//  */
// //----------------------------perfect---------------------------
// INDEX_TEMPLATE_ARGUMENTS
// void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key, const ValueType &new_value){
//   array_[1].second = old_value;
//   array_[2] = make_pair(new_key,new_value);
//   SetSize(2);
// }

// /*
//  * Insert new_key & new_value pair right after the pair with its value == old_value------------------ok
//  * @return:  new size after insertion----------------------------------------------------------------ok
//  */
// INDEX_TEMPLATE_ARGUMENTS
// int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key, const ValueType &new_value){
//   if(old_value == array_[GetSize()].second)//last_one
//     array_[GetSize()+1] = make_pair(new_key,new_value);
//   else 
//   {
//     for(int i = 1;i <= GetSize();i++){//find i
//       if(array_[i].second == old_value){
//         for(int j = GetSize()+1; j > i+1; j--)
//           array_[j] = array_[j-1];
//         array_[i+1] = make_pair(new_key,new_value);
//         break;
//       }
//     }
//   }
//   IncreaseSize(1);//size+1
//   return GetSize();
// }

// /*****************************************************************************
//  * SPLIT
//  *****************************************************************************/
// /*
//  * Remove half of key & value pairs from this page to "recipient" page----------------ok
//  */
// INDEX_TEMPLATE_ARGUMENTS
// void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient, BufferPoolManager *buffer_pool_manager) {
//   int min = (GetMaxSize()+2)/2;
//   int size = GetSize() - min;
//   for(int i = 1; i <= size; i++)
//   {
//     recipient->array_[i] = array_[i+min];
//     recipient->IncreaseSize(1);
//     IncreaseSize(-1);
//     auto *page = buffer_pool_manager->FetchPage(recipient->array_[i].second);
//     if(page!=nullptr)
//       {
//         BPlusTreePage *sub_node = reinterpret_cast<BPlusTreePage *>(page->GetData());//change type
//         sub_node->SetParentPageId(recipient->GetPageId());
//         buffer_pool_manager->UnpinPage(recipient->array_[i].second,true);
//       }
//   }
// }
// /* Copy entries into me, starting from {items} and copy {size} entries.
//  * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
//  * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
//  */
// INDEX_TEMPLATE_ARGUMENTS
// void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {//插N个mappingtype到最后
//   for (int i = 1;i<size+1;i++){
//     array_[i+GetSize()] = *items;
//     auto * page = buffer_pool_manager->FetchPage(array_[i+GetSize()].second); 
//     //**********取每一个value，这里不知道bufferpool里面的page_id是如何对应index的,这里先假设value为1的时候page_id也为1
//     //这里因为move之后孩子结点的parent id 变掉了，要读取这些页，重新写parent id
    
//     ASSERT(page!=nullptr,"the index is null"); //判断是否成功fetch
//     auto * child = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());//通过reinterpret_cast将Page中的data_重新解释
//     child->SetParentPageId(this->GetPageId()); //重新设置孩子结点的父节点id
//     buffer_pool_manager->UnpinPage(array_[i+GetSize()].second, true);//把fetch的unpin掉
//     items++; //到下一个mappingtype
//   }
//   IncreaseSize(size);
// }

// /*****************************************************************************
//  * REMOVE
//  *****************************************************************************/
// /*
//  * Remove the key & value pair in internal page according to input index(a.k.a
//  * array offset)
//  * NOTE: store key&value pair continuously after deletion
//  */
// INDEX_TEMPLATE_ARGUMENTS
// void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {//清除数组下标的kv
//   int size = GetSize();
//   for(int i = index;i<size;i++){
//     array_[i]=array_[i+1];
//   }
//   IncreaseSize(-1);
//   return;
// }



// /*
//  * Remove the only key & value pair in internal page and return the value
//  * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
//  */
// INDEX_TEMPLATE_ARGUMENTS
// ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {//只有在还有一个key的时候使用，返回第一个value
//   ValueType val = array_[0].second;
//   int size = GetSize();
//   this->SetSize(size--);
//   return val;
// }

// /*****************************************************************************
//  * MERGE
//  *****************************************************************************/
// /*
//  * Remove all of key & value pairs from this page to "recipient" page. ---------------------------------ok
//  * The middle_key is the separation key you should get from the parent. --------------------------------ok
//  * You need to make sure the middle key is added to the recipient to maintain the invariant.------------ok
//  * You also need to use BufferPoolManager to persist changes to the parent page id for those
//  * pages that are moved to the recipient----------------------------------------------------------------ok
//  */
// INDEX_TEMPLATE_ARGUMENTS
// void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
//                                                BufferPoolManager *buffer_pool_manager) {
//   array_[1].first = middle_key;
//   for(int i = 1; i <= GetSize(); i++)
//   {
//     MappingType p = array_[i];
//     recipient->CopyLastFrom(p,buffer_pool_manager);
//   }
//   SetSize(0);
// }

// /*****************************************************************************
//  * REDISTRIBUTE
//  *****************************************************************************/
// /*
//  * Remove the first key & value pair from this page to tail of "recipient" page.
//  *
//  * The middle_key is the separation key you should get from the parent. You need
//  * to make sure the middle key is added to the recipient to maintain the invariant.
//  * You also need to use BufferPoolManager to persist changes to the parent page id for those
//  * pages that are moved to the recipient
//  */
// INDEX_TEMPLATE_ARGUMENTS
// void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
//                                                       BufferPoolManager *buffer_pool_manager) {
//   auto * child = buffer_pool_manager->FetchPage(this->array_[0].second);
//   ASSERT(child != nullptr,"error! the page is null");  //确保page有效
//   auto * temp = reinterpret_cast<BPlusTreeInternalPage *>(child->GetData());
//   temp->SetParentPageId(recipient->GetPageId()); //把孩子结点的pageid更新为recipient
//   auto * parent = buffer_pool_manager->FetchPage(this->GetParentPageId()); //修改父节点
//   ASSERT(parent != nullptr,"error! the page is null");  //确保page有效
//   const MappingType pair = {array_[1].first,array_[0].second}; //不需要判断，直接把第一个key作为父节点的分隔值
//   temp->CopyLastFrom(pair,buffer_pool_manager);
//   buffer_pool_manager->UnpinPage(child->GetPageId(), true);
//   array_[0].second = array_[1].second;
//   Remove(1); //清除第一个
//   recipient->CopyLastFrom(pair, buffer_pool_manager);
//   buffer_pool_manager->UnpinPage(parent->GetPageId(), true); 
// }

// /* Append an entry at the end.---------------------------------------------------------------------------------------ok
//  * Since it is an internal page, the moved entry(page)'s parent needs to be updated.---------------------------------ok
//  */
// INDEX_TEMPLATE_ARGUMENTS
// void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
// //insert
//   array_[GetSize()+1] = pair;
//   IncreaseSize(1);
// //change p_id
//   auto *page = buffer_pool_manager->FetchPage(pair.second);
//   if(page != nullptr)
//   {
//     BPlusTreePage *subnode = reinterpret_cast<BPlusTreePage *>(page->GetData());
//     subnode->SetParentPageId(GetPageId());
//     buffer_pool_manager->UnpinPage(pair.second,true);
//   }
// }

// INDEX_TEMPLATE_ARGUMENTS
// void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
//                                                        BufferPoolManager *buffer_pool_manager) {//向this的最后一个借一个pair传入recipient当中，recipient的第一个value并不改变s
//   auto * child = buffer_pool_manager->FetchPage(this->array_[GetSize()].second);
//   ASSERT(child != nullptr,"error! the page is null");  //确保page有效
//   auto * temp = reinterpret_cast<BPlusTreeInternalPage *>(child->GetData());
//   temp->SetParentPageId(recipient->GetPageId()); //把孩子结点的pageid更新为recipient
//   const MappingType pair = this->array_[GetSize()];//取最后一个结点作为pair
//   temp->CopyFirstFrom(pair,buffer_pool_manager);
//   buffer_pool_manager->UnpinPage(child->GetPageId(), true);
//   this->IncreaseSize(-1);
// }


// INDEX_TEMPLATE_ARGUMENTS
// void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {//把pair添加到该结点的第一个结点，并且通过buffer_manager去更新父节点

//   this->InsertNodeAfter(array_[0].second,pair.first,pair.second);//pair.first在array[1],原来的array[0].second不改变
  
//   auto * parent = buffer_pool_manager->FetchPage(this->GetParentPageId()); //取出这个page
//   ASSERT(parent != nullptr,"error! the page is null");  //确保page有效
//   auto *  temp = reinterpret_cast<BPlusTreeInternalPage *>(parent->GetData());
  
//   auto v = this->GetPageId();
//   int index = temp->ValueIndex(v);//在父节点中找到对应的index
//   temp->SetKeyAt(index,pair.first);//改变父节点对应的key
//   buffer_pool_manager->UnpinPage(parent->GetPageId(),true); //unpin修改过的
// }

// template
// class BPlusTreeInternalPage<int, int, BasicComparator<int>>;

// template
// class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;

// template
// class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;

// template
// class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;

// template
// class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;

// template
// class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;

#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/b_plus_tree_internal_page.h"


/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetSize(0);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  KeyType key = array_[index].first;
  return key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  array_[index].first = key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  for( int i = 1; i <= GetSize(); i++ )
    if(array_[i].second == value)
      return i;
  return 0;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const {
  ValueType val = array_[index].second;
  return val;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
//----------------------------perfect now-------------------------------
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
  ValueType val{};
  int i = 2;
  for(; i < GetSize()+1; i++)
    if(comparator(key,array_[i].first) < 0)
    {
      val = array_[i-1].second;
      break;
    }
  if(i == GetSize()+1)
    val = array_[GetSize()].second;
  return val;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
//----------------------------perfect---------------------------
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key, const ValueType &new_value){
  array_[1].second = old_value;
  array_[2] = make_pair(new_key,new_value);
  SetSize(2);
}

/*
 * Insert new_key & new_value pair right after the pair with its value == old_value------------------ok
 * @return:  new size after insertion----------------------------------------------------------------ok
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key, const ValueType &new_value){
  if(old_value == array_[GetSize()].second)//last_one
    array_[GetSize()+1] = make_pair(new_key,new_value);
  else 
  {
    int i = 1;
    for(;i <= GetSize();i++)//find i
      if(array_[i].second == old_value)
        break;
    i++;//after_the_pair
    for(int j = GetSize()+1; j > i; j--)
      array_[j] = array_[j-1];
    array_[i] = make_pair(new_key,new_value);
  }
  IncreaseSize(1);//size+1
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page----------------ok
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient, BufferPoolManager *buffer_pool_manager) {
  int min = (GetMaxSize()+2)/2;
  int size = GetSize() - min;
  for(int i = 1; i <= size; i++)
  {
    recipient->array_[i] = array_[i+min];
    recipient->IncreaseSize(1);
    IncreaseSize(-1);
    auto *page = buffer_pool_manager->FetchPage(recipient->array_[i].second);
    if(page!=nullptr)
      {
        BPlusTreePage *sub_node = reinterpret_cast<BPlusTreePage *>(page->GetData());//change type
        sub_node->SetParentPageId(recipient->GetPageId());
        buffer_pool_manager->UnpinPage(recipient->array_[i].second,true);
      }
  }
}

/* Copy entries into me, starting from {items} and copy {size} entries.-----------------------------------------------------ok
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.-----------------------ok
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger------------ok
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  // me->SetSize(0);
  // for(int i = 0; i < GetSize(); i++)
  // {
  //   if( &array_[i] == items )//begin.
  //   {
  //     for(int j = 0; j < size; j++)
  //     {
  //       me->InsertNodeAfter(me->array_[me->GetSize()],array_[i+j].first,array_[i+j].second);
  //       auto *s_page = buffer_pool_manager->FetchPage(array_[j+i].second);
  //       if(s_page!=nullptr)
  //       {
  //         BPlusTreeInternalPage *sub_node = reinterpret_cast<BPlusTreeInternalPage *>(s_page->GetData());//change type
  //         sub_node->SetParentPageId(me->GetPageId());
  //         buffer_pool_manager->UnpinPage(sub_node->GetPageId(),true);
  //       }
  //     }
  //     break;
  //   }
  // }
  // return;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a array offset)--------ok
 * NOTE: store key&value pair continuously after deletion-------------------------------------------ok
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  for(int i = index; i < GetSize(); i++)
    array_[i] = array_[i+1];
  IncreaseSize(-1);
  return;
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  ValueType val{};
  val = array_[1].second;
  SetSize(0);
  return val;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page. ---------------------------------ok
 * The middle_key is the separation key you should get from the parent. --------------------------------ok
 * You need to make sure the middle key is added to the recipient to maintain the invariant.------------ok
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient----------------------------------------------------------------ok
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) {
  array_[1].first = middle_key;
  for(int i = 1; i <= GetSize(); i++)
  {
    MappingType p = array_[i];
    recipient->CopyLastFrom(p,buffer_pool_manager);
  }
  SetSize(0);
}









/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.------------------------ok,but first key is in parent.
 * The middle_key is the separation key you should get from the parent. --------------------------------ok
 * You need to make sure the middle key is added to the recipient to maintain the invariant.------------ok
 * You also need to use BufferPoolManager to persist changes 
 * to the parent page id for those pages that are moved to the recipient--------------------------------ok
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager) {
//step0: get the first pair.
  MappingType p = make_pair(middle_key,array_[1].second);
  Remove(1);//remove_the_first_pair, new array_[1].first will be used for redistribute.
//step1: insert_to_tail.
  recipient->CopyLastFrom(p,buffer_pool_manager);
}

/* Append an entry at the end.---------------------------------------------------------------------------------------ok
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.---------------------------------ok
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
//insert
  array_[GetSize()+1] = pair;
  IncreaseSize(1);
//change p_id
  auto *page = buffer_pool_manager->FetchPage(pair.second);
  if(page != nullptr)
  {
    BPlusTreePage *subnode = reinterpret_cast<BPlusTreePage *>(page->GetData());
    subnode->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(pair.second,true);
  }
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.--------------------------------------------ok
 * You need to handle the original dummy key properly,---------------------------------------------------------------------ok
 *  e.g. updating recipient’s array to position the middle_key at the right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient--------------------------------------------------------------------------------------------------ok
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                       BufferPoolManager *buffer_pool_manager) {
//get_and_remove  
  MappingType pair = array_[GetSize()];//pair first will be used for parent node.
  recipient->array_[1].first = middle_key;
  Remove(GetSize());
//append_an_entry_at_the_beginning.
  recipient->CopyFirstFrom(pair,buffer_pool_manager);
}

/* Append an entry at the beginning.-------------------------------------------------------------------------------ok
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.-------------------------------ok
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
//append
  for(int i = GetSize()+1; i > 1; i--)
    array_[i] = array_[i-1];
  array_[1] = pair;
  IncreaseSize(1);
//persist_change
  auto *page = buffer_pool_manager->FetchPage(pair.second);
  if(page != nullptr)
  {
    BPlusTreePage *subnode = reinterpret_cast<BPlusTreePage *>(page->GetData());
    subnode->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(pair.second,true);
  }
}

template
class BPlusTreeInternalPage<int, int, BasicComparator<int>>;

template
class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;

template
class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;

template
class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;

template
class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;

template
class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;