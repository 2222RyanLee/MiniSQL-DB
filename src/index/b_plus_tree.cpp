#include <string>
#include "glog/logging.h"
#include "index/b_plus_tree.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"
using namespace std;
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
        : index_id_(index_id),
          buffer_pool_manager_(buffer_pool_manager),
          comparator_(comparator),
          leaf_max_size_(leaf_max_size-1),
          internal_max_size_(internal_max_size) {
  IndexRootsPage *root_page = reinterpret_cast<IndexRootsPage *>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID));//取rootpage
  page_id_t *find_id = nullptr;//在这里要分两种情况讨论
  find_id = new(page_id_t);
  if(root_page->GetRootId(index_id,find_id)){//如果找到了，通过传入的page_id在index_roots_page当中获取到page_id
    root_page_id_ = *find_id;
  }
  else{//如果没有在rootpage当中找到
  page_id_t page_id;
  Page * page = buffer_pool_manager->NewPage(page_id);//分配一个root_page_id
  root_page_id_ = page_id; 
  BPlusTreeLeafPage<KeyType,ValueType,KeyComparator> * temp = reinterpret_cast<BPlusTreeLeafPage<KeyType,ValueType,KeyComparator> *>(page->GetData());
  temp->Init(page_id,INVALID_PAGE_ID,leaf_max_size-1);//初始化并设父结点为invalid
  UpdateRootPageId(0); //新插入一条
  buffer_pool_manager->UnpinPage(page_id,true);
  }
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID,false);  
}

//not finished yet.
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Destroy() {
  buffer_pool_manager_->DeletePage(root_page_id_);
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const {
  if(root_page_id_ == INVALID_PAGE_ID)
    return true;
  else
    return false;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> &result, Transaction *transaction) {
  page_id_t p_id = root_page_id_;
  while(p_id != INVALID_PAGE_ID)
  {
    auto *page =  buffer_pool_manager_->FetchPage(p_id);
    if(page != nullptr)
    {
      page_id_t old_id = p_id;
      BPlusTreePage* node = reinterpret_cast<BPlusTreePage *> (page->GetData());
      if(node->IsLeafPage())//leaf:look up for key
      {
        LeafPage* leaf = reinterpret_cast<LeafPage *> (page->GetData());
        ValueType v;
        if(leaf->Lookup(key,v,comparator_))//Found.
        {
          result.push_back(v);
          buffer_pool_manager_->UnpinPage(old_id,true);
          return true;
        }
        else//Not Found.
        {
          buffer_pool_manager_->UnpinPage(old_id,true);
          return false;
        }
      }
      else//internal: look up for value
      {
        InternalPage *internal = reinterpret_cast<InternalPage *> (page->GetData());
        p_id = internal->Lookup(key,comparator_);
        buffer_pool_manager_->UnpinPage(old_id,true);
      }
    }
    else
      break;
  }
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert entry.-----ok:1
 * otherwise insert into leaf page.----------------------------------------------------ok:2
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.-------------------------------------------ok:3
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  if(IsEmpty())
  {
    StartNewTree(key,value);//------------------------------------------------------------1
    return true;
  }
  else
  {
    if(InsertIntoLeaf(key,value,transaction))//--------------------------------------2
      return true;
  }
  return false;
}
/*
 * Insert constant key & value pair into an empty tree---------------------------------ok:1
 * User needs to first ask for new page from buffer pool manager-----------------------ok:2
 * (NOTICE: throw an "out of memory" exception if returned value is nullptr),----------ok:3
 * then update b+ tree's root page id and insert entry directly into leaf page.--------ok:4
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  MappingType pair(key,value);
  page_id_t root_page_id;
  auto *page = buffer_pool_manager_->NewPage(root_page_id);//2
  if(root_page_id != INVALID_PAGE_ID)
  {
    root_page_id_ = root_page_id;//4
    if(page != nullptr)
    {
      LeafPage * node = reinterpret_cast<LeafPage *>(page->GetData());
      node->Init(root_page_id_,INVALID_PAGE_ID,leaf_max_size_);
      node->Insert(key,value,comparator_);//1
      buffer_pool_manager_->UnpinPage(root_page_id_,true);
    }
  }
  else
    throw "out of memory";//3

}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the proper leaf page as insertion target, --------------------ok:1
 * then look through leaf page to see whether insert key exist or not. 
 * If exist, return immediately, ---------------------------------------------------------ok:2
 * otherwise insert entry. ---------------------------------------------------------------ok:3
 * Remember to deal with split if necessary.----------------------------------------------ok:4
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  page_id_t p_id = root_page_id_;
  while(p_id != INVALID_PAGE_ID)
  {
    auto *page = buffer_pool_manager_->FetchPage(p_id);
    if(page!=nullptr)
    {
      page_id_t old_id = p_id;
      BPlusTreePage *node = reinterpret_cast<BPlusTreePage *> (page->GetData());
      if(node->IsLeafPage())//leaf: insert
      {
        //cout << "leaf" << endl;
        LeafPage *leaf = reinterpret_cast<LeafPage *> (page->GetData());
        int old_size = leaf->GetSize();
        int new_size = leaf->Insert(key,value,comparator_);
        if(old_size == new_size)//duplicate
        {
          buffer_pool_manager_->UnpinPage(old_id,true);
          return false;//-------------------------------------------------------------------2
        }
        else//maybe split.
        {
          if(new_size > leaf_max_size_)//split----------------------------------------------4
          {
            LeafPage *next_leaf = Split(leaf);
            KeyType middle_key = next_leaf->KeyAt(1);
            InsertIntoParent(leaf,middle_key,next_leaf,transaction);
          }
          buffer_pool_manager_->UnpinPage(old_id,true);
          return true;//--------------------------------------------------------------------3
        }
      }
      else//internal: next_layer
      {
        InternalPage *internal = reinterpret_cast<InternalPage *> (page->GetData());
        p_id = internal->Lookup(key,comparator_);//-----------------------------------------1
        buffer_pool_manager_->UnpinPage(old_id,true);
      }
    }
    else
      break;
  }
    return false;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager---------------ok:1
 * (NOTICE: throw an "out of memory" exception if returned value is nullptr)---ok:2
 * then move half of key & value pairs from input page to newly created page---ok:3
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  page_id_t new_id;
  auto *page = buffer_pool_manager_->NewPage(new_id);//1
  if(new_id==INVALID_PAGE_ID)
    throw "out of memory";//2
  if(node->IsLeafPage())//leaf
  {
    LeafPage *l_node = reinterpret_cast<LeafPage *> (node);
    if(page!=nullptr)
    {
      LeafPage* recipient = reinterpret_cast<LeafPage *> (page->GetData());
      recipient->Init(new_id,l_node->GetParentPageId(),leaf_max_size_);
      l_node->MoveHalfTo(recipient);//3
      buffer_pool_manager_->UnpinPage(new_id,true);
      N* result = reinterpret_cast<N *> (recipient);
      return result;
    }
  }
  else
  {
    InternalPage *i_node = reinterpret_cast<InternalPage *> (node);
    if(page!=nullptr)
    {
      InternalPage* recipient = reinterpret_cast<InternalPage *> (page->GetData());
      recipient->Init(new_id,i_node->GetParentPageId(),internal_max_size_);
      i_node->MoveHalfTo(recipient,buffer_pool_manager_);//3
      buffer_pool_manager_->UnpinPage(new_id,true);
      N* result = reinterpret_cast<N *> (recipient);
      return result;
    }
  }
  return nullptr;
}


/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node,---------------------------------ok:1
 * parent node must be adjusted to take info of new_node into account. ------------------ok:2
 * Remember to deal with split recursively if necessary.---------------------------------ok:3
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  if(old_node->IsRootPage())//build new root
  {
    page_id_t new_r_id;
    auto *page = buffer_pool_manager_->NewPage(new_r_id);
    if(page!=nullptr)
    {
      InternalPage *root = reinterpret_cast<InternalPage *> (page->GetData());
      //initialize root and populate.
      root->Init(new_r_id,INVALID_PAGE_ID,internal_max_size_);
      root->PopulateNewRoot(old_node->GetPageId(),key,new_node->GetPageId());
      buffer_pool_manager_->UnpinPage(new_r_id,true);
    }
    //change parent_id of sub_nodes.
    old_node->SetParentPageId(new_r_id);
    new_node->SetParentPageId(new_r_id);
    //set new root:
    root_page_id_ = new_r_id;
    
  }
  else// insert into parent
  {
    page_id_t parent_id = old_node->GetParentPageId();//-----------------------------------1
    auto *page = buffer_pool_manager_->FetchPage(parent_id);
    if(page != nullptr)
    {
      InternalPage *parent = reinterpret_cast<InternalPage *> (page->GetData());//treat_as_internal
  //step0: insert.
      parent->InsertNodeAfter(old_node->GetPageId(),key,new_node->GetPageId());
      new_node->SetParentPageId(parent_id);
  //step1: if overflow, recursion.
      if(parent->GetSize() > parent->GetMaxSize())
      {
        InternalPage* new_parent = Split(parent);
        KeyType mid_key = new_parent->KeyAt(1);
        InsertIntoParent(parent,mid_key,new_parent,transaction);//------------------------------------------3
      }
      buffer_pool_manager_->UnpinPage(parent_id,true);
    }
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.---------------------------------------------ok:1
 * If not, User needs to first find the right leaf page as deletion target, -----------------ok:2
 * then delete entry from leaf page. --------------------------------------------------------ok:3
 * Remember to deal with redistribute or merge if necessary.---------------------------------ok:4
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if(IsEmpty())
    return;//1
  else
  {
    page_id_t p_id = root_page_id_;
    while(1)
    {
      auto *page =  buffer_pool_manager_->FetchPage(p_id);
      if(page != nullptr)
      {
        page_id_t old_id = p_id;
        BPlusTreePage* node = reinterpret_cast<BPlusTreePage *> (page->GetData());
        if(node->IsLeafPage())//leaf:look up for key
        {
          LeafPage* leaf = reinterpret_cast<LeafPage *> (page->GetData());
          //step 0: judge whether internal key need to be remove.
          if(comparator_(key,leaf->KeyAt(1)) < 0 || comparator_(key,leaf->KeyAt(1)) > 0){}
          else// need to remove internal key.
          {
            KeyType old_key = leaf->KeyAt(1), new_key = leaf->KeyAt(2);
            page_id_t _p_id = root_page_id_;
            while(1)
            {
              auto *_page =  buffer_pool_manager_->FetchPage(_p_id);
              if(_page != nullptr)
              {
                page_id_t _old_id = _p_id;
                BPlusTreePage* node = reinterpret_cast<BPlusTreePage *> (_page->GetData());
                if(node->IsLeafPage())
                {
                  buffer_pool_manager_->UnpinPage(_old_id,true);
                  break;
                }
                else// internal node, maybe delete.
                {
                  InternalPage *internal = reinterpret_cast<InternalPage *> (node);
                  for(int i = 2; i <= internal->GetSize(); i++)
                  {
                    if(comparator_(internal->KeyAt(i),old_key) < 0 || comparator_(internal->KeyAt(i),old_key) > 0){}
                    else
                      internal->SetKeyAt(i,new_key);
                  }
                  _p_id = internal->Lookup(old_key,comparator_);
                  buffer_pool_manager_->UnpinPage(_old_id,true);
                }
              }
              else
                break;
            }
          }
          leaf->RemoveAndDeleteRecord(key,comparator_);//----------------3
          if(leaf->GetSize() < leaf->GetMinSize())
            CoalesceOrRedistribute(leaf,transaction);//-------------------------------4
          buffer_pool_manager_->UnpinPage(old_id,true);
          return;
        }
        else//internal: look up for value-----------------------------------------------2
        {
          InternalPage *internal = reinterpret_cast<InternalPage *> (page->GetData());
          p_id = internal->Lookup(key,comparator_);
          buffer_pool_manager_->UnpinPage(old_id,true);
        }
      }
      else//error.
        break;
    }
  }
}

/*
 * User needs to first find the sibling of input page.----------------------------ok
 * If sibling's size + input page's size > page's max size, then redistribute. 
 * Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  if(node->IsRootPage())//root case
  {
    AdjustRoot(node);
  }
  else if(node->IsLeafPage())// leaf case
  {
//step 0: get parent address:
    LeafPage *l_node = reinterpret_cast<LeafPage *> (node);
    auto *page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
    if(page!=nullptr)
    {
      InternalPage *parent = reinterpret_cast<InternalPage *> (page->GetData());
  //step0.5: find sibling id and index.
      page_id_t sib_id;
      int index = 0;
      int i = parent->ValueIndex(l_node->GetPageId());
      if(i < parent->GetSize())// index == 0
        sib_id = parent->ValueAt(i+1);
      else// index == 1
      {
        index = 1;
        sib_id = parent->ValueAt(parent->GetSize()-1);
      }
//step 1:decide to coalesce or redistribute
      auto *page = buffer_pool_manager_->FetchPage(sib_id);
      if(page!=nullptr)
      {
        LeafPage *sibling = reinterpret_cast<LeafPage *> (page->GetData());
        if(sibling->GetSize() + node->GetSize() > leaf_max_size_)//redistribute.
          Redistribute(sibling,l_node,index);
        else//coalesce
          Coalesce(&sibling,&l_node,&parent,index,transaction);
        buffer_pool_manager_->UnpinPage(sib_id,true);
      }
      buffer_pool_manager_->UnpinPage(node->GetParentPageId(),true);
    }
  }
  else
  {
//step 0: get parent pointer
    InternalPage *i_node = reinterpret_cast<InternalPage *> (node);
    auto *page = buffer_pool_manager_->FetchPage(i_node->GetParentPageId());
    if(page!=nullptr)
    {
      InternalPage *parent = reinterpret_cast<InternalPage *> (page->GetData());
  //step 0.5: find sibling id and index
      page_id_t sib_id;
      int index = 0;
      int i = parent->ValueIndex(i_node->GetPageId());
      if(i < parent->GetSize())// index == 0
        sib_id = parent->ValueAt(i+1);
      else// index == 1
      {
        index = 1;
        sib_id = parent->ValueAt(parent->GetSize()-1);
      }
//step 1: decide to coalesce or redistribute.
      auto *page = buffer_pool_manager_->FetchPage(sib_id);
      if(page!=nullptr)
      {
        InternalPage *sibling = reinterpret_cast<InternalPage *> (page->GetData());
        if(sibling->GetSize() + i_node->GetSize() > internal_max_size_)//redistribute.
          Redistribute(sibling,i_node,index);
        else//coalesce
          Coalesce(&sibling,&i_node,&parent,index,transaction);
        buffer_pool_manager_->UnpinPage(sib_id,true);
      }
      buffer_pool_manager_->UnpinPage(node->GetParentPageId(),true); 
    }
  }
  return false;
}

/*
 * Move all the key & value pairs from one page to its sibling page, -------------ok:1
 * and notify buffer pool manager to delete this page. ---------------------------ok:2
 * Parent page must be adjusted to take info of deletion into account. -----------ok:3
 * Remember to deal with coalesce or redistribute recursively if necessary.-------ok:4
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) {

  if((*node)->IsLeafPage())// leaf page
  {
    LeafPage* l_neighbor = reinterpret_cast<LeafPage *> (*neighbor_node);
    LeafPage* l_node = reinterpret_cast<LeafPage *> (*node);
    if(!index)// node---neighbor
    {
      l_neighbor->MoveAllTo(l_node);//1
      buffer_pool_manager_->DeletePage(l_neighbor->GetPageId());//2
      (*parent)->Remove((*parent)->ValueIndex(l_neighbor->GetPageId()));//3
      *neighbor_node = *node;
    }
    else// neighbor-----node
    {
      l_node->MoveAllTo(l_neighbor);//1
      buffer_pool_manager_->DeletePage(l_node->GetPageId());//2
      (*parent)->Remove((*parent)->ValueIndex(l_node->GetPageId()));//3
    }
    if((*parent)->GetSize() < (*parent)->GetMinSize())
      CoalesceOrRedistribute(*parent,transaction);
    *node = nullptr;
  }
  else
  {
    InternalPage* i_neighbor = reinterpret_cast<InternalPage *> (*neighbor_node);
    InternalPage* i_node = reinterpret_cast<InternalPage *> (*node);
    if(!index)// node ---- neighbor
    {
      int i = (*parent)->ValueIndex(i_neighbor->GetPageId());
      i_neighbor->MoveAllTo(i_node,(*parent)->KeyAt(i),buffer_pool_manager_);
      buffer_pool_manager_->DeletePage(i_neighbor->GetPageId());
      (*parent)->Remove(i);//3
      *neighbor_node = *node;
    }
    else
    {
      int i = (*parent)->ValueIndex(i_node->GetPageId());
      i_node->MoveAllTo(i_neighbor,(*parent)->KeyAt(i),buffer_pool_manager_);//1
      buffer_pool_manager_->DeletePage(i_node->GetPageId());//2
      (*parent)->Remove(i);//3
    }
    if((*parent)->GetSize() < (*parent)->GetMinSize())
      CoalesceOrRedistribute(*parent,transaction); 
    *node = nullptr;
    
  }
  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. 
 * If index == 0, move sibling page's first key & value pair into end of input "node",-------------ok:1
 * otherwise move sibling page's last key & value pair into head of input "node".------------------ok:2
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  if(node->IsLeafPage())
  {
    LeafPage* l_neighbor = reinterpret_cast<LeafPage *> (neighbor_node);
    LeafPage* l_node = reinterpret_cast<LeafPage *> (node);
    KeyType old_key, new_key;
    // move the key.
    if(!index)
    {
      old_key = l_neighbor->KeyAt(1);
      l_neighbor->MoveFirstToEndOf(l_node);//1
      new_key = l_neighbor->KeyAt(1);
    }
    else
    {
      old_key = l_node->KeyAt(1);
      l_neighbor->MoveLastToFrontOf(l_node);//2
      new_key = l_node->KeyAt(1);
    }
    //find the old key, replace it with new_key
    page_id_t p_id = root_page_id_;
    while(1)
    {
      auto *page =  buffer_pool_manager_->FetchPage(p_id);
      if(page != nullptr)
      {
        page_id_t old_id = p_id;
        BPlusTreePage* node = reinterpret_cast<BPlusTreePage *> (page->GetData());
        if(node->IsLeafPage())
        {
          buffer_pool_manager_->UnpinPage(old_id,true);
          break;
        }
        else
        {
          InternalPage *internal = reinterpret_cast<InternalPage *> (node);
          for(int i = 2; i <= internal->GetSize(); i++)
          {
            if(comparator_(internal->KeyAt(i),old_key) < 0 || comparator_(internal->KeyAt(i),old_key) > 0){}
            else
              internal->SetKeyAt(i,new_key);
          }
          p_id = internal->Lookup(old_key,comparator_);
          buffer_pool_manager_->UnpinPage(old_id,true);
        }
      }
      else
        break;
    }
  }
  else
  {
    InternalPage* i_neighbor = reinterpret_cast<InternalPage *> (neighbor_node);
    InternalPage* i_node = reinterpret_cast<InternalPage *> (node);
    auto *page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
    if(page!=nullptr)
    {
      InternalPage *parent = reinterpret_cast<InternalPage *> (page->GetData());
      if(!index)//1
      {
        int i = 2;
        for(; i <= parent->GetSize(); i++)//find the middle_pair
          if(parent->ValueAt(i) == i_neighbor->GetPageId())
            break;
        i_neighbor->MoveFirstToEndOf(i_node,parent->KeyAt(i),buffer_pool_manager_);
        parent->SetKeyAt(i,i_neighbor->KeyAt(1));
      }
      else//2
      {
        int i = 2;
        for(; i <= parent->GetSize(); i++)
          if(parent->ValueAt(i) == i_node->GetPageId())
            break;
        i_neighbor->MoveLastToFrontOf(i_node,parent->KeyAt(i),buffer_pool_manager_);
        parent->SetKeyAt(i,i_node->KeyAt(1));
      }
      buffer_pool_manager_->UnpinPage(node->GetParentPageId(),true);
    }
  }
}


/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
//case 1:
  if(!(old_root_node->IsLeafPage()))
  {
    InternalPage* this_p = reinterpret_cast <InternalPage *> (old_root_node);
    auto *page = buffer_pool_manager_->FetchPage(this_p->RemoveAndReturnOnlyChild());
    if(page!=nullptr)
    {
      BPlusTreePage *sub_node = reinterpret_cast <BPlusTreePage *> (page->GetData());
      sub_node->SetParentPageId(INVALID_PAGE_ID);
      root_page_id_ = sub_node->GetPageId();
      buffer_pool_manager_->UnpinPage(this_p->RemoveAndReturnOnlyChild(),true);
    }
    buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
    return true;
  }
//case 2:
  else
  {
    root_page_id_ = INVALID_PAGE_ID;
    buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
    return true;
  }
  return false;
}





/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() {
  IndexIterator<KeyType,ValueType,KeyComparator> res;
  page_id_t p_id = root_page_id_;
  while(1)
  {
    auto *page =  buffer_pool_manager_->FetchPage(p_id);
    if(page != nullptr)
    {
      page_id_t old_id = p_id;
      BPlusTreePage* node = reinterpret_cast<BPlusTreePage *> (page->GetData());
      if(node->IsLeafPage())//leaf: found.
      {
        LeafPage* leaf = reinterpret_cast<LeafPage *> (page->GetData());
        res.SetLeaf(leaf);
        res.SetIndex(1);
        res.SetBuffer(buffer_pool_manager_);
        buffer_pool_manager_->UnpinPage(old_id,true);
        break;
      }
      else//internal: value at 1
      {
        InternalPage *internal = reinterpret_cast<InternalPage *> (page->GetData());
        p_id = internal->ValueAt(1);
        buffer_pool_manager_->UnpinPage(old_id,true);
      }
    }
    else
      break;
  }
  return res;
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  IndexIterator<KeyType,ValueType,KeyComparator> res;
  page_id_t p_id = root_page_id_;
  while(1)
  {
    auto *page =  buffer_pool_manager_->FetchPage(p_id);
    if(page != nullptr)
    {
      page_id_t old_id = p_id;
      BPlusTreePage* node = reinterpret_cast<BPlusTreePage *> (page->GetData());
      if(node->IsLeafPage())//leaf: found.
      {
        LeafPage* leaf = reinterpret_cast<LeafPage *> (page->GetData());
        res.SetLeaf(leaf);
        res.SetIndex(leaf->KeyIndex(key,comparator_));
        res.SetBuffer(buffer_pool_manager_);
        buffer_pool_manager_->UnpinPage(old_id,true);
        break;
      }
      else//internal: value at 1
      {
        InternalPage *internal = reinterpret_cast<InternalPage *> (page->GetData());
        p_id = internal->Lookup(key,comparator_);
        buffer_pool_manager_->UnpinPage(old_id,true);
      }
    }
    else
      break;
  }
  return res;
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::End() {
  IndexIterator<KeyType,ValueType,KeyComparator> res;
  page_id_t p_id = root_page_id_;
  while(1)
  {
    auto *page =  buffer_pool_manager_->FetchPage(p_id);
    if(page != nullptr)
    {
      page_id_t old_id = p_id;
      BPlusTreePage* node = reinterpret_cast<BPlusTreePage *> (page->GetData());
      if(node->IsLeafPage())//leaf: end.
      {
        LeafPage* leaf = reinterpret_cast<LeafPage *> (page->GetData());
        res.SetLeaf(leaf);
        res.SetIndex(leaf->GetSize());
        res.SetBuffer(buffer_pool_manager_);
        buffer_pool_manager_->UnpinPage(old_id,true);
        break;
      }
      else//internal: value at end.
      {
        InternalPage *internal = reinterpret_cast<InternalPage *> (page->GetData());
        p_id = internal->ValueAt(internal->GetSize());
        buffer_pool_manager_->UnpinPage(old_id,true);
      }
    }
    else
      break;
  }
  return res;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  IndexIterator<KeyType,ValueType,KeyComparator> iter;
  if(leftMost)
    iter = Begin();
  else
    iter = Begin(key);
  Page *res = reinterpret_cast<Page *> (iter.GetLeaf());
  buffer_pool_manager_->UnpinPage(iter.GetLeaf()->GetPageId(),true);
  return res;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  IndexRootsPage *root_page = reinterpret_cast<IndexRootsPage *>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID));
  if(insert_record == 1)
  {
    root_page->Update(index_id_,root_page_id_);
  }
  else
  {
    root_page->Insert(index_id_,root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID,true);
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 1; i <= leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId()
          << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 1; i <= inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 1) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> "
          << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 1; i <= inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 1) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId()
              << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}

template
class BPlusTree<int, int, BasicComparator<int>>;

template
class BPlusTree<GenericKey<4>, RowId, GenericComparator<4>>;

template
class BPlusTree<GenericKey<8>, RowId, GenericComparator<8>>;

template
class BPlusTree<GenericKey<16>, RowId, GenericComparator<16>>;

template
class BPlusTree<GenericKey<32>, RowId, GenericComparator<32>>;

template
class BPlusTree<GenericKey<64>, RowId, GenericComparator<64>>;
