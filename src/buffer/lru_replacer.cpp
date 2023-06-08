#include <algorithm>
#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) {

}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {//该函数的作用就是将一个可替换的page的页号返回回去，并且在lru-list中将其从尾端提到首端
  frame_id_t temp;
  if(lru_list_.size()!=0){//链表是非空的
    temp = lru_list_.back();//将链表的最后一个元素，也就是最长时间没访问的page记录下来
    lru_list_.pop_back();//从链表中将这一page删除
    *frame_id = temp;//将新加载进来的page分配到该page id
   // lru_list_.push_front(temp);//只要该page还未被pin，那么就是可以替换的对象，可以理解为刚刚访问的page
    return true;
  }
  else
    return false;
}

void LRUReplacer::Pin(frame_id_t frame_id) {//设定某个page的状态为不可替换

  list<frame_id_t>::iterator it;
//  int flag = 0;
  it = find(lru_list_.begin(),lru_list_.end(),frame_id);//判断链表中是否存在该page
  if (it != lru_list_.end()) {
    //lru_list_.erase(it);
    lru_list_.remove(frame_id);
  }
//  for(it=lru_list_.begin();it!=lru_list_.end();it++){
//    if(*it==frame_id) {
//      flag=1;
//      break;
//    }
//  }
//
//  if(flag==1){
//    lru_list_.remove(frame_id);
//  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {//设定某个page的状态为可以替换
  list<frame_id_t>::iterator it;
  it = find(lru_list_.begin(),lru_list_.end(),frame_id);//判断链表中是否存在该page
  if(it==lru_list_.end()){//没找到该元素
    lru_list_.push_front(frame_id);//将该page放到链表头，也就相当于最近访问
  }
//  else{//找到了该元素，也就是进行了重复的unpin，这里理解为最新访问
//    lru_list_.remove(frame_id);//先将该元素移除
//    lru_list_.push_front(frame_id);//在放到最新访问这里
//  }

}

size_t LRUReplacer::Size() {
  return lru_list_.size();//返回链表内元素的数量
}

//lru_list_
//整体而言，该模块重点在于标记
//需要考虑count是否由该模块维护