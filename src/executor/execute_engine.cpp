#include "executor/execute_engine.h"
#include "glog/logging.h"
extern "C" {
int yyparse(void);
//FILE *yyin;
#include "parser/minisql_lex.h"
#include "parser/parser.h"
}

ExecuteEngine::ExecuteEngine() {

}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast, ExecuteContext *context) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context);
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context);
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context);
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context);
    case kNodeShowTables:
      return ExecuteShowTables(ast, context);
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context);
    case kNodeDropTable:
      return ExecuteDropTable(ast, context);
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context);
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context);
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context);
    case kNodeSelect:
      return ExecuteSelect(ast, context);
    case kNodeInsert:
      return ExecuteInsert(ast, context);
    case kNodeDelete:
      return ExecuteDelete(ast, context);
    case kNodeUpdate:
      return ExecuteUpdate(ast, context);
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context);
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context);
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context);
    case kNodeExecFile:
      return ExecuteExecfile(ast, context);
    case kNodeQuit:
      return ExecuteQuit(ast, context);
    default:
      break;
  }
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {//新建数据库文件，用engine的构造函数
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  if(ast->type_==kNodeCreateDB){//判断根节点是否为创建database的操作
    ast=ast->child_;
    string name = ast->val_;//得到数据库的名字
    fstream databases;
    databases.open("databases.txt", ios::in); 
    std::string temp;
    while(getline(databases,temp)){//从文件当中查找有没有
      if(temp == name){
        LOG(INFO) <<name<<"already exist"<<std::endl;
        databases.close();
        return DB_FAILED;
      }
    }

    databases.close(); 
    databases.open("databases.txt", ios::app); //先读用来记录信息的文本文件
    databases<<name<<endl;//把dbname写入
    databases.close();
    name = name+ ".db";
    auto db_01 = new DBStorageEngine(name, true);  //初始化一个数据库
    //this->current_db_ = name;//首先是将当前的数据库名称进行设置

    this->dbs_.insert(std::unordered_map<std::string, DBStorageEngine *>::value_type(name,db_01));//然后将有关的指针也加载到整体的map中去
    //delete db_01;//***************database刷盘，其实在executeengine析构的时候会自动刷
    for(std::pair<std::string, DBStorageEngine *> i:this->dbs_){
    LOG(INFO)<<i.first<<std::endl;
  }
    //更改策略，由于设置了current_db_所以可以在切换db或者quit的时候刷
    //DiskFileMetaPage *meta_page = reinterpret_cast<DiskFileMetaPage *>(db_01->disk_mgr_->GetMetaData());
    //db_01->disk_mgr_->GetMetaData();
    return DB_SUCCESS;
  }
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  if (ast->type_==kNodeDropDB){//判断结点类型

    ast =ast->child_;
    ASSERT(ast->type_==kNodeIdentifier,"not indentigier");
    std::string db_name(ast->val_);//获取db名称

    //if(this->dbs_.find(db_name)==this->dbs_.end()) {//如果在db_name当中找不到
    //  LOG(INFO)<<"database"<<db_name<<"not exist"<<std::endl;
    //  return DB_TABLE_NOT_EXIST;
    //}

    fstream databases;
    databases.open("databases.txt", ios::in); //先读databases
    stringstream ss;
    bool flag = 0;
    std::string temp_db_name;
    while(getline(databases,temp_db_name)){//遍历每一个db_name 
      if(temp_db_name == db_name){    
    //   if(context->using_engine != nullptr) {delete context->using_engine;LOG(INFO) << "here2";}           //delete the old one;
    //    use_database_name = use_database_name +".db";
    //     //LOG(INFO) << "use_database " << use_database_name;
    //  context->using_engine = new DBStorageEngine(use_database_name, false);
    //  if(context->using_engine != nullptr) LOG(INFO) << "is not null";

      db_name = db_name+".db";
      if(this->dbs_.find(db_name)!=this->dbs_.end()) this->dbs_.erase(db_name);//如果在dbs_里面则erase
      //this->current_db_ = nullptr;
      //for(std::pair<std::string, DBStorageEngine *> i:this->dbs_){
    //LOG(INFO)<<i.first<<std::endl;
  //}
      flag = 1;

    } 
   else ss<<temp_db_name<<std::endl;//如果不是需要的database写入sstream，如果是需要的则不写
  }//endwhile

  databases.close();
  
  if (flag==1) {
    databases.open("databases.txt", ios::out|ios::trunc);//重新写
    databases<<ss.rdbuf();
    databases.close();
    return DB_SUCCESS;
  }

  else {
    LOG(INFO)<< "The database " << db_name << " is not exist!" <<std::endl;
    return DB_FAILED;
  }

  }//endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif

  if(ast->type_==kNodeShowDB){//从databases.txt当中showdatabases
    fstream databases;
    databases.open("databases.txt", ios::in); 
    std::string temp;
    while(getline(databases,temp)){
      LOG(INFO)<<"database:"<<temp<<std::endl;
    }
    databases.close();
    return DB_SUCCESS;
  }

  return DB_FAILED;
}


dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {//在切换db的时候刷盘
  #ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteUseDatabase" << std::endl;
  #endif
  pSyntaxNode child_node = ast->child_;
  ASSERT(child_node->type_ == kNodeIdentifier, "not kNodeIdentifier");

  std::string dbname(child_node->val_);   std::string temp;
  fstream databases;
  databases.open("databases.txt", ios::in); 

  while(getline(databases,temp)){//从文件当中查找有没有
     if(temp == dbname){
       std::string n = dbname+".db";
        if (n!=this->current_db_) {//如果不是正在使用的db
          delete dbs_[this->current_db_];
          dbs_.erase(this->current_db_);
          dbname = dbname +".db";
        LOG(INFO) << "use_database " << dbname<<std::endl;
        this->current_db_ = dbname;
        DBStorageEngine *ndb = new DBStorageEngine(dbname, false);
        dbs_[dbname] = ndb;
          }

          //切换db将现在正在用的db刷盘，保证每次只需要刷当前修改过的
        
        databases.close();
        //for(std::pair<std::string, DBStorageEngine *> i:this->dbs_){
    //LOG(INFO)<<i.first<<std::endl;
  //}
        return DB_SUCCESS;
      }
    } 

  LOG(INFO) << "The database " << dbname << " is not exist!" <<std::endl;//如果没找到
  databases.close();
  return DB_FAILED;
}


dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  if(ast->type_==kNodeShowTables){//判断根节点
    DBStorageEngine * db = dbs_[current_db_]; //获得当前的db
    std::vector<TableInfo *> tables;//新建一个table info的map
    ASSERT(db->catalog_mgr_->GetTables(tables)==DB_SUCCESS,"NO TABLE");
    LOG(INFO)<<"database "<<db->db_file_name_<<std::endl;
    int flag = 0;
    for(TableInfo *i:tables){//遍历每一个tableinfo
      LOG(INFO)<<i->GetTableName()<<std::endl;//输出tableinfo的table_name
      flag++;
    } 
    if (flag==0) LOG(INFO)<<"no table"<<std::endl;
    return DB_SUCCESS;
  }
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  if(ast->type_!=kNodeCreateTable) return DB_FAILED;//判断根节点
  ast = ast->child_;
  std::string table_name(ast->val_);//char* to std::string获取表名
  ast = ast->next_; //获取column definition list
  //ASSERT(ast->type_==kNodeColumnDefinitionList ,"not column_list");
  pSyntaxNode column_list = ast->child_;
  std::vector<Column *> columns; //初始化columns来构建schema
  int column_list_number = 0;
  while(column_list){//分别对所有column进行操作
    pSyntaxNode item = column_list->child_;
    int flag = 0;
    if(column_list->val_) {
      std::string col_type(column_list->val_);
      if (col_type=="primary keys") flag = 1;//为主键的情况
      else if (col_type=="unique") flag = 2;//为unique的情况
    }
    if (flag==1){//如果为主键的信息，主键总是在最后一个出现
      while(item){//遍历每一个item
        std::string pri_key_name(item->val_);
        for(Column* i:columns){
          if (pri_key_name==i->GetName()) i->SetUnique(true);//主键的unique设置为true
        }
        item = item->next_;
      }
      //*************************
    }
    else{//不为主键信息，为column信息
    TypeId t = kTypeInvalid;
    bool isunique = false;
    if (flag==2) isunique = true;
    std::string column_name = item->val_;//获取变量名
    item = item->next_;//此时的item是类型
    std::string type(item->val_);//获取类型名
    int length = 0;
    if(type=="int") t = kTypeInt; //判断column_type 类型
    else if (type=="char"){
      std::string str = item->child_->val_;
      auto it = str.begin();
      while (it != str.end() && std::isdigit(*it)) {//遍历字符串，检查每一个元素是否为数字
          it++;
      }
      if(str.empty() || it != str.end()){//char number不是自然数的情况
        LOG(INFO)<<"char number error"<<endl;
        return DB_FAILED;
      }
      t = kTypeChar; 
      length = std::stoi(str);//varchar的length
    }
    else if (type=="float") t= kTypeFloat;
    if (t!=kTypeChar) {//不是ktypechar的构造
    Column *column = new Column(column_name,t,column_list_number,true,isunique);
    columns.push_back(column);
    }
    else {//ktypechar的构造
    Column *column = new Column(column_name,t,length,column_list_number,true,isunique);
    columns.push_back(column);
    }
    }
    if(flag!=1) column_list_number++;
    column_list = column_list->next_;//寻找下一个column
  }//对columns的处理结束
  DBStorageEngine * db = dbs_[current_db_]; //假设当前db和clm没被删除，可以直接从dbs_中获得其db和clm;
  TableInfo* table_info = nullptr;
  Transaction * txn = nullptr;
  Schema *schema = new Schema(columns);//把上面得到的columns构造成一个schema
  dberr_t re = db->catalog_mgr_->CreateTable(table_name,schema,txn,table_info);//在clm中create table
  if(re==DB_TABLE_ALREADY_EXIST) LOG(INFO)<<"table"<<table_name<<"already exists"<<std::endl;
  return re;
}

dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  //LOG(INFO)<<current_db_<<std::endl;
  if(ast->type_==kNodeDropTable){//判断根节点
    ast = ast->child_;
    ASSERT(ast->type_==kNodeIdentifier,"not indentigier");
    std::string table_name(ast->val_);//char* to std::string
    DBStorageEngine * db = dbs_[current_db_]; //假设当前db和clm没被删除，可以直接从dbs_中获得其db和clm;
    //LOG(INFO)<<current_db_<<std::endl;
    dberr_t re = db->catalog_mgr_->DropTable(table_name);
    if (re==DB_TABLE_NOT_EXIST) LOG(INFO)<<"Unknown table "<<table_name<<std::endl;//输出错误信息

    return re;
  }
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  if(ast->type_==kNodeShowIndexes){//判断根节点是否为查看索引的操作
    string db_name=current_db_;
    //dbs_[db_name] = new DBStorageEngine(db_name, false);  //创建数据库
    DBStorageEngine * db = dbs_[current_db_]; //获得当前的db
    dbs_[db_name] = db;
    vector<IndexInfo*> indexes;
    dbs_[db_name]->catalog_mgr_->GetTableIndexes(db_name,indexes);
    for(int i=0;indexes[i]!=NULL;i++){//遍历所有indexes
      LOG(INFO)<<indexes[i]->GetIndexName()<<std::endl;
    }
    return DB_SUCCESS;
  }
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  if(ast->type_==kNodeCreateIndex){//判断根节点是否为查看索引的操作
    string db_name=current_db_;
    //dbs_[db_name] = new DBStorageEngine(db_name, false);  //创建数据库
    //DBStorageEngine * db_01 = dbs_[current_db_]; //获得当前的db
    vector<std::string> index_keys;
    pSyntaxNode ch = ast->child_;//找到所有index_keys
    string index_name=ch->val_;
    string db=ch->next_->val_;
    while(ch->type_!=kNodeColumnList) ch=ch->next_;//定位到kcolunmlist里面
    ch=ch->child_;
    while(ch!=nullptr){
      index_keys.push_back(ch->val_);
      ch=ch->next_;
    }

    IndexInfo* index_info;//怎样用语法树节点初始化index_info
    
    auto state=dbs_[db_name]->catalog_mgr_->CreateIndex(db,index_name,index_keys,nullptr,index_info);
    if(state == DB_COLUMN_NAME_NOT_EXIST){
      LOG(INFO)<<"colomn not exists!"<<std::endl;
      return DB_COLUMN_NAME_NOT_EXIST;
    }
    else if(state==DB_INDEX_ALREADY_EXIST){
      LOG(INFO)<<"index already exists!"<<std::endl;
      return DB_INDEX_ALREADY_EXIST;
    }
    else if(state==DB_TABLE_NOT_EXIST){
      LOG(INFO)<<"table not exists!"<<std::endl;
      return DB_TABLE_NOT_EXIST;
    }
    else if(state==DB_SUCCESS){
      LOG(INFO)<<"create success!"<<std::endl;
      return DB_SUCCESS;
    }
  }
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  if(ast->type_==kNodeDropIndex){
    string tb_name=current_db_;
    //dbs_[current_db_] = new DBStorageEngine(db_name, false);  //创建数据库
    //DBStorageEngine * db = dbs_[current_db_]; //获得当前的db
    string index_name=ast->child_->val_;
    
    if (dbs_[current_db_]->catalog_mgr_->GetIndexTableName(index_name,tb_name)==DB_INDEX_NOT_FOUND ) return DB_INDEX_NOT_FOUND;
    auto state = dbs_[current_db_]->catalog_mgr_->DropIndex(tb_name,index_name);
    if(state==DB_INDEX_NOT_FOUND){
      LOG(INFO)<<"index not found!"<<std::endl;
      return DB_INDEX_NOT_FOUND;
    }
    //else if(state==DB_TABLE_NOT_EXIST){
    //  LOG(INFO)<<"table not exists!"<<endl;
    //  return DB_TABLE_NOT_EXIST;
    //}
    else if(state==DB_SUCCESS){
      LOG(INFO)<<"drop success!"<<std::endl;
      return DB_SUCCESS;
    }
  }
  return DB_FAILED;
}

int judge_op_node_type(pSyntaxNode ast){
  std::string temp = ast->val_;
  if(temp =="=")return 1;
  else if(temp=="<=") return 2;
  else if(temp==">=") return 3;
  else if(temp=="<>") return 4;
  else if(temp=="<") return 5;
  else if(temp==">") return 6;
  else if(temp=="is") return 7;
  else if(temp=="not") return 8;
  else return -1;
}

dberr_t ExecuteEngine::ExecuteSelect(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteSelect" << std::endl;
#endif
  if (ast->type_==kNodeSelect){
    std::string db_name = current_db_;
    DBStorageEngine * db_01 = dbs_[current_db_]; //获得当前的db
    auto &catalog_01 = db_01->catalog_mgr_;;
    TableInfo *table_info1 = nullptr;
    TableHeap * myTableHeap;

    ast = ast->child_;//来到投影节点
    pSyntaxNode condition = ast->next_;//来到table节点



    //int if_select_all = 1;//需要判断是部分select全部
    
    string table_name = ast->next_->val_;
    if(catalog_01->GetTable(table_name,table_info1)==DB_SUCCESS){
      int flagg=0;
      std::set<RowId> row_to_delete1;
      std::set<RowId> row_to_delete2;
      std::set<RowId> result;
      std::stack<set<RowId>>  for_or;
      myTableHeap = table_info1->GetTableHeap();//不考虑roll back的情况，直接进行删除
      condition = condition->next_;
      if(!condition){//没有判断条件，通过tableheap迭代器全部取出rowid到delete1里面去
        for(TableIterator iter = myTableHeap->Begin();iter!=myTableHeap->End();iter++){
                  row_to_delete1.insert(iter->GetRowId());//如果符合条件就加到这里面去
              }
      }
      else{
      condition = condition->child_;
      std::stack<pSyntaxNode> myStack;//用这个栈来存储条件信息
      myStack.push(condition);
      if(condition->type_==kNodeConnector){//如果存在连接条件
        while(condition->child_->type_==kNodeConnector){
          condition = condition->child_;
          myStack.push(condition->next_);//operator入栈
          myStack.push(condition);//connector入栈
        }
      }
      if(condition->child_->type_==kNodeCompareOperator){
        condition = condition->child_;
        myStack.push(condition->next_);
        myStack.push(condition);//到这里，选择条件已全部入栈
      }
      

      //现在要判断有没有符合条件的索引               ////要继续用到ast了，目前ast是在columnList或者allcolumns
      vector<IndexInfo*> myIndexInfo;
      catalog_01->GetTableIndexes(table_name,myIndexInfo);//获取所有的index
      //不能在外面进行判断，而要对每一个条件进行判断

        while(!myStack.empty()){//只要不是空的就会一直循环
          if(flagg==0&&myStack.top()->type_== kNodeCompareOperator){
            pSyntaxNode temp1 = myStack.top();//接收其中一个operator
            int op1 = 0;
            op1 = judge_op_node_type(temp1);
            //  myStack.pop();
            std::string attrib = temp1->child_->val_;//需要根据属性得到是哪个field
            std::string value_s = temp1->child_->next_->val_;
            Schema * now_Schema = table_info1->GetSchema();
            int ind = now_Schema->getIDIndex(attrib);//得到对应的下标
            
            if(op1==1){
            int if_index = -1;//先找是否存在索引
            int num = 0;
            std::vector<IndexInfo*>::iterator it;
            for(it=myIndexInfo.begin();it!=myIndexInfo.end();it++){//遍历每一个index
              if_index = (*it)->GetIndexKeySchema()->getIDIndex(attrib);
              num = (*it)->GetIndexKeySchema()->GetColumns().size();
              if(if_index!=-1&&num==1){//如果存在索引而且索引只有一个column
                break;
              }
            }//找完了索引
            if (if_index!=-1){//如果有索引存在
              std::vector<uint32_t> key_map_of_this_index;
            IndexMetadata * myMetadata = (*it)->GetMetaData();
            key_map_of_this_index = myMetadata->GetKeyMapping();//有了这个keymapping之后，就可以挑选出自己的row
            IndexSchema * This_schema = (*it)->GetIndexKeySchema();//这里有了这个index需要的schema
            //int length = This_schema->GetSerializedSize();

            Index * temp = (*it)->GetIndex();
            TypeId t = This_schema->GetColumns()[if_index]->GetType();//需要构建一个field去找，先获得类型
            std::vector<Field> fields;
            if (t==kTypeChar){
            int length1 = This_schema->GetColumns()[if_index]->GetLength();
            Field field(This_schema->GetColumns()[if_index]->GetType(),(char *)value_s.data(),length1,true);
            fields.push_back(field);
            }
            else if(t==kTypeFloat){
            Field field(This_schema->GetColumns()[if_index]->GetType(),stof((char *)value_s.data()));    
            fields.push_back(field);
            }
            else if(t==kTypeInt){
            Field field(This_schema->GetColumns()[if_index]->GetType(),stoi((char *)value_s.data()));    
              fields.push_back(field);
            }
            Row row(fields);
            Transaction * txn = nullptr;
            std::vector<RowId> rows;
            temp->ScanKey(row,rows,txn);//得到rows
            for(RowId i:rows){
              row_to_delete1.insert(i);
            }
            }
            else{//没有索引用迭代器直接找
              for(TableIterator iter = myTableHeap->Begin(); iter!= myTableHeap->End();iter++){
                if(iter->GetField(ind)->BackValue()==value_s){
                  row_to_delete1.insert(iter->GetRowId());//如果符合条件就加到这里面去
                }
              }
            }
            }
            else if(op1==2){
              int flag = 0;//存在索引的话在b+树里能不能找到
              int if_index = -1;//先找是否存在索引
            int num = 0;
            std::vector<IndexInfo*>::iterator it;
            for(it=myIndexInfo.begin();it!=myIndexInfo.end();it++){//遍历每一个index
              if_index = (*it)->GetIndexKeySchema()->getIDIndex(attrib);
              num = (*it)->GetIndexKeySchema()->GetColumns().size();
              if(if_index!=-1&&num==1){//如果存在索引而且索引只有一个column
                break;
              }
            }//找完了索引
            if (if_index!=-1){//如果有索引存在
              std::vector<uint32_t> key_map_of_this_index;
            IndexMetadata * myMetadata = (*it)->GetMetaData();
            key_map_of_this_index = myMetadata->GetKeyMapping();//有了这个keymapping之后，就可以挑选出自己的row
            IndexSchema * This_schema = (*it)->GetIndexKeySchema();//这里有了这个index需要的schema
            int length = This_schema->GetSerializedSize();//得到序列化长度

            Index * temp = (*it)->GetIndex();
            TypeId t = This_schema->GetColumns()[if_index]->GetType();//需要构建一个field去找，先获得类型
            std::vector<Field> fields;
            if (t==kTypeChar){
            int length1 = This_schema->GetColumns()[if_index]->GetLength();
            Field field(This_schema->GetColumns()[if_index]->GetType(),(char *)value_s.data(),length1,true);
            fields.push_back(field);
            }
            else if(t==kTypeFloat){
            Field field(This_schema->GetColumns()[if_index]->GetType(),stof((char *)value_s.data()));    
            fields.push_back(field);
            }
            else if(t==kTypeInt){
            Field field(This_schema->GetColumns()[if_index]->GetType(),stoi((char *)value_s.data()));    
              fields.push_back(field);
            }
            Row row(fields);
            Transaction * txn = nullptr;
            std::vector<RowId> rows;
            //temp->InsertEntry()
            if(temp->ScanKey(row,rows,txn)==DB_SUCCESS)  flag = 1;//如果找到了，并且获得了rows，目前先只考虑获得了rows的情况s

              RowId rowid = row.GetRowId();//随便用一个
              if(flag==0) {temp->InsertEntry(row,rowid,txn);//如果没有找到则插入一个
              temp->ScanKey(row,rows,txn);//再找到rows
              }
              //for(RowId i:rows){
              //  row_to_delete1.insert(i);
              //}
              //index_id_t iid = myMetadata->GetIndexId();
              //if(temp->ScanKey())
             if(length<=4){//这里要根据模板类型构造迭代器，通过迭代器获取rowid然后insert到row_to_delete里面
             BPlusTreeIndex<GenericKey<4>,RowId,GenericComparator<4>> * BPtree =
                 (BPlusTreeIndex<GenericKey<4>,RowId,GenericComparator<4>>*)(temp);
                 int status = 0;
              IndexIterator<GenericKey<4UL>, RowId, GenericComparator<4UL>> i;
              for(i = BPtree->GetBeginIterator();i!= BPtree->GetEndIterator();i.operator++()){
                for(int j =0;j<i.GetLeaf()->GetSize();j++){
                  row_to_delete1.insert(i.GetLeaf()->GetItem(j).second);
                  if(i.GetLeaf()->GetItem(j).second.operator==(row.GetRowId())) {
                    status = 1;
                    break;
                }
                if (status==1) break;
              }
              }
            }//*****************************
           else if(length<=8){
             BPlusTreeIndex<GenericKey<8>,RowId,GenericComparator<8>> * BPtree =
                 (BPlusTreeIndex<GenericKey<8>,RowId,GenericComparator<8>>*)(temp);
                 int status = 0;
              IndexIterator<GenericKey<8UL>, RowId, GenericComparator<8UL>> i;
              for(i = BPtree->GetBeginIterator();i!= BPtree->GetEndIterator();i.operator++()){
                for(int j =0;j<i.GetLeaf()->GetSize();j++){
                  row_to_delete1.insert(i.GetLeaf()->GetItem(j).second);
                  if(i.GetLeaf()->GetItem(j).second.operator==(row.GetRowId())) {
                    status = 1;
                    break;
                }
                if (status==1) break;
              }
              }
           }
           else if(length<=16){
             BPlusTreeIndex<GenericKey<16>,RowId,GenericComparator<16>> * BPtree =
                 (BPlusTreeIndex<GenericKey<16>,RowId,GenericComparator<16>>*)(temp);
                 int status = 0;
              IndexIterator<GenericKey<16UL>, RowId, GenericComparator<16UL>> i;
              for(i = BPtree->GetBeginIterator();i!= BPtree->GetEndIterator();i.operator++()){
                for(int j =0;j<i.GetLeaf()->GetSize();j++){
                  row_to_delete1.insert(i.GetLeaf()->GetItem(j).second);
                  if(i.GetLeaf()->GetItem(j).second.operator==(row.GetRowId())) {
                    status = 1;
                    break;
                }
                if (status==1) break;
              }
              }
           }
           else if(length<=32){
             BPlusTreeIndex<GenericKey<32>,RowId,GenericComparator<32>> * BPtree =
                 (BPlusTreeIndex<GenericKey<32>,RowId,GenericComparator<32>>*)(temp);
                 int status = 0;
              IndexIterator<GenericKey<32UL>, RowId, GenericComparator<32UL>> i;
              for(i = BPtree->GetBeginIterator();i!= BPtree->GetEndIterator();i.operator++()){
                for(int j =0;j<i.GetLeaf()->GetSize();j++){
                  row_to_delete1.insert(i.GetLeaf()->GetItem(j).second);
                  if(i.GetLeaf()->GetItem(j).second.operator==(row.GetRowId())) {
                    status = 1;
                    break;
                }
                if (status==1) break;
              }
              }
           }
           else {
             BPlusTreeIndex<GenericKey<64>,RowId,GenericComparator<64>> * BPtree =
                 (BPlusTreeIndex<GenericKey<64>,RowId,GenericComparator<64>>*)(temp);
                 int status = 0;
              IndexIterator<GenericKey<64UL>, RowId, GenericComparator<64UL>> i;
              for(i = BPtree->GetBeginIterator();i!= BPtree->GetEndIterator();i.operator++()){
                for(int j =0;j<i.GetLeaf()->GetSize();j++){
                  row_to_delete1.insert(i.GetLeaf()->GetItem(j).second);
                  if(i.GetLeaf()->GetItem(j).second.operator==(row.GetRowId())) {
                    status = 1;
                    break;
                }
                if (status==1) break;
              }
              }
           }
                       if(flag==0) {
                         temp->RemoveEntry(row,rowid,txn);//如果是插入的那么要把这一条删除
                       row_to_delete1.erase(row.GetRowId());
                       }
            }
            //有索引的查找结束
            else {//无索引的情况 
                for(TableIterator iter = myTableHeap->Begin();iter!=myTableHeap->End();iter++){
                if(iter->GetField(ind)->BackValue()<=value_s){
                  row_to_delete1.insert(iter->GetRowId());//如果符合条件就加到这里面去
                }
              }
              }
            }
            else if(op1==3){
                            int flag = 0;//存在索引的话在b+树里能不能找到
              int if_index = -1;//先找是否存在索引
            int num = 0;
            std::vector<IndexInfo*>::iterator it;
            for(it=myIndexInfo.begin();it!=myIndexInfo.end();it++){//遍历每一个index
              if_index = (*it)->GetIndexKeySchema()->getIDIndex(attrib);
              num = (*it)->GetIndexKeySchema()->GetColumns().size();
              if(if_index!=-1&&num==1){//如果存在索引而且索引只有一个column
                break;
              }
            }//找完了索引
            if (if_index!=-1){//如果有索引存在
              std::vector<uint32_t> key_map_of_this_index;
            IndexMetadata * myMetadata = (*it)->GetMetaData();
            key_map_of_this_index = myMetadata->GetKeyMapping();//有了这个keymapping之后，就可以挑选出自己的row
            IndexSchema * This_schema = (*it)->GetIndexKeySchema();//这里有了这个index需要的schema
            int length = This_schema->GetSerializedSize();//得到序列化长度

            Index * temp = (*it)->GetIndex();
            TypeId t = This_schema->GetColumns()[if_index]->GetType();//需要构建一个field去找，先获得类型
            std::vector<Field> fields;
            if (t==kTypeChar){
            int length1 = This_schema->GetColumns()[if_index]->GetLength();
            Field field(This_schema->GetColumns()[if_index]->GetType(),(char *)value_s.data(),length1,true);
            fields.push_back(field);
            }
            else if(t==kTypeFloat){
            Field field(This_schema->GetColumns()[if_index]->GetType(),stof((char *)value_s.data()));    
            fields.push_back(field);
            }
            else if(t==kTypeInt){
            Field field(This_schema->GetColumns()[if_index]->GetType(),stoi((char *)value_s.data()));    
              fields.push_back(field);
            }
            Row row(fields);
            Transaction * txn = nullptr;
            std::vector<RowId> rows;
            //temp->InsertEntry()
            if(temp->ScanKey(row,rows,txn)==DB_SUCCESS)  flag = 1;//如果找到了，并且获得了rows，目前先只考虑获得了rows的情况s

              RowId rowid = row.GetRowId();//随便用一个
              if(flag==0) {temp->InsertEntry(row,rowid,txn);//如果没有找到则插入一个
              temp->ScanKey(row,rows,txn);//再找到rows
              }
              //for(RowId i:rows){
              //  row_to_delete1.insert(i);
              //}
              //index_id_t iid = myMetadata->GetIndexId();
              //if(temp->ScanKey())
             if(length<=4){//这里要根据模板类型构造迭代器，通过迭代器获取rowid然后insert到row_to_delete里面
            BPlusTreeIndex<GenericKey<4>,RowId,GenericComparator<4>> * BPtree =
              (BPlusTreeIndex<GenericKey<4>,RowId,GenericComparator<4>>*)(temp);
              int status = 0;
              IndexIterator<GenericKey<4UL>, RowId, GenericComparator<4UL>> i;
              for(i = BPtree->GetBeginIterator();i!= BPtree->GetEndIterator();i.operator++()){
                for(int j =0;j<i.GetLeaf()->GetSize();j++){
                  
                  if(i.GetLeaf()->GetItem(j).second.operator==(row.GetRowId())) {
                    status = 1;
                }
                if (status==1) row_to_delete1.insert(i.GetLeaf()->GetItem(j).second);
              }
              }
              }
            //*****************************
           else if(length<=8){
             BPlusTreeIndex<GenericKey<8>,RowId,GenericComparator<8>> * BPtree =
                 (BPlusTreeIndex<GenericKey<8>,RowId,GenericComparator<8>>*)(temp);
                 int status = 0;
              IndexIterator<GenericKey<8UL>, RowId, GenericComparator<8UL>> i;
              for(i = BPtree->GetBeginIterator();i!= BPtree->GetEndIterator();i.operator++()){
                for(int j =0;j<i.GetLeaf()->GetSize();j++){
                  
                  if(i.GetLeaf()->GetItem(j).second.operator==(row.GetRowId())) {
                    status = 1;
                }
                if (status==1) row_to_delete1.insert(i.GetLeaf()->GetItem(j).second);
              }
              }
           }
           else if(length<=16){
             BPlusTreeIndex<GenericKey<16>,RowId,GenericComparator<16>> * BPtree =
                 (BPlusTreeIndex<GenericKey<16>,RowId,GenericComparator<16>>*)(temp);
                 int status = 0;
              IndexIterator<GenericKey<16UL>, RowId, GenericComparator<16UL>> i;
              for(i = BPtree->GetBeginIterator();i!= BPtree->GetEndIterator();i.operator++()){
                for(int j =0;j<i.GetLeaf()->GetSize();j++){
                  
                  if(i.GetLeaf()->GetItem(j).second.operator==(row.GetRowId())) {
                    status = 1;
                }
                if (status==1) row_to_delete1.insert(i.GetLeaf()->GetItem(j).second);
              }
              }
           }
           else if(length<=32){
             BPlusTreeIndex<GenericKey<32>,RowId,GenericComparator<32>> * BPtree =
                 (BPlusTreeIndex<GenericKey<32>,RowId,GenericComparator<32>>*)(temp);
                 int status = 0;
              IndexIterator<GenericKey<32UL>, RowId, GenericComparator<32UL>> i;
              for(i = BPtree->GetBeginIterator();i!= BPtree->GetEndIterator();i.operator++()){
                for(int j =0;j<i.GetLeaf()->GetSize();j++){
                  
                  if(i.GetLeaf()->GetItem(j).second.operator==(row.GetRowId())) {
                    status = 1;
                }
                if (status==1) row_to_delete1.insert(i.GetLeaf()->GetItem(j).second);
              }
              }
           }
           else {
             BPlusTreeIndex<GenericKey<64>,RowId,GenericComparator<64>> * BPtree =
                 (BPlusTreeIndex<GenericKey<64>,RowId,GenericComparator<64>>*)(temp);
                 int status = 0;
              IndexIterator<GenericKey<64UL>, RowId, GenericComparator<64UL>> i;
              for(i = BPtree->GetBeginIterator();i!= BPtree->GetEndIterator();i.operator++()){
                for(int j =0;j<i.GetLeaf()->GetSize();j++){
                  
                  if(i.GetLeaf()->GetItem(j).second.operator==(row.GetRowId())) {
                    status = 1;
                }
                if (status==1) row_to_delete1.insert(i.GetLeaf()->GetItem(j).second);
              }
              }
           }
                       if(flag==0) {
                         temp->RemoveEntry(row,rowid,txn);//如果是插入的那么要把这一条删除
                       row_to_delete1.erase(row.GetRowId());
                       }
            }
            //有索引的查找结束
            else {//无索引的情况 
                for(TableIterator iter = myTableHeap->Begin();iter!=myTableHeap->End();iter++){
                if(iter->GetField(ind)->BackValue()>=value_s){
                  row_to_delete1.insert(iter->GetRowId());//如果符合条件就加到这里面去
                }
              }
              }
            }
            else if(op1==4){
              int if_index = -1;//先找是否存在索引
            int num = 0;
            std::vector<IndexInfo*>::iterator it;
            for(it=myIndexInfo.begin();it!=myIndexInfo.end();it++){//遍历每一个index
              if_index = (*it)->GetIndexKeySchema()->getIDIndex(attrib);
              num = (*it)->GetIndexKeySchema()->GetColumns().size();
              if(if_index!=-1&&num==1){//如果存在索引而且索引只有一个column
                break;
              }
            }//找完了索引
            if (if_index!=-1){//如果有索引存在
              std::vector<uint32_t> key_map_of_this_index;
            IndexMetadata * myMetadata = (*it)->GetMetaData();
            key_map_of_this_index = myMetadata->GetKeyMapping();//有了这个keymapping之后，就可以挑选出自己的row
            IndexSchema * This_schema = (*it)->GetIndexKeySchema();//这里有了这个index需要的schema
            //int length = This_schema->GetSerializedSize();

            Index * temp = (*it)->GetIndex();
            TypeId t = This_schema->GetColumns()[if_index]->GetType();//需要构建一个field去找，先获得类型
            std::vector<Field> fields;
            if (t==kTypeChar){
            int length = This_schema->GetColumns()[if_index]->GetLength();
            Field field(This_schema->GetColumns()[if_index]->GetType(),(char *)value_s.data(),length,true);
            fields.push_back(field);
            }
            else if(t==kTypeFloat){
            Field field(This_schema->GetColumns()[if_index]->GetType(),stof((char *)value_s.data()));    
            fields.push_back(field);
            }
            else if(t==kTypeInt){
            Field field(This_schema->GetColumns()[if_index]->GetType(),stoi((char *)value_s.data()));    
              fields.push_back(field);
            }
            Row row(fields);
            Transaction * txn = nullptr;
            std::vector<RowId> rows;
            temp->ScanKey(row,rows,txn);//得到rows
            for(TableIterator iter = myTableHeap->Begin();iter!=myTableHeap->End();iter++){
                  row_to_delete1.insert(iter->GetRowId());//先全部取出来再用Index erase

              }
            for(RowId i:rows){
              row_to_delete1.erase(i);
            }
            }

              else{
              for(TableIterator iter = myTableHeap->Begin();iter!=myTableHeap->End();iter++){
                if(iter->GetField(ind)->BackValue()!=value_s){
                  row_to_delete1.insert(iter->GetRowId());//如果符合条件就加到这里面去
                }
              }
              }
            }
            else if(op1==5){
              int flag = 0;//存在索引的话在b+树里能不能找到
              int if_index = -1;//先找是否存在索引
            int num = 0;
            std::vector<IndexInfo*>::iterator it;
            for(it=myIndexInfo.begin();it!=myIndexInfo.end();it++){//遍历每一个index
              if_index = (*it)->GetIndexKeySchema()->getIDIndex(attrib);
              num = (*it)->GetIndexKeySchema()->GetColumns().size();
              if(if_index!=-1&&num==1){//如果存在索引而且索引只有一个column
                break;
              }
            }//找完了索引
            if (if_index!=-1){//如果有索引存在
              std::vector<uint32_t> key_map_of_this_index;
            IndexMetadata * myMetadata = (*it)->GetMetaData();
            key_map_of_this_index = myMetadata->GetKeyMapping();//有了这个keymapping之后，就可以挑选出自己的row
            IndexSchema * This_schema = (*it)->GetIndexKeySchema();//这里有了这个index需要的schema
            int length = This_schema->GetSerializedSize();//得到序列化长度

            Index * temp = (*it)->GetIndex();
            TypeId t = This_schema->GetColumns()[if_index]->GetType();//需要构建一个field去找，先获得类型
            std::vector<Field> fields;
            if (t==kTypeChar){
            int length1 = This_schema->GetColumns()[if_index]->GetLength();
            Field field(This_schema->GetColumns()[if_index]->GetType(),(char *)value_s.data(),length1,true);
            fields.push_back(field);
            }
            else if(t==kTypeFloat){
            Field field(This_schema->GetColumns()[if_index]->GetType(),stof((char *)value_s.data()));    
            fields.push_back(field);
            }
            else if(t==kTypeInt){
            Field field(This_schema->GetColumns()[if_index]->GetType(),stoi((char *)value_s.data()));    
              fields.push_back(field);
            }
            Row row(fields);
            Transaction * txn = nullptr;
            std::vector<RowId> rows;
            //temp->InsertEntry()
            if(temp->ScanKey(row,rows,txn)==DB_SUCCESS)  flag = 1;//如果找到了，并且获得了rows，目前先只考虑获得了rows的情况s

              RowId rowid = row.GetRowId();//随便用一个
              if(flag==0) {temp->InsertEntry(row,rowid,txn);//如果没有找到则插入一个
              temp->ScanKey(row,rows,txn);//再找到rows
              }
              //for(RowId i:rows){
              //  row_to_delete1.insert(i);
              //}
              //index_id_t iid = myMetadata->GetIndexId();
              //if(temp->ScanKey())
             if(length<=4){//这里要根据模板类型构造迭代器，通过迭代器获取rowid然后insert到row_to_delete里面
             BPlusTreeIndex<GenericKey<4>,RowId,GenericComparator<4>> * BPtree =
                 (BPlusTreeIndex<GenericKey<4>,RowId,GenericComparator<4>>*)(temp);
                 int status = 0;
              IndexIterator<GenericKey<4UL>, RowId, GenericComparator<4UL>> i;
              for(i = BPtree->GetBeginIterator();i!= BPtree->GetEndIterator();i.operator++()){
                for(int j =0;j<i.GetLeaf()->GetSize();j++){
                  row_to_delete1.insert(i.GetLeaf()->GetItem(j).second);
                  if(i.GetLeaf()->GetItem(j).second.operator==(row.GetRowId())) {
                    status = 1;
                    break;
                }
                if (status==1) break;
              }
              }
            }//*****************************
           else if(length<=8){
             BPlusTreeIndex<GenericKey<8>,RowId,GenericComparator<8>> * BPtree =
                 (BPlusTreeIndex<GenericKey<8>,RowId,GenericComparator<8>>*)(temp);
                 int status = 0;
              IndexIterator<GenericKey<8UL>, RowId, GenericComparator<8UL>> i;
              for(i = BPtree->GetBeginIterator();i!= BPtree->GetEndIterator();i.operator++()){
                for(int j =0;j<i.GetLeaf()->GetSize();j++){
                  row_to_delete1.insert(i.GetLeaf()->GetItem(j).second);
                  if(i.GetLeaf()->GetItem(j).second.operator==(row.GetRowId())) {
                    status = 1;
                    break;
                }
                if (status==1) break;
              }
              }
           }
           else if(length<=16){
             BPlusTreeIndex<GenericKey<16>,RowId,GenericComparator<16>> * BPtree =
                 (BPlusTreeIndex<GenericKey<16>,RowId,GenericComparator<16>>*)(temp);
                 int status = 0;
              IndexIterator<GenericKey<16UL>, RowId, GenericComparator<16UL>> i;
              for(i = BPtree->GetBeginIterator();i!= BPtree->GetEndIterator();i.operator++()){
                for(int j =0;j<i.GetLeaf()->GetSize();j++){
                  row_to_delete1.insert(i.GetLeaf()->GetItem(j).second);
                  if(i.GetLeaf()->GetItem(j).second.operator==(row.GetRowId())) {
                    status = 1;
                    break;
                }
                if (status==1) break;
              }
              }
           }
           else if(length<=32){
             BPlusTreeIndex<GenericKey<32>,RowId,GenericComparator<32>> * BPtree =
                 (BPlusTreeIndex<GenericKey<32>,RowId,GenericComparator<32>>*)(temp);
                 int status = 0;
              IndexIterator<GenericKey<32UL>, RowId, GenericComparator<32UL>> i;
              for(i = BPtree->GetBeginIterator();i!= BPtree->GetEndIterator();i.operator++()){
                for(int j =0;j<i.GetLeaf()->GetSize();j++){
                  row_to_delete1.insert(i.GetLeaf()->GetItem(j).second);
                  if(i.GetLeaf()->GetItem(j).second.operator==(row.GetRowId())) {
                    status = 1;
                    break;
                }
                if (status==1) break;
              }
              }
           }
           else {
             BPlusTreeIndex<GenericKey<64>,RowId,GenericComparator<64>> * BPtree =
                 (BPlusTreeIndex<GenericKey<64>,RowId,GenericComparator<64>>*)(temp);
                 int status = 0;
              IndexIterator<GenericKey<64UL>, RowId, GenericComparator<64UL>> i;
              for(i = BPtree->GetBeginIterator();i!= BPtree->GetEndIterator();i.operator++()){
                for(int j =0;j<i.GetLeaf()->GetSize();j++){
                  row_to_delete1.insert(i.GetLeaf()->GetItem(j).second);
                  if(i.GetLeaf()->GetItem(j).second.operator==(row.GetRowId())) {
                    status = 1;
                    break;
                }
                if (status==1) break;
              }
              }
           }
                       if(flag==0) {
                         temp->RemoveEntry(row,rowid,txn);//如果是插入的那么要把这一条删除
                       }
                       row_to_delete1.erase(row.GetRowId());
            }
            //有索引的查找结束
            else {//无索引的情况 
                for(TableIterator iter = myTableHeap->Begin();iter!=myTableHeap->End();iter++){
                if(iter->GetField(ind)->BackValue()<value_s){
                  row_to_delete1.insert(iter->GetRowId());//如果符合条件就加到这里面去
                }
              }
              }
            }
            else if(op1==6){
                            int flag = 0;//存在索引的话在b+树里能不能找到
              int if_index = -1;//先找是否存在索引
            int num = 0;
            std::vector<IndexInfo*>::iterator it;
            for(it=myIndexInfo.begin();it!=myIndexInfo.end();it++){//遍历每一个index
              if_index = (*it)->GetIndexKeySchema()->getIDIndex(attrib);
              num = (*it)->GetIndexKeySchema()->GetColumns().size();
              if(if_index!=-1&&num==1){//如果存在索引而且索引只有一个column
                break;
              }
            }//找完了索引
            if (if_index!=-1){//如果有索引存在
              std::vector<uint32_t> key_map_of_this_index;
            IndexMetadata * myMetadata = (*it)->GetMetaData();
            key_map_of_this_index = myMetadata->GetKeyMapping();//有了这个keymapping之后，就可以挑选出自己的row
            IndexSchema * This_schema = (*it)->GetIndexKeySchema();//这里有了这个index需要的schema
            int length = This_schema->GetSerializedSize();//得到序列化长度

            Index * temp = (*it)->GetIndex();
            TypeId t = This_schema->GetColumns()[if_index]->GetType();//需要构建一个field去找，先获得类型
            std::vector<Field> fields;
            if (t==kTypeChar){
            int length1 = This_schema->GetColumns()[if_index]->GetLength();
            Field field(This_schema->GetColumns()[if_index]->GetType(),(char *)value_s.data(),length1,true);
            fields.push_back(field);
            }
            else if(t==kTypeFloat){
            Field field(This_schema->GetColumns()[if_index]->GetType(),stof((char *)value_s.data()));    
            fields.push_back(field);
            }
            else if(t==kTypeInt){
            Field field(This_schema->GetColumns()[if_index]->GetType(),stoi((char *)value_s.data()));    
              fields.push_back(field);
            }
            Row row(fields);
            Transaction * txn = nullptr;
            std::vector<RowId> rows;
            //temp->InsertEntry()
            if(temp->ScanKey(row,rows,txn)==DB_SUCCESS)  flag = 1;//如果找到了，并且获得了rows，目前先只考虑获得了rows的情况s

              RowId rowid = row.GetRowId();//随便用一个
              if(flag==0) {temp->InsertEntry(row,rowid,txn);//如果没有找到则插入一个
              temp->ScanKey(row,rows,txn);//再找到rows
              }
              //for(RowId i:rows){
              //  row_to_delete1.insert(i);
              //}
              //index_id_t iid = myMetadata->GetIndexId();
              //if(temp->ScanKey())
             if(length<=4){//这里要根据模板类型构造迭代器，通过迭代器获取rowid然后insert到row_to_delete里面
            BPlusTreeIndex<GenericKey<4>,RowId,GenericComparator<4>> * BPtree =
              (BPlusTreeIndex<GenericKey<4>,RowId,GenericComparator<4>>*)(temp);
              int status = 0;
              IndexIterator<GenericKey<4UL>, RowId, GenericComparator<4UL>> i;
              for(i = BPtree->GetBeginIterator();i!= BPtree->GetEndIterator();i.operator++()){
                for(int j =0;j<i.GetLeaf()->GetSize();j++){
                  
                  if(i.GetLeaf()->GetItem(j).second.operator==(row.GetRowId())) {
                    status = 1;
                }
                if (status==1) row_to_delete1.insert(i.GetLeaf()->GetItem(j).second);
              }
              }
              }
            //*****************************
           else if(length<=8){
             BPlusTreeIndex<GenericKey<8>,RowId,GenericComparator<8>> * BPtree =
                 (BPlusTreeIndex<GenericKey<8>,RowId,GenericComparator<8>>*)(temp);
                 int status = 0;
              IndexIterator<GenericKey<8UL>, RowId, GenericComparator<8UL>> i;
              for(i = BPtree->GetBeginIterator();i!= BPtree->GetEndIterator();i.operator++()){
                for(int j =0;j<i.GetLeaf()->GetSize();j++){
                  
                  if(i.GetLeaf()->GetItem(j).second.operator==(row.GetRowId())) {
                    status = 1;
                }
                if (status==1) row_to_delete1.insert(i.GetLeaf()->GetItem(j).second);
              }
              }
           }
           else if(length<=16){
             BPlusTreeIndex<GenericKey<16>,RowId,GenericComparator<16>> * BPtree =
                 (BPlusTreeIndex<GenericKey<16>,RowId,GenericComparator<16>>*)(temp);
                 int status = 0;
              IndexIterator<GenericKey<16UL>, RowId, GenericComparator<16UL>> i;
              for(i = BPtree->GetBeginIterator();i!= BPtree->GetEndIterator();i.operator++()){
                for(int j =0;j<i.GetLeaf()->GetSize();j++){
                  
                  if(i.GetLeaf()->GetItem(j).second.operator==(row.GetRowId())) {
                    status = 1;
                }
                if (status==1) row_to_delete1.insert(i.GetLeaf()->GetItem(j).second);
              }
              }
           }
           else if(length<=32){
             BPlusTreeIndex<GenericKey<32>,RowId,GenericComparator<32>> * BPtree =
                 (BPlusTreeIndex<GenericKey<32>,RowId,GenericComparator<32>>*)(temp);
                 int status = 0;
              IndexIterator<GenericKey<32UL>, RowId, GenericComparator<32UL>> i;
              for(i = BPtree->GetBeginIterator();i!= BPtree->GetEndIterator();i.operator++()){
                for(int j =0;j<i.GetLeaf()->GetSize();j++){
                  
                  if(i.GetLeaf()->GetItem(j).second.operator==(row.GetRowId())) {
                    status = 1;
                }
                if (status==1) row_to_delete1.insert(i.GetLeaf()->GetItem(j).second);
              }
              }
           }
           else {
             BPlusTreeIndex<GenericKey<64>,RowId,GenericComparator<64>> * BPtree =
                 (BPlusTreeIndex<GenericKey<64>,RowId,GenericComparator<64>>*)(temp);
                 int status = 0;
              IndexIterator<GenericKey<64UL>, RowId, GenericComparator<64UL>> i;
              for(i = BPtree->GetBeginIterator();i!= BPtree->GetEndIterator();i.operator++()){
                for(int j =0;j<i.GetLeaf()->GetSize();j++){
                  
                  if(i.GetLeaf()->GetItem(j).second.operator==(row.GetRowId())) {
                    status = 1;
                }
                if (status==1) row_to_delete1.insert(i.GetLeaf()->GetItem(j).second);
              }
              }
           }
                       if(flag==0) {
                         temp->RemoveEntry(row,rowid,txn);//如果是插入的那么要把这一条删除
                       }
                       row_to_delete1.erase(row.GetRowId());
            }
            //有索引的查找结束
            else {//无索引的情况 
                for(TableIterator iter = myTableHeap->Begin();iter!=myTableHeap->End();iter++){
                if(iter->GetField(ind)->BackValue()>value_s){
                  row_to_delete1.insert(iter->GetRowId());//如果符合条件就加到这里面去
                }
              }
              }
            }
            else if(op1==7){
              for(TableIterator iter = myTableHeap->Begin();iter!=myTableHeap->End();iter++){
                if(iter->GetField(ind)->IsNull()){// is null
                  row_to_delete1.insert(iter->GetRowId());//如果符合条件就加到这里面去
                }
              }
            }
            else if(op1==8){
              for(TableIterator iter = myTableHeap->Begin();iter!=myTableHeap->End();iter++){
                if(!(iter->GetField(ind)->IsNull())){//非null
                  row_to_delete1.insert(iter->GetRowId());//如果符合条件就加到这里面去
                }
              }
            }
            else{
                //理论上不存在该种情况
            }
            myStack.pop();//将栈最上层的元素弹出
            flagg = 1;
          }
          else if(flagg==1&&myStack.top()->type_== kNodeCompareOperator){
            pSyntaxNode temp1 = myStack.top();//接收其中一个operator
            int op1 = 0;
            op1 = judge_op_node_type(temp1);
            myStack.pop();
            std::string attrib = temp1->child_->val_;//需要根据属性得到是哪个field
            std::string value_s = temp1->child_->next_->val_;
            Schema * now_Schema = table_info1->GetSchema();
            int ind = now_Schema->getIDIndex(attrib);//得到对应的下标

            if(op1==1){
            int if_index = -1;//先找是否存在索引
            int num = 0;
            std::vector<IndexInfo*>::iterator it;
            for(it=myIndexInfo.begin();it!=myIndexInfo.end();it++){//遍历每一个index
              if_index = (*it)->GetIndexKeySchema()->getIDIndex(attrib);
              num = (*it)->GetIndexKeySchema()->GetColumns().size();
              if(if_index!=-1&&num==1){//如果存在索引而且索引只有一个column
                break;
              }
            }//找完了索引
            if (if_index!=-1){//如果有索引存在
              std::vector<uint32_t> key_map_of_this_index;
            IndexMetadata * myMetadata = (*it)->GetMetaData();
            key_map_of_this_index = myMetadata->GetKeyMapping();//有了这个keymapping之后，就可以挑选出自己的row
            IndexSchema * This_schema = (*it)->GetIndexKeySchema();//这里有了这个index需要的schema
            //int length = This_schema->GetSerializedSize();

            Index * temp = (*it)->GetIndex();
            TypeId t = This_schema->GetColumns()[if_index]->GetType();//需要构建一个field去找，先获得类型
            std::vector<Field> fields;
            if (t==kTypeChar){
            int length1 = This_schema->GetColumns()[if_index]->GetLength();
            Field field(This_schema->GetColumns()[if_index]->GetType(),(char *)value_s.data(),length1,true);
            fields.push_back(field);
            }
            else if(t==kTypeFloat){
            Field field(This_schema->GetColumns()[if_index]->GetType(),stof((char *)value_s.data()));    
            fields.push_back(field);
            }
            else if(t==kTypeInt){
            Field field(This_schema->GetColumns()[if_index]->GetType(),stoi((char *)value_s.data()));    
              fields.push_back(field);
            }
            Row row(fields);
            Transaction * txn = nullptr;
            std::vector<RowId> rows;
            temp->ScanKey(row,rows,txn);//得到rows
            for(RowId i:rows){
              row_to_delete2.insert(i);
            }
            }
            else{//没有索引用迭代器直接找
              for(TableIterator iter = myTableHeap->Begin(); iter!= myTableHeap->End();iter++){
                if(iter->GetField(ind)->BackValue()==value_s){
                  row_to_delete2.insert(iter->GetRowId());//如果符合条件就加到这里面去
                }
              }
            }
            }
            else if(op1==2){
              int flag = 0;//存在索引的话在b+树里能不能找到
              int if_index = -1;//先找是否存在索引
            int num = 0;
            std::vector<IndexInfo*>::iterator it;
            for(it=myIndexInfo.begin();it!=myIndexInfo.end();it++){//遍历每一个index
              if_index = (*it)->GetIndexKeySchema()->getIDIndex(attrib);
              num = (*it)->GetIndexKeySchema()->GetColumns().size();
              if(if_index!=-1&&num==1){//如果存在索引而且索引只有一个column
                break;
              }
            }//找完了索引
            if (if_index!=-1){//如果有索引存在
              std::vector<uint32_t> key_map_of_this_index;
            IndexMetadata * myMetadata = (*it)->GetMetaData();
            key_map_of_this_index = myMetadata->GetKeyMapping();//有了这个keymapping之后，就可以挑选出自己的row
            IndexSchema * This_schema = (*it)->GetIndexKeySchema();//这里有了这个index需要的schema
            int length = This_schema->GetSerializedSize();//得到序列化长度

            Index * temp = (*it)->GetIndex();
            TypeId t = This_schema->GetColumns()[if_index]->GetType();//需要构建一个field去找，先获得类型
            std::vector<Field> fields;
            if (t==kTypeChar){
            int length1 = This_schema->GetColumns()[if_index]->GetLength();
            Field field(This_schema->GetColumns()[if_index]->GetType(),(char *)value_s.data(),length1,true);
            fields.push_back(field);
            }
            else if(t==kTypeFloat){
            Field field(This_schema->GetColumns()[if_index]->GetType(),stof((char *)value_s.data()));    
            fields.push_back(field);
            }
            else if(t==kTypeInt){
            Field field(This_schema->GetColumns()[if_index]->GetType(),stoi((char *)value_s.data()));    
              fields.push_back(field);
            }
            Row row(fields);
            Transaction * txn = nullptr;
            std::vector<RowId> rows;
            //temp->InsertEntry()
            if(temp->ScanKey(row,rows,txn)==DB_SUCCESS)  flag = 1;//如果找到了，并且获得了rows，目前先只考虑获得了rows的情况s

              RowId rowid = row.GetRowId();//随便用一个
              if(flag==0) {temp->InsertEntry(row,rowid,txn);//如果没有找到则插入一个
              temp->ScanKey(row,rows,txn);//再找到rows
              }
              //for(RowId i:rows){
              //  row_to_delete1.insert(i);
              //}
              //index_id_t iid = myMetadata->GetIndexId();
              //if(temp->ScanKey())
             if(length<=4){//这里要根据模板类型构造迭代器，通过迭代器获取rowid然后insert到row_to_delete里面
             BPlusTreeIndex<GenericKey<4>,RowId,GenericComparator<4>> * BPtree =
                 (BPlusTreeIndex<GenericKey<4>,RowId,GenericComparator<4>>*)(temp);
                 int status = 0;
              IndexIterator<GenericKey<4UL>, RowId, GenericComparator<4UL>> i;
              for(i = BPtree->GetBeginIterator();i!= BPtree->GetEndIterator();i.operator++()){
                for(int j =0;j<i.GetLeaf()->GetSize();j++){
                  row_to_delete2.insert(i.GetLeaf()->GetItem(j).second);
                  if(i.GetLeaf()->GetItem(j).second.operator==(row.GetRowId())) {
                    status = 1;
                    break;
                }
                if (status==1) break;
              }
              }
            }//*****************************
           else if(length<=8){
             BPlusTreeIndex<GenericKey<8>,RowId,GenericComparator<8>> * BPtree =
                 (BPlusTreeIndex<GenericKey<8>,RowId,GenericComparator<8>>*)(temp);
                 int status = 0;
              IndexIterator<GenericKey<8UL>, RowId, GenericComparator<8UL>> i;
              for(i = BPtree->GetBeginIterator();i!= BPtree->GetEndIterator();i.operator++()){
                for(int j =0;j<i.GetLeaf()->GetSize();j++){
                  row_to_delete2.insert(i.GetLeaf()->GetItem(j).second);
                  if(i.GetLeaf()->GetItem(j).second.operator==(row.GetRowId())) {
                    status = 1;
                    break;
                }
                if (status==1) break;
              }
              }
           }
           else if(length<=16){
             BPlusTreeIndex<GenericKey<16>,RowId,GenericComparator<16>> * BPtree =
                 (BPlusTreeIndex<GenericKey<16>,RowId,GenericComparator<16>>*)(temp);
                 int status = 0;
              IndexIterator<GenericKey<16UL>, RowId, GenericComparator<16UL>> i;
              for(i = BPtree->GetBeginIterator();i!= BPtree->GetEndIterator();i.operator++()){
                for(int j =0;j<i.GetLeaf()->GetSize();j++){
                  row_to_delete2.insert(i.GetLeaf()->GetItem(j).second);
                  if(i.GetLeaf()->GetItem(j).second.operator==(row.GetRowId())) {
                    status = 1;
                    break;
                }
                if (status==1) break;
              }
              }
           }
           else if(length<=32){
             BPlusTreeIndex<GenericKey<32>,RowId,GenericComparator<32>> * BPtree =
                 (BPlusTreeIndex<GenericKey<32>,RowId,GenericComparator<32>>*)(temp);
                 int status = 0;
              IndexIterator<GenericKey<32UL>, RowId, GenericComparator<32UL>> i;
              for(i = BPtree->GetBeginIterator();i!= BPtree->GetEndIterator();i.operator++()){
                for(int j =0;j<i.GetLeaf()->GetSize();j++){
                  row_to_delete2.insert(i.GetLeaf()->GetItem(j).second);
                  if(i.GetLeaf()->GetItem(j).second.operator==(row.GetRowId())) {
                    status = 1;
                    break;
                }
                if (status==1) break;
              }
              }
           }
           else {
             BPlusTreeIndex<GenericKey<64>,RowId,GenericComparator<64>> * BPtree =
                 (BPlusTreeIndex<GenericKey<64>,RowId,GenericComparator<64>>*)(temp);
                 int status = 0;
              IndexIterator<GenericKey<64UL>, RowId, GenericComparator<64UL>> i;
              for(i = BPtree->GetBeginIterator();i!= BPtree->GetEndIterator();i.operator++()){
                for(int j =0;j<i.GetLeaf()->GetSize();j++){
                  row_to_delete2.insert(i.GetLeaf()->GetItem(j).second);
                  if(i.GetLeaf()->GetItem(j).second.operator==(row.GetRowId())) {
                    status = 1;
                    break;
                }
                if (status==1) break;
              }
              }
           }
                       if(flag==0) {
                         temp->RemoveEntry(row,rowid,txn);//如果是插入的那么要把这一条删除
                       row_to_delete2.erase(row.GetRowId());
                       }
            }
            //有索引的查找结束
            else {//无索引的情况 
                for(TableIterator iter = myTableHeap->Begin();iter!=myTableHeap->End();iter++){
                if(iter->GetField(ind)->BackValue()<=value_s){
                  row_to_delete2.insert(iter->GetRowId());//如果符合条件就加到这里面去
                }
              }
              }
            }
            else if(op1==3){
                            int flag = 0;//存在索引的话在b+树里能不能找到
              int if_index = -1;//先找是否存在索引
            int num = 0;
            std::vector<IndexInfo*>::iterator it;
            for(it=myIndexInfo.begin();it!=myIndexInfo.end();it++){//遍历每一个index
              if_index = (*it)->GetIndexKeySchema()->getIDIndex(attrib);
              num = (*it)->GetIndexKeySchema()->GetColumns().size();
              if(if_index!=-1&&num==1){//如果存在索引而且索引只有一个column
                break;
              }
            }//找完了索引
            if (if_index!=-1){//如果有索引存在
              std::vector<uint32_t> key_map_of_this_index;
            IndexMetadata * myMetadata = (*it)->GetMetaData();
            key_map_of_this_index = myMetadata->GetKeyMapping();//有了这个keymapping之后，就可以挑选出自己的row
            IndexSchema * This_schema = (*it)->GetIndexKeySchema();//这里有了这个index需要的schema
            int length = This_schema->GetSerializedSize();//得到序列化长度

            Index * temp = (*it)->GetIndex();
            TypeId t = This_schema->GetColumns()[if_index]->GetType();//需要构建一个field去找，先获得类型
            std::vector<Field> fields;
            if (t==kTypeChar){
            int length1 = This_schema->GetColumns()[if_index]->GetLength();
            Field field(This_schema->GetColumns()[if_index]->GetType(),(char *)value_s.data(),length1,true);
            fields.push_back(field);
            }
            else if(t==kTypeFloat){
            Field field(This_schema->GetColumns()[if_index]->GetType(),stof((char *)value_s.data()));    
            fields.push_back(field);
            }
            else if(t==kTypeInt){
            Field field(This_schema->GetColumns()[if_index]->GetType(),stoi((char *)value_s.data()));    
              fields.push_back(field);
            }
            Row row(fields);
            Transaction * txn = nullptr;
            std::vector<RowId> rows;
            //temp->InsertEntry()
            if(temp->ScanKey(row,rows,txn)==DB_SUCCESS)  flag = 1;//如果找到了，并且获得了rows，目前先只考虑获得了rows的情况s

              RowId rowid = row.GetRowId();//随便用一个
              if(flag==0) {temp->InsertEntry(row,rowid,txn);//如果没有找到则插入一个
              temp->ScanKey(row,rows,txn);//再找到rows
              }
              //for(RowId i:rows){
              //  row_to_delete1.insert(i);
              //}
              //index_id_t iid = myMetadata->GetIndexId();
              //if(temp->ScanKey())
             if(length<=4){//这里要根据模板类型构造迭代器，通过迭代器获取rowid然后insert到row_to_delete里面
            BPlusTreeIndex<GenericKey<4>,RowId,GenericComparator<4>> * BPtree =
              (BPlusTreeIndex<GenericKey<4>,RowId,GenericComparator<4>>*)(temp);
              int status = 0;
              IndexIterator<GenericKey<4UL>, RowId, GenericComparator<4UL>> i;
              for(i = BPtree->GetBeginIterator();i!= BPtree->GetEndIterator();i.operator++()){
                for(int j =0;j<i.GetLeaf()->GetSize();j++){
                  
                  if(i.GetLeaf()->GetItem(j).second.operator==(row.GetRowId())) {
                    status = 1;
                }
                if (status==1) row_to_delete2.insert(i.GetLeaf()->GetItem(j).second);
              }
              }
              }
            //*****************************
           else if(length<=8){
             BPlusTreeIndex<GenericKey<8>,RowId,GenericComparator<8>> * BPtree =
                 (BPlusTreeIndex<GenericKey<8>,RowId,GenericComparator<8>>*)(temp);
                 int status = 0;
              IndexIterator<GenericKey<8UL>, RowId, GenericComparator<8UL>> i;
              for(i = BPtree->GetBeginIterator();i!= BPtree->GetEndIterator();i.operator++()){
                for(int j =0;j<i.GetLeaf()->GetSize();j++){
                  
                  if(i.GetLeaf()->GetItem(j).second.operator==(row.GetRowId())) {
                    status = 1;
                }
                if (status==1) row_to_delete2.insert(i.GetLeaf()->GetItem(j).second);
              }
              }
           }
           else if(length<=16){
             BPlusTreeIndex<GenericKey<16>,RowId,GenericComparator<16>> * BPtree =
                 (BPlusTreeIndex<GenericKey<16>,RowId,GenericComparator<16>>*)(temp);
                 int status = 0;
              IndexIterator<GenericKey<16UL>, RowId, GenericComparator<16UL>> i;
              for(i = BPtree->GetBeginIterator();i!= BPtree->GetEndIterator();i.operator++()){
                for(int j =0;j<i.GetLeaf()->GetSize();j++){
                  
                  if(i.GetLeaf()->GetItem(j).second.operator==(row.GetRowId())) {
                    status = 1;
                }
                if (status==1) row_to_delete2.insert(i.GetLeaf()->GetItem(j).second);
              }
              }
           }
           else if(length<=32){
             BPlusTreeIndex<GenericKey<32>,RowId,GenericComparator<32>> * BPtree =
                 (BPlusTreeIndex<GenericKey<32>,RowId,GenericComparator<32>>*)(temp);
                 int status = 0;
              IndexIterator<GenericKey<32UL>, RowId, GenericComparator<32UL>> i;
              for(i = BPtree->GetBeginIterator();i!= BPtree->GetEndIterator();i.operator++()){
                for(int j =0;j<i.GetLeaf()->GetSize();j++){
                  
                  if(i.GetLeaf()->GetItem(j).second.operator==(row.GetRowId())) {
                    status = 1;
                }
                if (status==1) row_to_delete2.insert(i.GetLeaf()->GetItem(j).second);
              }
              }
           }
           else {
             BPlusTreeIndex<GenericKey<64>,RowId,GenericComparator<64>> * BPtree =
                 (BPlusTreeIndex<GenericKey<64>,RowId,GenericComparator<64>>*)(temp);
                 int status = 0;
              IndexIterator<GenericKey<64UL>, RowId, GenericComparator<64UL>> i;
              for(i = BPtree->GetBeginIterator();i!= BPtree->GetEndIterator();i.operator++()){
                for(int j =0;j<i.GetLeaf()->GetSize();j++){
                  
                  if(i.GetLeaf()->GetItem(j).second.operator==(row.GetRowId())) {
                    status = 1;
                }
                if (status==1) row_to_delete2.insert(i.GetLeaf()->GetItem(j).second);
              }
              }
           }
                       if(flag==0) {
                         temp->RemoveEntry(row,rowid,txn);//如果是插入的那么要把这一条删除
                       row_to_delete2.erase(row.GetRowId());
                       }
            }
            //有索引的查找结束
            else {//无索引的情况 
                for(TableIterator iter = myTableHeap->Begin();iter!=myTableHeap->End();iter++){
                if(iter->GetField(ind)->BackValue()>=value_s){
                  row_to_delete2.insert(iter->GetRowId());//如果符合条件就加到这里面去
                }
              }
              }
            }
            else if(op1==4){
              int if_index = -1;//先找是否存在索引
            int num = 0;
            std::vector<IndexInfo*>::iterator it;
            for(it=myIndexInfo.begin();it!=myIndexInfo.end();it++){//遍历每一个index
              if_index = (*it)->GetIndexKeySchema()->getIDIndex(attrib);
              num = (*it)->GetIndexKeySchema()->GetColumns().size();
              if(if_index!=-1&&num==1){//如果存在索引而且索引只有一个column
                break;
              }
            }//找完了索引
            if (if_index!=-1){//如果有索引存在
              std::vector<uint32_t> key_map_of_this_index;
            IndexMetadata * myMetadata = (*it)->GetMetaData();
            key_map_of_this_index = myMetadata->GetKeyMapping();//有了这个keymapping之后，就可以挑选出自己的row
            IndexSchema * This_schema = (*it)->GetIndexKeySchema();//这里有了这个index需要的schema
            //int length = This_schema->GetSerializedSize();

            Index * temp = (*it)->GetIndex();
            TypeId t = This_schema->GetColumns()[if_index]->GetType();//需要构建一个field去找，先获得类型
            std::vector<Field> fields;
            if (t==kTypeChar){
            int length = This_schema->GetColumns()[if_index]->GetLength();
            Field field(This_schema->GetColumns()[if_index]->GetType(),(char *)value_s.data(),length,true);
            fields.push_back(field);
            }
            else if(t==kTypeFloat){
            Field field(This_schema->GetColumns()[if_index]->GetType(),stof((char *)value_s.data()));    
            fields.push_back(field);
            }
            else if(t==kTypeInt){
            Field field(This_schema->GetColumns()[if_index]->GetType(),stoi((char *)value_s.data()));    
              fields.push_back(field);
            }
            Row row(fields);
            Transaction * txn = nullptr;
            std::vector<RowId> rows;
            temp->ScanKey(row,rows,txn);//得到rows
            for(TableIterator iter = myTableHeap->Begin();iter!=myTableHeap->End();iter++){
                  row_to_delete2.insert(iter->GetRowId());//先全部取出来再用Index erase

              }
            for(RowId i:rows){
              row_to_delete2.erase(i);
            }
            }

              else{
              for(TableIterator iter = myTableHeap->Begin();iter!=myTableHeap->End();iter++){
                if(iter->GetField(ind)->BackValue()!=value_s){
                  row_to_delete2.insert(iter->GetRowId());//如果符合条件就加到这里面去
                }
              }
              }
            }
            else if(op1==5){
              int flag = 0;//存在索引的话在b+树里能不能找到
              int if_index = -1;//先找是否存在索引
            int num = 0;
            std::vector<IndexInfo*>::iterator it;
            for(it=myIndexInfo.begin();it!=myIndexInfo.end();it++){//遍历每一个index
              if_index = (*it)->GetIndexKeySchema()->getIDIndex(attrib);
              num = (*it)->GetIndexKeySchema()->GetColumns().size();
              if(if_index!=-1&&num==1){//如果存在索引而且索引只有一个column
                break;
              }
            }//找完了索引
            if (if_index!=-1){//如果有索引存在
              std::vector<uint32_t> key_map_of_this_index;
            IndexMetadata * myMetadata = (*it)->GetMetaData();
            key_map_of_this_index = myMetadata->GetKeyMapping();//有了这个keymapping之后，就可以挑选出自己的row
            IndexSchema * This_schema = (*it)->GetIndexKeySchema();//这里有了这个index需要的schema
            int length = This_schema->GetSerializedSize();//得到序列化长度

            Index * temp = (*it)->GetIndex();
            TypeId t = This_schema->GetColumns()[if_index]->GetType();//需要构建一个field去找，先获得类型
            std::vector<Field> fields;
            if (t==kTypeChar){
            int length1 = This_schema->GetColumns()[if_index]->GetLength();
            Field field(This_schema->GetColumns()[if_index]->GetType(),(char *)value_s.data(),length1,true);
            fields.push_back(field);
            }
            else if(t==kTypeFloat){
            Field field(This_schema->GetColumns()[if_index]->GetType(),stof((char *)value_s.data()));    
            fields.push_back(field);
            }
            else if(t==kTypeInt){
            Field field(This_schema->GetColumns()[if_index]->GetType(),stoi((char *)value_s.data()));    
              fields.push_back(field);
            }
            Row row(fields);
            Transaction * txn = nullptr;
            std::vector<RowId> rows;
            //temp->InsertEntry()
            if(temp->ScanKey(row,rows,txn)==DB_SUCCESS)  flag = 1;//如果找到了，并且获得了rows，目前先只考虑获得了rows的情况s

              RowId rowid = row.GetRowId();//随便用一个
              if(flag==0) {temp->InsertEntry(row,rowid,txn);//如果没有找到则插入一个
              temp->ScanKey(row,rows,txn);//再找到rows
              }
              //for(RowId i:rows){
              //  row_to_delete1.insert(i);
              //}
              //index_id_t iid = myMetadata->GetIndexId();
              //if(temp->ScanKey())
             if(length<=4){//这里要根据模板类型构造迭代器，通过迭代器获取rowid然后insert到row_to_delete里面
             BPlusTreeIndex<GenericKey<4>,RowId,GenericComparator<4>> * BPtree =
                 (BPlusTreeIndex<GenericKey<4>,RowId,GenericComparator<4>>*)(temp);
                 int status = 0;
              IndexIterator<GenericKey<4UL>, RowId, GenericComparator<4UL>> i;
              for(i = BPtree->GetBeginIterator();i!= BPtree->GetEndIterator();i.operator++()){
                for(int j =0;j<i.GetLeaf()->GetSize();j++){
                  row_to_delete2.insert(i.GetLeaf()->GetItem(j).second);
                  if(i.GetLeaf()->GetItem(j).second.operator==(row.GetRowId())) {
                    status = 1;
                    break;
                }
                if (status==1) break;
              }
              }
            }//*****************************
           else if(length<=8){
             BPlusTreeIndex<GenericKey<8>,RowId,GenericComparator<8>> * BPtree =
                 (BPlusTreeIndex<GenericKey<8>,RowId,GenericComparator<8>>*)(temp);
                 int status = 0;
              IndexIterator<GenericKey<8UL>, RowId, GenericComparator<8UL>> i;
              for(i = BPtree->GetBeginIterator();i!= BPtree->GetEndIterator();i.operator++()){
                for(int j =0;j<i.GetLeaf()->GetSize();j++){
                  row_to_delete2.insert(i.GetLeaf()->GetItem(j).second);
                  if(i.GetLeaf()->GetItem(j).second.operator==(row.GetRowId())) {
                    status = 1;
                    break;
                }
                if (status==1) break;
              }
              }
           }
           else if(length<=16){
             BPlusTreeIndex<GenericKey<16>,RowId,GenericComparator<16>> * BPtree =
                 (BPlusTreeIndex<GenericKey<16>,RowId,GenericComparator<16>>*)(temp);
                 int status = 0;
              IndexIterator<GenericKey<16UL>, RowId, GenericComparator<16UL>> i;
              for(i = BPtree->GetBeginIterator();i!= BPtree->GetEndIterator();i.operator++()){
                for(int j =0;j<i.GetLeaf()->GetSize();j++){
                  row_to_delete2.insert(i.GetLeaf()->GetItem(j).second);
                  if(i.GetLeaf()->GetItem(j).second.operator==(row.GetRowId())) {
                    status = 1;
                    break;
                }
                if (status==1) break;
              }
              }
           }
           else if(length<=32){
             BPlusTreeIndex<GenericKey<32>,RowId,GenericComparator<32>> * BPtree =
                 (BPlusTreeIndex<GenericKey<32>,RowId,GenericComparator<32>>*)(temp);
                 int status = 0;
              IndexIterator<GenericKey<32UL>, RowId, GenericComparator<32UL>> i;
              for(i = BPtree->GetBeginIterator();i!= BPtree->GetEndIterator();i.operator++()){
                for(int j =0;j<i.GetLeaf()->GetSize();j++){
                  row_to_delete2.insert(i.GetLeaf()->GetItem(j).second);
                  if(i.GetLeaf()->GetItem(j).second.operator==(row.GetRowId())) {
                    status = 1;
                    break;
                }
                if (status==1) break;
              }
              }
           }
           else {
             BPlusTreeIndex<GenericKey<64>,RowId,GenericComparator<64>> * BPtree =
                 (BPlusTreeIndex<GenericKey<64>,RowId,GenericComparator<64>>*)(temp);
                 int status = 0;
              IndexIterator<GenericKey<64UL>, RowId, GenericComparator<64UL>> i;
              for(i = BPtree->GetBeginIterator();i!= BPtree->GetEndIterator();i.operator++()){
                for(int j =0;j<i.GetLeaf()->GetSize();j++){
                  row_to_delete2.insert(i.GetLeaf()->GetItem(j).second);
                  if(i.GetLeaf()->GetItem(j).second.operator==(row.GetRowId())) {
                    status = 1;
                    break;
                }
                if (status==1) break;
              }
              }
           }
                       if(flag==0) {
                         temp->RemoveEntry(row,rowid,txn);//如果是插入的那么要把这一条删除
                       }
                       row_to_delete2.erase(row.GetRowId());
            }
            //有索引的查找结束
            else {//无索引的情况 
                for(TableIterator iter = myTableHeap->Begin();iter!=myTableHeap->End();iter++){
                if(iter->GetField(ind)->BackValue()<value_s){
                  row_to_delete2.insert(iter->GetRowId());//如果符合条件就加到这里面去
                }
              }
              }
            }
            else if(op1==6){
                            int flag = 0;//存在索引的话在b+树里能不能找到
              int if_index = -1;//先找是否存在索引
            int num = 0;
            std::vector<IndexInfo*>::iterator it;
            for(it=myIndexInfo.begin();it!=myIndexInfo.end();it++){//遍历每一个index
              if_index = (*it)->GetIndexKeySchema()->getIDIndex(attrib);
              num = (*it)->GetIndexKeySchema()->GetColumns().size();
              if(if_index!=-1&&num==1){//如果存在索引而且索引只有一个column
                break;
              }
            }//找完了索引
            if (if_index!=-1){//如果有索引存在
              std::vector<uint32_t> key_map_of_this_index;
            IndexMetadata * myMetadata = (*it)->GetMetaData();
            key_map_of_this_index = myMetadata->GetKeyMapping();//有了这个keymapping之后，就可以挑选出自己的row
            IndexSchema * This_schema = (*it)->GetIndexKeySchema();//这里有了这个index需要的schema
            int length = This_schema->GetSerializedSize();//得到序列化长度

            Index * temp = (*it)->GetIndex();
            TypeId t = This_schema->GetColumns()[if_index]->GetType();//需要构建一个field去找，先获得类型
            std::vector<Field> fields;
            if (t==kTypeChar){
            int length1 = This_schema->GetColumns()[if_index]->GetLength();
            Field field(This_schema->GetColumns()[if_index]->GetType(),(char *)value_s.data(),length1,true);
            fields.push_back(field);
            }
            else if(t==kTypeFloat){
            Field field(This_schema->GetColumns()[if_index]->GetType(),stof((char *)value_s.data()));    
            fields.push_back(field);
            }
            else if(t==kTypeInt){
            Field field(This_schema->GetColumns()[if_index]->GetType(),stoi((char *)value_s.data()));    
              fields.push_back(field);
            }
            Row row(fields);
            Transaction * txn = nullptr;
            std::vector<RowId> rows;
            //temp->InsertEntry()
            if(temp->ScanKey(row,rows,txn)==DB_SUCCESS)  flag = 1;//如果找到了，并且获得了rows，目前先只考虑获得了rows的情况s

              RowId rowid = row.GetRowId();//随便用一个
              if(flag==0) {temp->InsertEntry(row,rowid,txn);//如果没有找到则插入一个
              temp->ScanKey(row,rows,txn);//再找到rows
              }
              //for(RowId i:rows){
              //  row_to_delete1.insert(i);
              //}
              //index_id_t iid = myMetadata->GetIndexId();
              //if(temp->ScanKey())
             if(length<=4){//这里要根据模板类型构造迭代器，通过迭代器获取rowid然后insert到row_to_delete里面
            BPlusTreeIndex<GenericKey<4>,RowId,GenericComparator<4>> * BPtree =
              (BPlusTreeIndex<GenericKey<4>,RowId,GenericComparator<4>>*)(temp);
              int status = 0;
              IndexIterator<GenericKey<4UL>, RowId, GenericComparator<4UL>> i;
              for(i = BPtree->GetBeginIterator();i!= BPtree->GetEndIterator();i.operator++()){
                for(int j =0;j<i.GetLeaf()->GetSize();j++){
                  
                  if(i.GetLeaf()->GetItem(j).second.operator==(row.GetRowId())) {
                    status = 1;
                }
                if (status==1) row_to_delete2.insert(i.GetLeaf()->GetItem(j).second);
              }
              }
              }
            //*****************************
           else if(length<=8){
             BPlusTreeIndex<GenericKey<8>,RowId,GenericComparator<8>> * BPtree =
                 (BPlusTreeIndex<GenericKey<8>,RowId,GenericComparator<8>>*)(temp);
                 int status = 0;
              IndexIterator<GenericKey<8UL>, RowId, GenericComparator<8UL>> i;
              for(i = BPtree->GetBeginIterator();i!= BPtree->GetEndIterator();i.operator++()){
                for(int j =0;j<i.GetLeaf()->GetSize();j++){
                  
                  if(i.GetLeaf()->GetItem(j).second.operator==(row.GetRowId())) {
                    status = 1;
                }
                if (status==1) row_to_delete2.insert(i.GetLeaf()->GetItem(j).second);
              }
              }
           }
           else if(length<=16){
             BPlusTreeIndex<GenericKey<16>,RowId,GenericComparator<16>> * BPtree =
                 (BPlusTreeIndex<GenericKey<16>,RowId,GenericComparator<16>>*)(temp);
                 int status = 0;
              IndexIterator<GenericKey<16UL>, RowId, GenericComparator<16UL>> i;
              for(i = BPtree->GetBeginIterator();i!= BPtree->GetEndIterator();i.operator++()){
                for(int j =0;j<i.GetLeaf()->GetSize();j++){
                  
                  if(i.GetLeaf()->GetItem(j).second.operator==(row.GetRowId())) {
                    status = 1;
                }
                if (status==1) row_to_delete2.insert(i.GetLeaf()->GetItem(j).second);
              }
              }
           }
           else if(length<=32){
             BPlusTreeIndex<GenericKey<32>,RowId,GenericComparator<32>> * BPtree =
                 (BPlusTreeIndex<GenericKey<32>,RowId,GenericComparator<32>>*)(temp);
                 int status = 0;
              IndexIterator<GenericKey<32UL>, RowId, GenericComparator<32UL>> i;
              for(i = BPtree->GetBeginIterator();i!= BPtree->GetEndIterator();i.operator++()){
                for(int j =0;j<i.GetLeaf()->GetSize();j++){
                  
                  if(i.GetLeaf()->GetItem(j).second.operator==(row.GetRowId())) {
                    status = 1;
                }
                if (status==1) row_to_delete2.insert(i.GetLeaf()->GetItem(j).second);
              }
              }
           }
           else {
             BPlusTreeIndex<GenericKey<64>,RowId,GenericComparator<64>> * BPtree =
                 (BPlusTreeIndex<GenericKey<64>,RowId,GenericComparator<64>>*)(temp);
                 int status = 0;
              IndexIterator<GenericKey<64UL>, RowId, GenericComparator<64UL>> i;
              for(i = BPtree->GetBeginIterator();i!= BPtree->GetEndIterator();i.operator++()){
                for(int j =0;j<i.GetLeaf()->GetSize();j++){
                  
                  if(i.GetLeaf()->GetItem(j).second.operator==(row.GetRowId())) {
                    status = 1;
                }
                if (status==1) row_to_delete2.insert(i.GetLeaf()->GetItem(j).second);
              }
              }
           }
                       if(flag==0) {
                         temp->RemoveEntry(row,rowid,txn);//如果是插入的那么要把这一条删除
                       }
                       row_to_delete2.erase(row.GetRowId());
            }
            //有索引的查找结束
            else {//无索引的情况 
                for(TableIterator iter = myTableHeap->Begin();iter!=myTableHeap->End();iter++){
                if(iter->GetField(ind)->BackValue()>value_s){
                  row_to_delete2.insert(iter->GetRowId());//如果符合条件就加到这里面去
                }
              }
              }
            }
            else if(op1==7){
              for(TableIterator iter = myTableHeap->Begin();iter!=myTableHeap->End();iter++){
                if(iter->GetField(ind)->IsNull()){// is null
                  row_to_delete2.insert(iter->GetRowId());//如果符合条件就加到这里面去
                }
              }
            }
            else if(op1==8){
              for(TableIterator iter = myTableHeap->Begin();iter!=myTableHeap->End();iter++){
                if(!(iter->GetField(ind)->IsNull())){//非null
                  row_to_delete2.insert(iter->GetRowId());//如果符合条件就加到这里面去
                }
              }
            }
            else{

            }
            myStack.pop();
          }
          else if(myStack.top()->type_==kNodeConnector){
            std::string temp1 = myStack.top()->val_;
            if(temp1=="and"){
              set_intersection(row_to_delete1.begin(),row_to_delete1.end(),row_to_delete2.begin(),row_to_delete2.end(),
                               inserter(result,result.begin()));//两个取交集然后放到result里
              row_to_delete1.clear();
              row_to_delete1 = result;//然后再把结果放到第一个set里面
              result.clear();
              row_to_delete2.clear();
              myStack.pop();
            }
            else{//也就是or
              for_or.push(row_to_delete1);//思路是将第一个集合压栈，然后让第二个操作数到第一个set里
              row_to_delete1.clear();
              row_to_delete1 = row_to_delete2;
              row_to_delete2.clear();
              myStack.pop();
            }
          }

        }
      }
        //找到了所有的需要输出的rowid，都储存在row-to-delete1里
        vector<Row> row_to_output;
        vector<int> map_of_output;//用ast来判断要输出的是哪几个属性
        if(ast->type_==kNodeAllColumns){
          int t = table_info1->GetSchema()->GetColumnCount();
          for(int i = 0;i<t;i++ ){
            map_of_output.push_back(i);
          }

        }
        else{//就得根据keymap来决定输出什么
          ast = ast->child_;
          string name = ast->val_;
          int t = table_info1->GetSchema()->getIDIndex(name);
          map_of_output.push_back(t);
          ast = ast->next_;
          while(ast){
            string name1 = ast->val_;
            int t1 = table_info1->GetSchema()->getIDIndex(name1);
            map_of_output.push_back(t1);
            ast = ast->next_;
          }
        }

        //得到需要输出的属性的下标


        for(TableIterator iter = myTableHeap->Begin();iter!=myTableHeap->End();iter++){
          std::set<RowId>::iterator it1 = row_to_delete1.find(iter->GetRowId());
          if(it1!=row_to_delete1.end()){
              for(auto i:map_of_output){
                cout<<iter->GetField(i)<<"  ";
            }
            cout<<endl;
          }
        }
      return DB_SUCCESS;
    }
    else {//如果table没找到
      LOG(INFO)<<"table not exist"<<std::endl;
      return DB_TABLE_NOT_EXIST;
    }
    
  }
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteInsert(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteInsert" << std::endl;
#endif
  //思路：首先判断根是否为insert，然后向下找到儿子节点判断是哪个table（table——name），然后是兄弟节点column-values，其子节点
  //为所有的属性。
  if(ast->type_==kNodeInsert){//判断是否为插入问题，改正
    std::string table_name;
    ast = ast->child_;//来到table节点
    table_name = ast->val_;//得到table的名字
    //auto db_01 = new DBStorageEngine(this->current_db_, false);  //加载数据库
    DBStorageEngine * db_01 = dbs_[current_db_]; //获得当前的db
    auto &catalog_01 = db_01->catalog_mgr_;;
    TableInfo* table_info1 ;
    int i=0;

    TableHeap * myTableHeap;//获取堆表
    vector<Field> myField;//建立一个Field的vector，然后把读到的东西往里面传入

    if(catalog_01->GetTable(table_name,table_info1)==DB_SUCCESS){//如果该数据库中确实存在该table
      std::vector<Column *> myColumns;
      myColumns = table_info1->GetSchema()->GetColumns();
      pSyntaxNode child = ast->next_;//开始判断每一个数据节点的类型
      child = child->child_;
      while(child){
        //TypeId make_sure = myColumns[i]->GetType();//获取当前table的每一个column的属性，与当前节点的属性进行比较
        if(child->type_==kNodeNumber){//如果是数字类型的数据
          std::string content = child->val_;
//          typeid(child->type_)== typeid(int)
          float a = atoi(content.c_str());
          if((int)a==a){//如果在进行了强制类型转换之后仍然相等，那么说明是int型
            Field t(TypeId::kTypeInt,(int)a);
            myField.push_back(t);//赋初始值后加到vector里
          }
          else{//说明是float类型的
            Field t(TypeId::kTypeFloat,a);
            myField.push_back(t);
          }
        }
        else if(child->type_==kNodeString){//也就是char类型的node
          Field t (TypeId::kTypeChar,child->val_,sizeof(child->val_),true);//注意一下manage-data的含义
          myField.push_back(t);
        }
        else{//也就是空值
          //std::vector<Column *> myColumns = table_info1->GetSchema()->GetColumns();
          TypeId type_of_null = myColumns[i]->GetType();
          if(myColumns[i]->IsNullable()){//就是该元素可以为空
            Field t(type_of_null);
            myField.push_back(t);
          }
          else return DB_FAILED;//不然就直接无法插入
        }
        child=child->next_;//继续寻找下一个元素值
        i++;
      }//当跳出循环时，Myfield构建完毕，可以进行插入了

    }
    else {
      LOG(INFO)<<"table not exist"<<std::endl;
      return DB_TABLE_NOT_EXIST;}
    Row a (myField);
    Transaction* txn = nullptr;
    myTableHeap = table_info1->GetTableHeap();
    myTableHeap->InsertTuple(a,txn);//到这里，table的插入算是完毕了，还有考虑b+tree的插入
    //当这里insert-tuple进行完之后，自动会将rowid更新，所以这一点不用管

    //首先得知道到底有没有index
    vector<IndexInfo*> index_of_this_table;
   // IndexInfo* index_info1;
    //index_info1.
    catalog_01->GetTableIndexes(table_name,index_of_this_table);
    vector<IndexInfo*>::iterator it;
    if(!index_of_this_table.empty()){//如果这个表确实存在index，那么无论插入什么tuple都要更新B+tree
      for(it=index_of_this_table.begin();it!=index_of_this_table.end();it++){
        //就是我首先得判断需要转换成的B+tree index的模板类型
        std::vector<uint32_t> key_map_of_this_index;
        IndexMetadata * myMetadata = (*it)->GetMetaData();
        key_map_of_this_index = myMetadata->GetKeyMapping();//有了这个keymapping之后，就可以挑选出自己的row

       // IndexSchema * This_schema = (*it)->GetIndexKeySchema();//这里有了这个index需要的schema
       // int length = This_schema->GetSerializedSize();

        Index * temp = (*it)->GetIndex();
        std::vector<Field> Field_of_this_index;
        //std::vector<Column *> columns_of_this_index;
        //columns_of_this_index = This_schema->GetColumns();
        for(auto j:key_map_of_this_index){
          Field_of_this_index.push_back(myField[j]);//根据keymap来填充要加到B+树里的fields
        }

        Row row_of_this_index(Field_of_this_index);
        temp->InsertEntry(row_of_this_index,a.GetRowId(), nullptr);
      }
    }

    return DB_SUCCESS;

  }

  return DB_FAILED;
}




dberr_t ExecuteEngine::ExecuteDelete(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDelete" << std::endl;
#endif
  //思路，先在对应的table的tableheap里进行删除，然后再对index进行删除
  if(ast->type_==kNodeDelete){//首先判断根节点是不是delete的指令
    ast = ast->child_;//到达表节点
    std::string table_name = ast->val_;//获取表的名称
    DBStorageEngine * db_01 = dbs_[current_db_]; //获得当前的db
    //auto db_01 = new DBStorageEngine(this->current_db_, false);  //加载数据库
    auto &catalog_01 = db_01->catalog_mgr_;;
    TableInfo * tableInfo1;
    if(catalog_01->GetTable(table_name,tableInfo1)!=DB_SUCCESS){
      LOG(INFO)<<"TABLE NOT EXIST"<<std::endl;
      return DB_TABLE_NOT_EXIST;
    };
    TableHeap * myTableHeap;
    myTableHeap = tableInfo1->GetTableHeap();//不考虑roll back的情况，直接进行删除
    std::stack<pSyntaxNode> myStack;

    ast = ast->next_;//到达判断条件节点
    ast = ast->child_;//到达最后一个连接条件,准备开始入栈
    myStack.push(ast);//连接条件入栈
    //因为有可能只有一个选择条件，那就不存在连接条件，所以得判断一下
    if(ast->type_==kNodeConnector){//这种情况就是存在连接条件
      while(ast->child_->type_==kNodeConnector){
        ast = ast->child_;
        myStack.push(ast->next_);//operator入栈
        myStack.push(ast);//connector入栈
      }
      //如果child不再是connector，说明连接条件已经没有了，接下来都是operator了

    }
    if(ast->child_->type_==kNodeCompareOperator){
      ast = ast->child_;
      myStack.push(ast->next_);
      myStack.push(ast);//到这里，选择条件已全部入栈
    }


    int flagg=0;
    std::set<RowId> row_to_delete1;
    std::set<RowId> row_to_delete2;
    std::set<RowId> result;
    std::stack<set<RowId>>  for_or;
    while(!myStack.empty()){//只要不是空的就会一直循环
      if(flagg==0&&myStack.top()->type_== kNodeCompareOperator){
        pSyntaxNode temp1 = myStack.top();//接收其中一个operator
        int op1 = 0;
        op1 = judge_op_node_type(temp1);
      //  myStack.pop();
        std::string attrib = temp1->child_->val_;//需要根据属性得到是哪个field
        std::string value_s = temp1->child_->next_->val_;
        Schema * now_Schema = tableInfo1->GetSchema();
        int ind = now_Schema->getIDIndex(attrib);//得到对应的下标

        if(op1==1){
          for(TableIterator iter = myTableHeap->Begin(); iter!= myTableHeap->End();iter++){
            if(iter->GetField(ind)->BackValue()==value_s){
              row_to_delete1.insert(iter->GetRowId());//如果符合条件就加到这里面去
            }
          }
        }
        else if(op1==2){
          for(TableIterator iter = myTableHeap->Begin();iter!=myTableHeap->End();iter++){
            if(iter->GetField(ind)->BackValue()<=value_s){
              row_to_delete1.insert(iter->GetRowId());//如果符合条件就加到这里面去
            }
          }
        }
        else if(op1==3){
          for(TableIterator iter = myTableHeap->Begin();iter!=myTableHeap->End();iter++){
            if(iter->GetField(ind)->BackValue()>=value_s){
              row_to_delete1.insert(iter->GetRowId());//如果符合条件就加到这里面去
            }
          }
        }
        else if(op1==4){
          for(TableIterator iter = myTableHeap->Begin();iter!=myTableHeap->End();iter++){
            if(iter->GetField(ind)->BackValue()!=value_s){
              row_to_delete1.insert(iter->GetRowId());//如果符合条件就加到这里面去
            }
          }
        }
        else if(op1==5){
          for(TableIterator iter = myTableHeap->Begin();iter!=myTableHeap->End();iter++){
            if(iter->GetField(ind)->BackValue()<value_s){
              row_to_delete1.insert(iter->GetRowId());//如果符合条件就加到这里面去
            }
          }
        }
        else if(op1==6){
          for(TableIterator iter = myTableHeap->Begin();iter!=myTableHeap->End();iter++){
            if(iter->GetField(ind)->BackValue()>value_s){
              row_to_delete1.insert(iter->GetRowId());//如果符合条件就加到这里面去
            }
          }
        }
        else if(op1==7){
          for(TableIterator iter = myTableHeap->Begin();iter!=myTableHeap->End();iter++){
            if(iter->GetField(ind)->IsNull()){// is null
              row_to_delete1.insert(iter->GetRowId());//如果符合条件就加到这里面去
            }
          }
        }
        else if(op1==8){
          for(TableIterator iter = myTableHeap->Begin();iter!=myTableHeap->End();iter++){
            if(!(iter->GetField(ind)->IsNull())){//非null
              row_to_delete1.insert(iter->GetRowId());//如果符合条件就加到这里面去
            }
          }
        }
        else{

        }
        myStack.pop();//将栈最上层的元素弹出
        flagg = 1;
      }
      else if(flagg==1&&myStack.top()->type_== kNodeCompareOperator){
        pSyntaxNode temp1 = myStack.top();//接收其中一个operator
        int op1 = 0;
        op1 = judge_op_node_type(temp1);
        myStack.pop();
        std::string attrib = temp1->child_->val_;//需要根据属性得到是哪个field
        std::string value_s = temp1->child_->next_->val_;
        Schema * now_Schema = tableInfo1->GetSchema();
        int ind = now_Schema->getIDIndex(attrib);//得到对应的下标

        if(op1==1){
          for(TableIterator iter = myTableHeap->Begin();iter!=myTableHeap->End();iter++){
            if(iter->GetField(ind)->BackValue()==value_s){
              row_to_delete2.insert(iter->GetRowId());//如果符合条件就加到这里面去
            }
          }
        }
        else if(op1==2){
          for(TableIterator iter = myTableHeap->Begin();iter!=myTableHeap->End();iter++){
            if(iter->GetField(ind)->BackValue()<=value_s){
              row_to_delete2.insert(iter->GetRowId());//如果符合条件就加到这里面去
            }
          }
        }
        else if(op1==3){
          for(TableIterator iter = myTableHeap->Begin();iter!=myTableHeap->End();iter++){
            if(iter->GetField(ind)->BackValue()>=value_s){
              row_to_delete2.insert(iter->GetRowId());//如果符合条件就加到这里面去
            }
          }
        }
        else if(op1==4){
          for(TableIterator iter = myTableHeap->Begin();iter!=myTableHeap->End();iter++){
            if(iter->GetField(ind)->BackValue()!=value_s){
              row_to_delete2.insert(iter->GetRowId());//如果符合条件就加到这里面去
            }
          }
        }
        else if(op1==5){
          for(TableIterator iter = myTableHeap->Begin();iter!=myTableHeap->End();iter++){
            if(iter->GetField(ind)->BackValue()<value_s){
              row_to_delete2.insert(iter->GetRowId());//如果符合条件就加到这里面去
            }
          }
        }
        else if(op1==6){
          for(TableIterator iter = myTableHeap->Begin();iter!=myTableHeap->End();iter++){
            if(iter->GetField(ind)->BackValue()>value_s){
              row_to_delete2.insert(iter->GetRowId());//如果符合条件就加到这里面去
            }
          }
        }
        else if(op1==7){
          for(TableIterator iter = myTableHeap->Begin();iter!=myTableHeap->End();iter++){
            if(iter->GetField(ind)->IsNull()){// is null
              row_to_delete2.insert(iter->GetRowId());//如果符合条件就加到这里面去
            }
          }
        }
        else if(op1==8){
          for(TableIterator iter = myTableHeap->Begin();iter!=myTableHeap->End();iter++){
            if(!(iter->GetField(ind)->IsNull())){//非null
              row_to_delete2.insert(iter->GetRowId());//如果符合条件就加到这里面去
            }
          }
        }
        else{

        }
        myStack.pop();
      }
      else if(myStack.top()->type_==kNodeConnector){
        std::string temp1 = myStack.top()->val_;
        if(temp1=="and"){
          set_intersection(row_to_delete1.begin(),row_to_delete1.end(),row_to_delete2.begin(),row_to_delete2.end(),
                           inserter(result,result.begin()));//两个取交集然后放到result里
          row_to_delete1.clear();
          row_to_delete1 = result;//然后再把结果放到第一个set里面
          result.clear();
          row_to_delete2.clear();
          myStack.pop();
        }
        else{//也就是or
          for_or.push(row_to_delete1);//思路是将第一个集合压栈，然后让第二个操作数到第一个set里
          row_to_delete1.clear();
          row_to_delete1 = row_to_delete2;
          row_to_delete2.clear();
          myStack.pop();
        }
      }

    }

    for_or.push(row_to_delete1);
    row_to_delete1.clear();

    row_to_delete1 = for_or.top();
    for_or.pop();
    while(!for_or.empty()){
      row_to_delete2 = for_or.top();
      set_union(row_to_delete1.begin(),row_to_delete1.end(),row_to_delete2.begin(),row_to_delete2.end(),
                inserter(result,result.begin()));
      row_to_delete1.clear();
      row_to_delete1 = result;
      result.clear();
      row_to_delete2.clear();
      for_or.pop();
    }


//    vector<vector<Field*>> myFields;
//
//    for(TableIterator iter = myTableHeap->Begin(nullptr);iter!=myTableHeap->End();iter++){
//      set<RowId>::iterator it = row_to_delete1.find(iter->GetRowId());
//      if(it!=row_to_delete1.end()){
//        myFields.push_back(iter->GetFields());
//      }
//    }


    //vector<Row> myRow;

    std::vector<IndexInfo*> index_of_this_table;//先尝试用索引进行查找
    // IndexInfo* index_info1;
    //index_info1.
    catalog_01->GetTableIndexes(table_name,index_of_this_table);
    std::vector<IndexInfo*>::iterator it;
    if(!index_of_this_table.empty()){//如果这个表确实存在index
      for(it=index_of_this_table.begin();it!=index_of_this_table.end();it++){
        //就是我首先得判断需要转换成的B+tree index的模板类型
        std::vector<uint32_t> key_map_of_this_index;
        IndexMetadata * myMetadata = (*it)->GetMetaData();
        key_map_of_this_index = myMetadata->GetKeyMapping();//有了这个keymapping之后，就可以挑选出自己的row

       // IndexSchema * This_schema = (*it)->GetIndexKeySchema();//这里有了这个index需要的schema
       // int length = This_schema->GetSerializedSize();

        Index * temp = (*it)->GetIndex();

        for(auto i:row_to_delete1){//这个循环的目的是删除掉指定条数的index
          std::vector<Field> Field_of_this_index;
          std::vector<Field> myField;//首先得利用i，把原有的field都填充到myField里
          for(TableIterator iter = myTableHeap->Begin();iter!=myTableHeap->End();iter++){
            std::set<RowId>::iterator it1 = row_to_delete1.find(iter->GetRowId());
            if(it1!=row_to_delete1.end()){
              for(size_t l = 0;l<iter->GetFieldCount();l++) {
                myField.push_back(*(iter->GetField(l)));
              }
            }
          }
          for(auto j:key_map_of_this_index){

            Field_of_this_index.push_back(myField[j]);//根据keymap来填充要加到B+树里的fields
          }

          Row row_of_this_index(Field_of_this_index);

          temp->RemoveEntry(row_of_this_index,i, nullptr);

//          if(length<=4){
//            BPlusTreeIndex<GenericKey<4>,RowId,GenericComparator<4>> * BPtree =
//                (BPlusTreeIndex<GenericKey<4>,RowId,GenericComparator<4>>*)(temp);
//            BPtree->RemoveEntry(row_of_this_index,i, nullptr);
//
//            //这里进行强转，（本来就是用子类进行初始化的）
//          }
//          else if(length<=8){
//            BPlusTreeIndex<GenericKey<8>,RowId,GenericComparator<8>> * BPtree =
//                (BPlusTreeIndex<GenericKey<8>,RowId,GenericComparator<8>>*)(temp);
//            BPtree->RemoveEntry(row_of_this_index,i, nullptr);
//          }
//          else if(length<=16){
//            BPlusTreeIndex<GenericKey<16>,RowId,GenericComparator<16>> * BPtree =
//                (BPlusTreeIndex<GenericKey<16>,RowId,GenericComparator<16>>*)(temp);
//            BPtree->RemoveEntry(row_of_this_index,i, nullptr);
//          }
//          else if(length<=32){
//            BPlusTreeIndex<GenericKey<32>,RowId,GenericComparator<32>> * BPtree =
//                (BPlusTreeIndex<GenericKey<32>,RowId,GenericComparator<32>>*)(temp);
//            BPtree->RemoveEntry(row_of_this_index,i, nullptr);
//          }
//          else {//length<=64,最多支持64字节
//            BPlusTreeIndex<GenericKey<64>,RowId,GenericComparator<64>> * BPtree =
//                (BPlusTreeIndex<GenericKey<64>,RowId,GenericComparator<64>>*)(temp);
//            BPtree->RemoveEntry(row_of_this_index,i, nullptr);
//          }
        }
        //std::vector<Column *> columns_of_this_index;
        //columns_of_this_index = This_schema->GetColumns();

      }
    }

    for(auto i:row_to_delete1){
      myTableHeap->MarkDelete(i, nullptr);
      myTableHeap->ApplyDelete(i, nullptr);
    }//最后再将tableheap里的内容删除

    return DB_SUCCESS;
  }
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteUpdate(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUpdate" << std::endl;
#endif

  //思路：应该是整两套row，一套旧的，一套新的，然后根据具体的index来决定再怎么创建新的key进行插入
  if(ast->type_== kNodeUpdate){
    ast = ast->child_;//来到table节点
    string table_name = ast->val_;
    string DB_name = this->current_db_;
    auto db_01 = this->dbs_[DB_name];
    //auto db_01 = new DBStorageEngine(this->current_db_, false);  //加载数据库
    auto &catalog_01 = db_01->catalog_mgr_;
    TableInfo * tableInfo1;
    if(catalog_01->GetTable(table_name,tableInfo1)!=DB_SUCCESS){
      LOG(INFO)<<"table not exists"<<std::endl;
    };
    TableHeap * myTableHeap;
    myTableHeap = tableInfo1->GetTableHeap();//得到对应表的堆表
    ast = ast->next_;//来到转折点
    pSyntaxNode condition = ast->next_;
    condition = condition->child_;
    std::stack<pSyntaxNode> myStack;//用这个栈来存储条件信息
    myStack.push(condition);
    if(condition->type_==kNodeConnector){//如果存在连接条件
      while(condition->child_->type_==kNodeConnector){
        condition = condition->child_;
        myStack.push(condition->next_);//operator入栈
        myStack.push(condition);//connector入栈
      }
    }
    if(condition->child_->type_==kNodeCompareOperator){
      condition = condition->child_;
      myStack.push(condition->next_);
      myStack.push(condition);//到这里，选择条件已全部入栈
    }

    int flag=0;
    std::set<RowId> row_to_update1;
    std::set<RowId> row_to_update2;
    std::set<RowId> result;
    std::stack<set<RowId>>  for_or;
    while(!myStack.empty()){//只要不是空的就会一直循环
      if(flag==0&&myStack.top()->type_== kNodeCompareOperator){
        pSyntaxNode temp1 = myStack.top();//接收其中一个operator
        int op1 = 0;
        op1 = judge_op_node_type(temp1);
        //  myStack.pop();
        std::string attrib = temp1->child_->val_;//需要根据属性得到是哪个field
        std::string value_s = temp1->child_->next_->val_;
        Schema * now_Schema = tableInfo1->GetSchema();
        int ind = now_Schema->getIDIndex(attrib);//得到对应的下标

        if(op1==1){
          for(TableIterator iter = myTableHeap->Begin(); iter!= myTableHeap->End();iter++){
            if(iter->GetField(ind)->BackValue()==value_s){
              row_to_update1.insert(iter->GetRowId());//如果符合条件就加到这里面去
            }
          }
        }
        else if(op1==2){
          for(TableIterator iter = myTableHeap->Begin();iter!=myTableHeap->End();iter++){
            if(iter->GetField(ind)->BackValue()<=value_s){
              row_to_update1.insert(iter->GetRowId());//如果符合条件就加到这里面去
            }
          }
        }
        else if(op1==3){
          for(TableIterator iter = myTableHeap->Begin();iter!=myTableHeap->End();iter++){
            if(iter->GetField(ind)->BackValue()>=value_s){
              row_to_update1.insert(iter->GetRowId());//如果符合条件就加到这里面去
            }
          }
        }
        else if(op1==4){
          for(TableIterator iter = myTableHeap->Begin();iter!=myTableHeap->End();iter++){
            if(iter->GetField(ind)->BackValue()!=value_s){
              row_to_update1.insert(iter->GetRowId());//如果符合条件就加到这里面去
            }
          }
        }
        else if(op1==5){
          for(TableIterator iter = myTableHeap->Begin();iter!=myTableHeap->End();iter++){
            if(iter->GetField(ind)->BackValue()<value_s){
              row_to_update1.insert(iter->GetRowId());//如果符合条件就加到这里面去
            }
          }
        }
        else if(op1==6){
          for(TableIterator iter = myTableHeap->Begin();iter!=myTableHeap->End();iter++){
            if(iter->GetField(ind)->BackValue()>value_s){
              row_to_update1.insert(iter->GetRowId());//如果符合条件就加到这里面去
            }
          }
        }
        else if(op1==7){
          for(TableIterator iter = myTableHeap->Begin();iter!=myTableHeap->End();iter++){
            if(iter->GetField(ind)->IsNull()){// is null
              row_to_update1.insert(iter->GetRowId());//如果符合条件就加到这里面去
            }
          }
        }
        else if(op1==8){
          for(TableIterator iter = myTableHeap->Begin();iter!=myTableHeap->End();iter++){
            if(!(iter->GetField(ind)->IsNull())){//非null
              row_to_update1.insert(iter->GetRowId());//如果符合条件就加到这里面去
            }
          }
        }
        else{

        }
        myStack.pop();//将栈最上层的元素弹出
        flag = 1;
      }
      else if(flag==1&&myStack.top()->type_== kNodeCompareOperator){
        pSyntaxNode temp1 = myStack.top();//接收其中一个operator
        int op1 = 0;
        op1 = judge_op_node_type(temp1);
        myStack.pop();
        std::string attrib = temp1->child_->val_;//需要根据属性得到是哪个field
        std::string value_s = temp1->child_->next_->val_;
        Schema * now_Schema = tableInfo1->GetSchema();
        int ind = now_Schema->getIDIndex(attrib);//得到对应的下标

        if(op1==1){
          for(TableIterator iter = myTableHeap->Begin();iter!=myTableHeap->End();iter++){
            if(iter->GetField(ind)->BackValue()==value_s){
              row_to_update2.insert(iter->GetRowId());//如果符合条件就加到这里面去
            }
          }
        }
        else if(op1==2){
          for(TableIterator iter = myTableHeap->Begin();iter!=myTableHeap->End();iter++){
            if(iter->GetField(ind)->BackValue()<=value_s){
              row_to_update2.insert(iter->GetRowId());//如果符合条件就加到这里面去
            }
          }
        }
        else if(op1==3){
          for(TableIterator iter = myTableHeap->Begin();iter!=myTableHeap->End();iter++){
            if(iter->GetField(ind)->BackValue()>=value_s){
              row_to_update2.insert(iter->GetRowId());//如果符合条件就加到这里面去
            }
          }
        }
        else if(op1==4){
          for(TableIterator iter = myTableHeap->Begin();iter!=myTableHeap->End();iter++){
            if(iter->GetField(ind)->BackValue()!=value_s){
              row_to_update2.insert(iter->GetRowId());//如果符合条件就加到这里面去
            }
          }
        }
        else if(op1==5){
          for(TableIterator iter = myTableHeap->Begin();iter!=myTableHeap->End();iter++){
            if(iter->GetField(ind)->BackValue()<value_s){
              row_to_update2.insert(iter->GetRowId());//如果符合条件就加到这里面去
            }
          }
        }
        else if(op1==6){
          for(TableIterator iter = myTableHeap->Begin();iter!=myTableHeap->End();iter++){
            if(iter->GetField(ind)->BackValue()>value_s){
              row_to_update2.insert(iter->GetRowId());//如果符合条件就加到这里面去
            }
          }
        }
        else if(op1==7){
          for(TableIterator iter = myTableHeap->Begin();iter!=myTableHeap->End();iter++){
            if(iter->GetField(ind)->IsNull()){// is null
              row_to_update2.insert(iter->GetRowId());//如果符合条件就加到这里面去
            }
          }
        }
        else if(op1==8){
          for(TableIterator iter = myTableHeap->Begin();iter!=myTableHeap->End();iter++){
            if(!(iter->GetField(ind)->IsNull())){//非null
              row_to_update2.insert(iter->GetRowId());//如果符合条件就加到这里面去
            }
          }
        }
        else{

        }
        myStack.pop();
      }
      else if(myStack.top()->type_==kNodeConnector){
        std::string temp1 = myStack.top()->val_;
        if(temp1=="and"){
          set_intersection(row_to_update1.begin(),row_to_update1.end(),row_to_update2.begin(),row_to_update2.end(),
                           inserter(result,result.begin()));//两个取交集然后放到result里
          row_to_update1.clear();
          row_to_update1 = result;//然后再把结果放到第一个set里面
          result.clear();
          row_to_update2.clear();
          myStack.pop();
        }
        else{//也就是or
          for_or.push(row_to_update1);//思路是将第一个集合压栈，然后让第二个操作数到第一个set里
          row_to_update1.clear();
          row_to_update1 = row_to_update2;
          row_to_update2.clear();
          myStack.pop();
        }
      }

    }

    for_or.push(row_to_update1);
    row_to_update1.clear();

    row_to_update1 = for_or.top();
    for_or.pop();
    while(!for_or.empty()){
      row_to_update2 = for_or.top();
      set_union(row_to_update1.begin(),row_to_update1.end(),row_to_update2.begin(),row_to_update2.end(),
                inserter(result,result.begin()));
      row_to_update1.clear();
      row_to_update1 = result;
      result.clear();
      row_to_update2.clear();
      for_or.pop();
    }
    //到这里已经得到了需要更新的所有row的rowid

    vector <Row> oldRows;
    vector <Row> newRows;
    for(TableIterator iter = myTableHeap->Begin();iter!=myTableHeap->End();iter++){
      RowId temp = iter->GetRowId();
        if(row_to_update1.find(temp)!=row_to_update1.end()){
          oldRows.push_back(*iter);//我们就把这整个row放到oldRows里面
      }
    }

    vector<Field> fields_to_change;
    vector<int> map_for_change;//这两个vector是一一对应的

    if(ast->child_->type_==kNodeUpdateValue){
      ast =ast->child_;
    }
    int i = 0;
    std::vector<Column *> myColumns = tableInfo1->GetSchema()->GetColumns();//用来获得map
    while(ast){//这里就是在把每一个需要改变的field加入到这个vector里去
      pSyntaxNode child = ast->child_;

      string attrib  = child->val_;
      for(unsigned long j = 0;j<myColumns.size();j++){
        if(myColumns[j]->GetName()==attrib){
          map_for_change.push_back(j);
          break;
        }
      }

      child = child->next_;//在这之前获得map
        //TypeId make_sure = myColumns[i]->GetType();//获取当前table的每一个column的属性，与当前节点的属性进行比较
        if(child->type_==kNodeNumber){//如果是数字类型的数据
          std::string content = child->val_;
          //          typeid(child->type_)== typeid(int)
          float a = atoi(content.c_str());
          if((int)a==a){//如果在进行了强制类型转换之后仍然相等，那么说明是int型
            Field t(TypeId::kTypeInt,(int)a);
            fields_to_change.push_back(t);//赋初始值后加到vector里
          }
          else{//说明是float类型的
            Field t(TypeId::kTypeFloat,a);
            fields_to_change.push_back(t);
          }
        }
        else if(child->type_==kNodeString){//也就是char类型的node
          Field t (TypeId::kTypeChar,child->val_,sizeof(child->val_),true);//注意一下manage-data的含义
          fields_to_change.push_back(t);
        }
        else{//也就是空值

          TypeId type_of_null = myColumns[i]->GetType();
          if(myColumns[i]->IsNullable()){//就是该元素可以为空
            Field t(type_of_null);
            fields_to_change.push_back(t);
          }
          else return DB_FAILED;//不然就直接无法插入
        }
        child=child->next_;//继续寻找下一个元素值
        i++;
        ast = ast->next_;

     //当跳出循环时，Myfield构建完毕，可以进行插入了
    }
      int columns_count ;
      if(!oldRows.empty()){
        columns_count = oldRows[0].GetFieldCount();
      }
      for(unsigned long j = 0;j<oldRows.size();j++) {
        vector<Field> myField;
        for (int k = 0; k < columns_count; k++) {
          vector<int>::iterator it = find(map_for_change.begin(), map_for_change.end(), k);
          if (it == map_for_change.end()) {  //如果没找到，那么说明可以插入原值
            Field temp1(*(oldRows[j].GetField(k)));
            myField.push_back(temp1);
          } else {  //否则就得插入修改过的field
            int index = std::distance(map_for_change.begin(), it);
            myField.push_back(fields_to_change[index]);
          }
        }
        Row a(myField);
        newRows.push_back(a);
      }
      //到这里，老的row和新的row都得到了，而且是一一对应的




    //然后还要更新index


      std::vector<IndexInfo*> index_of_this_table;//先尝试用索引进行查找
      // IndexInfo* index_info1;
      //index_info1.
      catalog_01->GetTableIndexes(table_name,index_of_this_table);
      std::vector<IndexInfo*>::iterator it;
      if(!index_of_this_table.empty()){//如果这个表确实存在index
        for(it=index_of_this_table.begin();it!=index_of_this_table.end();it++){
          //就是我首先得判断需要转换成的B+tree index的模板类型
          std::vector<uint32_t> key_map_of_this_index;
          IndexMetadata * myMetadata = (*it)->GetMetaData();
          key_map_of_this_index = myMetadata->GetKeyMapping();//有了这个keymapping之后，就可以挑选出自己的row

//          IndexSchema * This_schema = (*it)->GetIndexKeySchema();//这里有了这个index需要的schema
//          int length = This_schema->GetSerializedSize();

          Index * temp = (*it)->GetIndex();

          for(unsigned long j = 0;j<oldRows.size();j++){
            std::vector<Field> Field_of_this_index;
            std::vector<Field> myField;//首先得利用k，把原有的field都填充到myField里
            std::vector<Field> Field_of_this_index_new;//新的field
            std::vector<Field> myField_new;
            Row old_row(oldRows[j]);
            Row new_row(newRows[j]);

            for(int k = 0;k<columns_count;k++){
              myField.push_back(*(old_row.GetField(k)));
              myField_new.push_back((*(new_row.GetField(k))));
            }

            for(auto l:key_map_of_this_index){
              Field_of_this_index.push_back(myField[l]);//删除老的索引需要的field
              Field_of_this_index_new.push_back(myField_new[l]);//插入新的索引需要的field
            }

            Row row_of_this_index(Field_of_this_index);
            Row row_of_this_index_new(Field_of_this_index_new);

            temp->RemoveEntry(row_of_this_index,old_row.GetRowId(), nullptr);
            temp->InsertEntry(row_of_this_index_new,old_row.GetRowId(), nullptr);

//            if(length<=4){
//              BPlusTreeIndex<GenericKey<4>,RowId,GenericComparator<4>> * BPtree =
//                  (BPlusTreeIndex<GenericKey<4>,RowId,GenericComparator<4>>*)(temp);
//              BPtree->RemoveEntry(row_of_this_index,old_row.GetRowId(), nullptr);
//              BPtree->InsertEntry(row_of_this_index_new,old_row.GetRowId(), nullptr);
//
//              //这里进行强转，（本来就是用子类进行初始化的）
//            }
//            else if(length<=8){
//              BPlusTreeIndex<GenericKey<8>,RowId,GenericComparator<8>> * BPtree =
//                  (BPlusTreeIndex<GenericKey<8>,RowId,GenericComparator<8>>*)(temp);
//              BPtree->RemoveEntry(row_of_this_index,old_row.GetRowId(), nullptr);
//              BPtree->InsertEntry(row_of_this_index_new,old_row.GetRowId(), nullptr);
//            }
//            else if(length<=16){
//              BPlusTreeIndex<GenericKey<16>,RowId,GenericComparator<16>> * BPtree =
//                  (BPlusTreeIndex<GenericKey<16>,RowId,GenericComparator<16>>*)(temp);
//              BPtree->RemoveEntry(row_of_this_index,old_row.GetRowId(), nullptr);
//              BPtree->InsertEntry(row_of_this_index_new,old_row.GetRowId(), nullptr);
//            }
//            else if(length<=32){
//              BPlusTreeIndex<GenericKey<32>,RowId,GenericComparator<32>> * BPtree =
//                  (BPlusTreeIndex<GenericKey<32>,RowId,GenericComparator<32>>*)(temp);
//              BPtree->RemoveEntry(row_of_this_index,old_row.GetRowId(), nullptr);
//              BPtree->InsertEntry(row_of_this_index_new,old_row.GetRowId(), nullptr);
//            }
//            else {//length<=64,最多支持64字节
//              BPlusTreeIndex<GenericKey<64>,RowId,GenericComparator<64>> * BPtree =
//                  (BPlusTreeIndex<GenericKey<64>,RowId,GenericComparator<64>>*)(temp);
//              BPtree->RemoveEntry(row_of_this_index,old_row.GetRowId(), nullptr);
//              BPtree->InsertEntry(row_of_this_index_new,old_row.GetRowId(), nullptr);
//            }

          }

        }
      }

      for(unsigned long j = 0;j<newRows.size();j++){
        myTableHeap->UpdateTuple(newRows[j],oldRows[j].GetRowId(), nullptr);
      }//table——heap更新完毕
      return DB_SUCCESS;

  }
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {//我的理解是通过这个函数调用一个txt文件，执行里面的sql命令
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
if(ast->type_==kNodeExecFile){
  fstream sql;
  sql.open(ast->child_->val_, ios::in);
  std::string sqls_;
  ExecuteEngine engine;
  while(getline(sql,sqls_)){//txt里面的每一条sql语句进行操作
    YY_BUFFER_STATE bp = yy_scan_string(sqls_.data());
    if (bp == nullptr) {
      LOG(ERROR) << "Failed to create yy buffer state." << std::endl;
      exit(1);
    }
    yy_switch_to_buffer(bp);
    MinisqlParserInit();
    yyparse();
    if (MinisqlParserGetError()) {
      printf("%s\n", MinisqlParserGetErrorMessage());
    } 
    else {
      #ifdef ENABLE_PARSER_DEBUG
      printf("[INFO] Sql syntax parse ok!\n");
      SyntaxTreePrinter printer(MinisqlGetParserRootNode());
      printer.PrintTree(syntax_tree_file_mgr[syntax_tree_id++]);
    #endif
    }

    //ExecuteContext context;
    engine.Execute(MinisqlGetParserRootNode(), context);
    MinisqlParserFinish();
    yy_delete_buffer(bp);
    yylex_destroy();
    if (context->flag_quit_) {//退出minisql
      printf("bye!\n");
      break;
    }
  }
  sql.close();
  return DB_SUCCESS;
}
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
  ASSERT(ast->type_ == kNodeQuit, "Unexpected node type.");
  context->flag_quit_ = true;
  //for (auto it : dbs_) {//刷盘在什么时候刷，没必要刷所有的
  //    delete it.second;
    //}
  delete this->dbs_[this->current_db_];//把正在使用的database刷盘
  this->dbs_.erase(this->current_db_);
  //for(std::pair<std::string, DBStorageEngine *> i:this->dbs_){
  //  LOG(INFO)<<i.first<<std::endl;
  //}
  return DB_SUCCESS;
}
