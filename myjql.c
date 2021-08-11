#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <sys/types.h>   
#include <unistd.h>
#include <time.h>

/* need to do:
*/
#define INPUT_BUFFER_SIZE 31
#define COLUMN_B_SIZE 11

#define MEMORY_MAX_PAGES  100


const uint32_t PAGE_SIZE = 4096;

const uint32_t NODE_TYPE_SIZE = sizeof(uint8_t);
const uint32_t NODE_TYPE_OFFSET = 0;
const uint32_t IS_ROOT_SIZE = sizeof(uint8_t);
//const uint32_t IS_ROOT_OFFSET = NODE_TYPE_SIZE;
const uint32_t IS_ROOT_OFFSET = sizeof(uint8_t);

const uint32_t PARENT_POINTER_SIZE = sizeof(uint32_t);
const uint32_t PARENT_POINTER_OFFSET = 2*sizeof(uint8_t);

//const uint8_t COMMON_NODE_HEADER_SIZE =
//NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;
const uint32_t COMMON_NODE_HEADER_SIZE = 6*sizeof(uint8_t);

const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
//const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = 6*sizeof(uint8_t);

const uint32_t LEAF_NODE_NEXT_LEAF_SIZE = sizeof(uint32_t);

const uint32_t LEAF_NODE_NEXT_LEAF_OFFSET = 10*sizeof(uint8_t);
//  COMMON_NODE_HEADER_SIZE+LEAF_NODE_NUM_CELLS_SIZE;

const uint32_t LEAF_NODE_HEADER_SIZE = 14*sizeof(uint8_t);
//  COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE + LEAF_NODE_NEXT_LEAF_SIZE;

const uint32_t LEAF_NODE_KEY_SIZE = 12 * sizeof(uint8_t);
const uint32_t LEAF_NODE_KEY_OFFSET = 0;
const uint32_t LEAF_NODE_VALUE_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_VALUE_OFFSET = 12*sizeof(uint8_t);
//LEAF_NODE_KEY_SIZE;
const uint32_t LEAF_NODE_CELL_SIZE = 16*sizeof(uint8_t);
//LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
#define LEAF_NODE_SPACE_FOR_CELLS  (PAGE_SIZE - LEAF_NODE_HEADER_SIZE)
#define LEAF_NODE_MAX_CELLS  (LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE)

#define LEAF_NODE_RIGHT_SPLIT_COUNT  ((LEAF_NODE_MAX_CELLS + 1) / 2)

#define LEAF_NODE_LEFT_SPLIT_COUNT  ((LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT)

/*
+ * Internal Node Header Layout
+ */
const uint32_t INTERNAL_NODE_NUM_KEYS_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_NUM_KEYS_OFFSET = 6*sizeof(uint8_t);
//COMMON_NODE_HEADER_SIZE;
const uint32_t INTERNAL_NODE_RIGHT_CHILD_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_RIGHT_CHILD_OFFSET = 10*sizeof(uint8_t);
//INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE;

const uint32_t INTERNAL_NODE_HEADER_SIZE = 14*sizeof(uint8_t);
/*
* Internal Node Body Layout
*/

const uint32_t INTERNAL_NODE_KEY_SIZE = 12 * sizeof(uint8_t);
const uint32_t INTERNAL_NODE_CHILD_SIZE = 4*sizeof(uint8_t);
const uint32_t INTERNAL_NODE_CELL_SIZE =16*sizeof(uint8_t);
//INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE;
#define INTERNAL_NODE_SPACE_FOR_CELLS  (PAGE_SIZE - INTERNAL_NODE_HEADER_SIZE)

#define INTERNAL_NODE_MAX_CELLS  (INTERNAL_NODE_SPACE_FOR_CELLS / INTERNAL_NODE_CELL_SIZE)

#define INTERNAL_NODE_LEFT_SPLIT_COUNT  (INTERNAL_NODE_MAX_CELLS/2)

#define INTERNAL_NODE_RIGHT_SPLIT_COUNT  (INTERNAL_NODE_MAX_CELLS-1 - INTERNAL_NODE_LEFT_SPLIT_COUNT)

#define ORDER  ((PAGE_SIZE - INTERNAL_NODE_HEADER_SIZE)/INTERNAL_NODE_CELL_SIZE + 1)
#define LEAF_NODE_MIN  ((ORDER+1)/2)
#define INTERNAL_NODE_MIN (INTERNAL_NODE_MAX_CELLS/2)


typedef struct {
    uint32_t a;
    char b[COLUMN_B_SIZE + 1];
} Row;

typedef struct {
    char buffer[INPUT_BUFFER_SIZE + 1];
    size_t length;
} input_command;

input_command input_buffer;

typedef struct  //all informations about pages and so on
{
    int file_descriptor;
    uint32_t file_length;
    uint32_t used_pages;
    uint32_t file_pages;
    uint32_t replace;
    void* pages[MEMORY_MAX_PAGES]; 
    uint32_t translate[MEMORY_MAX_PAGES]; // root page always stays at page 0
} Cache;

Cache cache;

void write_back(int index)
{
    if(cache.pages[index]==NULL) return;    // unused. just do it.

    lseek(cache.file_descriptor,
                    cache.translate[index]*PAGE_SIZE,
                    SEEK_SET);
    write(
        cache.file_descriptor,cache.pages[index],PAGE_SIZE
    );
}

void* get_page(uint32_t page_num)
{
    //printf("get_page\n");
    if(page_num==0) {
        //printf("=0\n");
        void* node =cache.pages[0];
        return node;
    }
    //printf("not 0\n");
    for(int i=1;i<MEMORY_MAX_PAGES;++i)
        if(cache.translate[i]==page_num) return cache.pages[i];
    
    if(cache.used_pages<MEMORY_MAX_PAGES)
    {
        void* new_page = malloc(PAGE_SIZE);
        lseek(cache.file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
        ssize_t bytes_read = read(cache.file_descriptor,new_page,PAGE_SIZE);
        cache.pages[cache.used_pages] = new_page;
        cache.translate[cache.used_pages] = page_num;
        cache.used_pages++;
        return cache.pages[cache.used_pages-1];
    }
    else{
        int a;
        a = (cache.replace - 1)%(MEMORY_MAX_PAGES-1)+1;
        cache.replace++;
        write_back(a);
        void* new_page = malloc(PAGE_SIZE);
        lseek(cache.file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
        ssize_t bytes_read = read(cache.file_descriptor,new_page,PAGE_SIZE);
        cache.pages[a] = new_page;
        cache.translate[a] = page_num;
        return cache.pages[a];
    }
}

uint32_t* leaf_node_num_cells(void* node) {
    return node + LEAF_NODE_NUM_CELLS_OFFSET;
}

void* leaf_node_cell(void* node, uint32_t cell_num) {
    return node + LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE;
}

void* leaf_node_key(void* node, uint32_t cell_num) {
    return leaf_node_cell(node, cell_num);
}

uint32_t* leaf_node_value(void* node, uint32_t cell_num) {
    return leaf_node_cell(node, cell_num) + LEAF_NODE_KEY_SIZE;
}

uint32_t* leaf_node_next_leaf(void* node) {
  return node + LEAF_NODE_NEXT_LEAF_OFFSET;
}

void set_node_type(void* node, uint8_t type) { 
  uint8_t value = type;
  *((uint8_t*)(node + NODE_TYPE_OFFSET)) = (uint8_t)value;
}

void set_node_root(void* node,uint8_t type){
  uint8_t value = type;
  *((uint8_t*)(node + IS_ROOT_OFFSET)) = (uint8_t)value;
}

void initialize_leaf_node(void* node) {
  set_node_type(node, 1);
  set_node_root(node,0);
  *leaf_node_num_cells(node) = 0;
}

uint8_t get_node_type(void* node) {
  uint8_t value = *((uint8_t*)(node + NODE_TYPE_OFFSET)); // 0 = internal && 1 = leaf
  return value;
}

uint8_t is_node_root(void* node)
{
    uint8_t value = *((uint8_t*)(node + IS_ROOT_OFFSET)); // 0 = not && 1 = yes
    return value;
}

uint32_t* internal_node_num_keys(void* node) {
  return node + INTERNAL_NODE_NUM_KEYS_OFFSET;
}

uint32_t* internal_node_right_child(void* node) {
  return node + INTERNAL_NODE_RIGHT_CHILD_OFFSET;
}

uint32_t* internal_node_cell(void* node, uint32_t cell_num) {
  return node + INTERNAL_NODE_HEADER_SIZE + cell_num * INTERNAL_NODE_CELL_SIZE;
}

void* internal_node_key(void* node, uint32_t key_num) {

  return (void*)internal_node_cell(node, key_num) + INTERNAL_NODE_CHILD_SIZE;
}

uint32_t* internal_node_child(void* node, uint32_t child_num) {
  uint32_t num_keys = *internal_node_num_keys(node);
  if (child_num > num_keys) {
    printf("Tried to access child_num %d > num_keys %d\n", child_num, num_keys);
    exit(EXIT_FAILURE);
  } else if (child_num == num_keys) {

    //printf("fuck internal_node_right_child.%d\n",num_keys);
    return internal_node_right_child(node);
  } else {
    //printf("fuck internal_node_cell. %d\n",num_keys);
    //printf("this time not happen yet.%s\n",(char*)internal_node_key(get_page(0),0));
    return internal_node_cell(node, child_num);
    
  }
}



void initialize_internal_node(void* node){
  set_node_type(node, 0);
  set_node_root(node, 0);
  *internal_node_num_keys(node) = 0;
}

char* get_node_max_key(void* node) {
  switch (get_node_type(node)) {
    case 0:
      return internal_node_key(node, *internal_node_num_keys(node) - 1);
    case 1:
      return leaf_node_key(node, *leaf_node_num_cells(node) - 1);
    default: return NULL;
  }
}

char* get_real_max(void* node){
    if(get_node_type(node)==1) return leaf_node_key(node,*leaf_node_num_cells(node) - 1);
    else return get_real_max(get_page(*internal_node_right_child(node)));
}






void open_file(const char* filename)
{
    int fd = open(filename,
     	O_RDWR | 	// Read/Write mode
     	    O_CREAT,	// Create file if it does not exist
     	S_IWUSR |	// User write permission
     	    S_IRUSR	// User read permission
     	);
    
    cache.file_descriptor = fd;
    cache.file_length = lseek(fd,0,SEEK_END);
    cache.file_pages = cache.file_length/PAGE_SIZE;
    cache.used_pages = 1;
    cache.replace = 1;

    if(cache.file_length % PAGE_SIZE!=0){
        printf("Db file is not a whole number of pages. Corrupt file.\n");
        exit(EXIT_FAILURE);
    }
    for (int i = 0; i < MEMORY_MAX_PAGES; i++) {
        cache.pages[i] = NULL;
        cache.translate[i] = 0; 
    }

    if(cache.file_pages==0){
        void* root_node = malloc(PAGE_SIZE);
        cache.pages[0] = root_node;
        initialize_leaf_node(root_node);
        set_node_root(root_node, 1);
        *leaf_node_next_leaf(root_node)=0;
        cache.file_pages = 1;
        cache.file_length = PAGE_SIZE;
    }
    else{
        void* root_node = malloc(PAGE_SIZE);
        lseek(cache.file_descriptor,0,SEEK_SET);
        read(cache.file_descriptor,root_node,PAGE_SIZE);
        cache.pages[0] = root_node;
    }

    write_back(0);
}

void print_prompt(){printf("myjql> ");}

int read_input(){
    input_buffer.length = 0;
    while (input_buffer.length <= INPUT_BUFFER_SIZE
    && (input_buffer.buffer[input_buffer.length++] = getchar()) != '\n'
    && input_buffer.buffer[input_buffer.length - 1] != EOF);
    if(input_buffer.buffer[input_buffer.length-1]==EOF)
        exit(EXIT_SUCCESS);
    input_buffer.length--;

    if(input_buffer.length == INPUT_BUFFER_SIZE
        && input_buffer.buffer[input_buffer.length] != '\n'){
            while(getchar()!='\n');
            return 0;
    }
    input_buffer.buffer[input_buffer.length]=0;
    return 1;
}

void close_file(){
    for(int i=0;i<MEMORY_MAX_PAGES;++i){
        if(cache.pages[i]==NULL) continue;
        write_back(i);
        cache.translate[i] = 0;
        free(cache.pages[i]);
        cache.pages[i]=NULL;
    }

    close(cache.file_descriptor);
}

uint32_t leaf_node_find(uint32_t page_num,char* key){
    void* node = get_page(page_num);

    char* key_at_index;

    uint32_t num_cells = *leaf_node_num_cells(node);
    
    for(int i=0;i<num_cells;++i){
        key_at_index = (char*)leaf_node_key(node,i);
        if(strcmp(key,key_at_index)<=0) return i;
    }
    return num_cells;
    
}
/*
This will either return

the position of the key,
the position of another key that we’ll need to move if we want to insert the new key, or
the position one past the last key
*/

uint32_t get_new_page(){
    //printf("get a new page.\n");
    if(cache.used_pages<MEMORY_MAX_PAGES)
    {
        void* new_page = malloc(PAGE_SIZE);
        cache.pages[cache.used_pages] = new_page;
        cache.translate[cache.used_pages] = cache.file_pages;
    }
    else{
        int a;
        a = (cache.replace-1)%(MEMORY_MAX_PAGES-1)+1;
        write_back(a);
        void* new_page = malloc(PAGE_SIZE);
        cache.pages[a] = new_page;
        cache.translate[a] = cache.file_pages;
    }
    cache.used_pages++;
    cache.file_pages++;
    cache.replace++;
    cache.file_length+=PAGE_SIZE;
    return cache.file_pages-1;
}

uint32_t* node_parent(void* node) { return node + PARENT_POINTER_OFFSET; }


uint32_t internal_node_find_child(void* node,char* key)
{
    uint32_t min_index = 0;
    uint32_t max_index = *internal_node_num_keys(node);
    /*
    while(min_index != max_index){
        uint32_t index = (min_index+max_index)/2;
        char* key_to_right = internal_node_key(node,index);
        if(strcmp(key_to_right,key)>=0) max_index = index;
        else min_index = index+1;
    }
    

    for(int i = (int)min_index-1;i>=0;--i)
        if(strcmp(key,internal_node_key(node,i))==0) min_index=i;
        else break;
    
    return min_index;
    */

   for(int i=0;i<max_index;++i){
       //printf("key:%s inkey:%s\n",key,(char*)internal_node_key(node,i));

       if(strcmp(key,(char*)internal_node_key(node,i))<=0) return i;

   }

   return max_index;
}

void update_internal_node_key(void* node,char* old_max,char* new_max){

    //printf("%s %s\n",get_node_max_key(node),old_max);
    if(strcmp(get_node_max_key(node),old_max)<0) return;

    uint32_t old_index = internal_node_find_child(node,old_max);

    //printf("old index = %d\n",old_index);

    strcpy((char*)internal_node_key(node,old_index),new_max);

    //printf("%s  %s\n",new_max,(char*)internal_node_key(node,0));
}

void create_new_root(uint32_t right_page_num)
{
    //printf("create a new root!\n");
    void* root=get_page(0);
    void* right_child = get_page(right_page_num);

    uint32_t left_page_num = get_new_page();

    void* left_child = get_page(left_page_num);
    if(get_node_type(root)==0){
        for(uint32_t i=0;i<*internal_node_num_keys(root);++i)
            *node_parent(get_page(*internal_node_child(root,i)))=left_page_num;
        *node_parent(get_page(*internal_node_right_child(root)))=left_page_num;
    }
    memcpy(left_child,root,PAGE_SIZE);
    set_node_root(left_child,0);

    initialize_internal_node(root);
    set_node_root(root,1);   
    *internal_node_num_keys(root) = 1;
    *internal_node_child(root,0) = left_page_num;
    char* left_child_max_key = get_real_max(left_child);

    //printf("create new root 0-key %ld",internal_node_key(root,0)-root);

    strcpy(internal_node_key(root,0),left_child_max_key);
    *internal_node_right_child(root) = right_page_num;

    *node_parent(left_child) = 0;
    *node_parent(right_child) = 0;
    return;
}

void internal_node_insert(uint32_t ,uint32_t );

void internal_node_split_and_insert(uint32_t parent_page_num,uint32_t child_page_num){
    void* parent = get_page(parent_page_num);

    char* old_max = (char*)malloc(12*sizeof(char));
    strcpy(old_max,get_real_max(parent));

    void* child = get_page(child_page_num);
    char* child_max_key = get_real_max(child);
    uint32_t new_page_num=get_new_page();
    void* new_node = get_page(new_page_num);
    initialize_internal_node(new_node);

    *node_parent(new_node) = *node_parent(parent);

    for(int i=INTERNAL_NODE_LEFT_SPLIT_COUNT+1;i<INTERNAL_NODE_MAX_CELLS;++i)
    {
        *node_parent( get_page(*internal_node_child(parent,i))) = new_page_num;

        void* destination = internal_node_cell(new_node,i-INTERNAL_NODE_LEFT_SPLIT_COUNT-1);
        memcpy(destination,internal_node_cell(parent,i),INTERNAL_NODE_CELL_SIZE);
    }
    *(internal_node_right_child(new_node))=*internal_node_right_child(parent);
    *node_parent( get_page(*internal_node_right_child(parent))) = new_page_num;

    *(internal_node_right_child(parent))=*internal_node_child(parent,INTERNAL_NODE_LEFT_SPLIT_COUNT);

    *(internal_node_num_keys(new_node))=INTERNAL_NODE_RIGHT_SPLIT_COUNT;
    *(internal_node_num_keys(parent))=INTERNAL_NODE_LEFT_SPLIT_COUNT;
    
    if(strcmp(child_max_key,get_real_max(parent))<=0) internal_node_insert(parent_page_num,child_page_num);
    else internal_node_insert(new_page_num,child_page_num);

    if(is_node_root(parent)==1) create_new_root(new_page_num);
    else{
        uint32_t parent_num= *node_parent(parent);
        char* new_max = get_real_max(parent);
        void* p_parent = get_page(parent_num);
        update_internal_node_key(p_parent,old_max,new_max);
        internal_node_insert(parent_num,new_page_num);
    }
    free(old_max);
}

// insert a page after a split
void internal_node_insert(uint32_t parent_page_num,uint32_t child_page_num){
    void* parent = get_page(parent_page_num);
    void* child = get_page(child_page_num);
    char* child_max_key = get_real_max(child);
    uint32_t index = internal_node_find_child(parent,child_max_key);


    //printf("insert position %d\n",index);
    //printf("child_page_number %d\n",child_page_num);

    uint32_t original_num_keys = *internal_node_num_keys(parent);
    
    if(original_num_keys >= INTERNAL_NODE_MAX_CELLS)
    {
        internal_node_split_and_insert(parent_page_num,child_page_num);
        return;
    }



    *internal_node_num_keys(parent) = original_num_keys + 1;
    *node_parent(child) = parent_page_num;

    uint32_t right_child_page_num = *internal_node_right_child(parent);
    void* right_child = get_page(right_child_page_num);


    if(strcmp(child_max_key , get_real_max(right_child))>0){
        *internal_node_child(parent,original_num_keys)=right_child_page_num;
        strcpy((char*)internal_node_key(parent,original_num_keys),get_real_max(right_child));
        *internal_node_right_child(parent)=child_page_num;
    }else{

        for(int i = original_num_keys;i>index;i--){
            void* destination = internal_node_cell(parent,i);
            void* sourse = internal_node_cell(parent,i-1);
            memcpy(destination,sourse,INTERNAL_NODE_CELL_SIZE);
        }



        *internal_node_child(parent,index) = child_page_num;



        strcpy((char*)internal_node_key(parent,index),child_max_key);

    }


}

void leaf_node_split_and_insert(uint32_t page_num,uint32_t index,Row* row){
    void* old_node = get_page(page_num);

    //char* old_max = get_node_max_key(old_node);
    char* old_max = (char*)malloc(12*sizeof(char));
    strcpy(old_max,get_node_max_key(old_node));


    uint32_t new_page_num = get_new_page();
    void* new_node = get_page(new_page_num);

    //printf("new page num ? %d\n",new_page_num);

    initialize_leaf_node(new_node);
    *node_parent(new_node) = *node_parent(old_node);

    *leaf_node_next_leaf(new_node)=*leaf_node_next_leaf(old_node);
    *leaf_node_next_leaf(old_node)=new_page_num;
    
    for(int i = LEAF_NODE_MAX_CELLS;i>=0;i--)
    {
        void* destination_node;
        if(i>=LEAF_NODE_LEFT_SPLIT_COUNT) destination_node=new_node;
        else destination_node = old_node;

        uint32_t index_within_node = i%LEAF_NODE_LEFT_SPLIT_COUNT;
        void* destination = leaf_node_cell(destination_node,index_within_node);

        if(i==index){
            strcpy(((char*)leaf_node_key(destination_node,index_within_node)),row->b);
            *(leaf_node_value(destination_node,index_within_node)) = row->a;
        }
        else if(i>index){
            memcpy(destination,leaf_node_cell(old_node,i-1),LEAF_NODE_CELL_SIZE);
        }
        else memcpy(destination,leaf_node_cell(old_node,i),LEAF_NODE_CELL_SIZE);
    }
    /* Update cell count on both leaf nodes */
    *(leaf_node_num_cells(old_node)) = LEAF_NODE_LEFT_SPLIT_COUNT;
    *(leaf_node_num_cells(new_node)) = LEAF_NODE_RIGHT_SPLIT_COUNT;

    //printf("old max: %s",old_max);

    //printf("decide node type %d\n",is_node_root(old_node));
    if(is_node_root(old_node)==1) create_new_root(new_page_num);
    else{

        uint32_t parent_page_num = *node_parent(old_node);
        char* new_max = get_node_max_key(old_node);
        void* parent = get_page(parent_page_num);

        update_internal_node_key(parent,old_max,new_max);
        internal_node_insert(parent_page_num,new_page_num);
    }
    free(old_max);
}



void leaf_node_insert(uint32_t page_num,uint32_t index, Row* row)
{
    //printf("insert page: %d index : %d",page_num,index);
    void* node = get_page(page_num);
    uint32_t num_cells = *leaf_node_num_cells(node);
    //printf("insert node type? %d\n",is_node_root(node));
    
    if(num_cells >= LEAF_NODE_MAX_CELLS){
        //printf("split!!\n");
        leaf_node_split_and_insert(page_num,index,row);
        return;
    }

    for(int i = num_cells;i>index;i--)
    {
        memcpy(leaf_node_cell(node,i),leaf_node_cell(node,i-1),LEAF_NODE_CELL_SIZE);
    }
    *(leaf_node_num_cells(node)) += 1;
    strcpy((char*)leaf_node_key(node,index),row->b);
    *(leaf_node_value(node,index)) = row->a;
}

uint32_t find_leaf_node(uint32_t page_num,char* key){
    void* node = get_page(page_num);
    if(get_node_type(node)==1) return page_num;

    uint32_t min_index = 0;
    uint32_t max_index = *internal_node_num_keys(node);

    //printf("key nums = %d\n",max_index);
    //printf("1 %s\n",(char*)internal_node_key(node,0));
    //printf("2 %s\n",(char*)internal_node_key(node,1));

    /*while(min_index != max_index){
        uint32_t index = (min_index+max_index)/2;
        char* key_to_right = internal_node_key(node,index);
        if(strcmp(key_to_right,key)>=0) max_index = index;
        else min_index = index+1;
    }

    for(int i = (int)min_index-1;i>=0;--i)
        if(strcmp(key,internal_node_key(node,i))==0) min_index=i;
        else break;
    //printf("min_index: %d\n",min_index);
    //printf("page_number :%d\n",*internal_node_child(node,min_index));*/
    //for(int i=0;i<*internal_node_num_keys(node);++i)
    //{
    //    printf("%d %s\n",i,(char*)internal_node_key(node,i)); 
    //}

    for(int i=0;i<*internal_node_num_keys(node);++i)
    {
        if(strcmp(key,(char*)internal_node_key(node,i))<=0) return find_leaf_node(*internal_node_child(node,i),key);
    }
    return find_leaf_node(*internal_node_child(node,max_index),key);
}

void insert(Row* row,uint32_t page_num){
    //printf("first %d\n",page_num);
    void* page = get_page(page_num);

    //printf("page_num %d",page_num);
    if(get_node_type(page)==1){
        uint32_t index = leaf_node_find(page_num,row->b);

        //printf("%d\n",index);
        leaf_node_insert(page_num,index,row);
    }
    else{
        uint32_t leaf_num = find_leaf_node(0,row->b);
        //printf("find_leaf_num = %d \n",leaf_num);
        uint32_t index = leaf_node_find(leaf_num,row->b);

        leaf_node_insert(leaf_num,index,row);
    }
}

void execute_insert(){
    char* keyword = strtok(input_buffer.buffer," ");
    char* a = strtok(NULL," ");
    char* b = strtok(NULL," ");
    int x;

    if(a == NULL || b == NULL){
        printf("input error.\n");
        return;
    }
    x = atoi(a);

    Row* row = malloc(sizeof(Row));
    row->a = x;
    strcpy(row->b,b);
    
    insert(row,0);

    free(row);

    printf("\nExecuted.\n\n");
}

void meta_command() {
    if (strcmp(input_buffer.buffer, ".exit") == 0) {
        printf("bye~\n");
        close_file();
        exit(EXIT_SUCCESS);
    }
    else printf("Unrecognized command.\n");
}

void select_all(){
    void* node = get_page(0);

    while(get_node_type(node)==0) node = get_page(*internal_node_child(node,0));

    if(*leaf_node_num_cells(node)==0) {
        printf("\n(Empty)\n");
        printf("\nExecuted.\n\n");
        return;
    }
    printf("\n");
    do{
        // printf("\n");
        for(int i=0;i<*(leaf_node_num_cells(node));++i){
            char* key = (char*)leaf_node_key(node,i);
            uint32_t value = *leaf_node_value(node,i);
            printf("(%d, %s)\n",value,key);
        }
        node = get_page(*leaf_node_next_leaf(node));
    }while(node!=get_page(0));
    printf("\nExecuted.\n\n");
}

void select_key(char* key){
    uint32_t leaf_num = find_leaf_node(0,key);
    void* page = get_page(leaf_num);
    int index = -1;
    for(int i=0;i<*leaf_node_num_cells(page);++i) 
    {
        if(strcmp(key,(char*)leaf_node_key(page,i))==0){
            index=i;
            break;
        }
    }
    if(index==-1){
        printf("\n(Empty)\n");
        printf("\nExecuted.\n\n");
        return;
    }
    printf("\n");
    do{
        int tag=0;
        for(int i=index;i<*(leaf_node_num_cells(page));++i){
            if(strcmp(key,(char*)leaf_node_key(page,i))==0){
                uint32_t value = *leaf_node_value(page,i);
                printf("(%d, %s)\n",value,key);
            }
            else{
                tag=1;
                break;
            }
        }
        if(tag) break;
        page = get_page(*leaf_node_next_leaf(page));
        index=0;
    }while(page!=get_page(0));
    printf("\nExecuted.\n\n");
        

}

void execute_select(){
    char* keyword = strtok(input_buffer.buffer," ");
    char* a = strtok(NULL," ");
    char* b = strtok(NULL," ");

    if(a == NULL && b == NULL){
        select_all();
        return;
    }
    
    char* key = malloc(LEAF_NODE_KEY_SIZE);
    strcpy(key,a);
    select_key(key);
    free(key);
}

void internal_node_delete(uint32_t ,uint32_t );

void borrow_or_merge_internal(uint32_t parent_num,uint32_t child_num){
    void* parent = get_page(parent_num);
    void* child = get_page(child_num);
    uint32_t cell_num = *internal_node_num_keys(parent);

    uint32_t sibling_num;

    if(is_node_root(parent)==1&&*internal_node_num_keys(parent)==1){
        if(*internal_node_child(parent,0)==child_num){
            sibling_num = *internal_node_child(parent,1);

            void* sibling = get_page(sibling_num);

            if(*internal_node_num_keys(sibling)>INTERNAL_NODE_MIN){

                internal_node_insert(*internal_node_child(parent,0),*internal_node_child(sibling,0));
                internal_node_delete(sibling_num,0);
                strcpy((char*)internal_node_key(parent,0),get_real_max(child));
            }

            else{

                for(uint32_t j = 0;j<=*internal_node_num_keys(sibling);++j)
                {
                    internal_node_insert(child_num,*internal_node_child(sibling,j));
                }
                void* root = get_page(0);
                memcpy(root,child,PAGE_SIZE);
                set_node_root(root,1);
                for(uint32_t i=0;i<=*internal_node_num_keys(root);i++) 
                    *node_parent(get_page(*internal_node_child(child,i)))=0;
            }
        }
        else{
            sibling_num = *internal_node_child(parent,0);

            void* sibling = get_page(sibling_num);

            if(*internal_node_num_keys(sibling)>INTERNAL_NODE_MIN){

                internal_node_insert(child_num,*internal_node_child(sibling,*internal_node_num_keys(sibling)));
                internal_node_delete(sibling_num,*internal_node_num_keys(sibling));
                strcpy((char*)internal_node_key(parent,0),get_real_max(sibling));
            }

            else{

                for(uint32_t j = 0;j<=*internal_node_num_keys(sibling);++j)
                {
                    internal_node_insert(child_num,*internal_node_child(sibling,j));
                }
                void* root = get_page(0);
                memcpy(root,child,PAGE_SIZE);
                set_node_root(root,1);
                for(uint32_t i=0;i<=*internal_node_num_keys(root);i++) 
                    *node_parent(get_page(*internal_node_child(child,i)))=0;
            }
        }
        return;
    }

    // parent == root not considered

    if(*internal_node_child(parent,0)==child_num){

        sibling_num = *internal_node_child(parent,1);

        void* sibling = get_page(sibling_num);

        if(*internal_node_num_keys(sibling)>INTERNAL_NODE_MIN){

            internal_node_insert(*internal_node_child(parent,0),*internal_node_child(sibling,0));
            internal_node_delete(sibling_num,0);
            strcpy((char*)internal_node_key(parent,0),get_real_max(child));
        }

        else{

            for(uint32_t j = 0;j<=*internal_node_num_keys(sibling);++j)
            {
                internal_node_insert(child_num,*internal_node_child(sibling,j));
            }
            strcpy((char*)internal_node_key(parent,0),get_real_max(child));
            internal_node_delete(parent_num,1);
        }
    }
    else{
        int i;
        for(i =1;i<cell_num;++i)
        {
            if(*internal_node_child(parent,i)==child_num) {
                sibling_num = *internal_node_child(parent,i-1);
                break;
            }
        }
        if(i==cell_num) sibling_num = *internal_node_child(parent,cell_num-1);

        void* sibling = get_page(sibling_num);

        if(*leaf_node_num_cells(sibling)>LEAF_NODE_MIN){
            internal_node_insert(child_num,*internal_node_child(sibling,*internal_node_num_keys(sibling)));
            internal_node_delete(sibling_num,*internal_node_num_keys(sibling));
            strcpy((char*)internal_node_key(parent,i-1),get_real_max(sibling));

        }
        else{
            for(int j = 0;j<=*internal_node_num_keys(sibling);++j)
            {
                internal_node_insert(child_num,*internal_node_child(sibling,j));
            }
            internal_node_delete(parent_num,i-1);
        }
    }
}

void internal_node_delete(uint32_t internal_num,uint32_t index)
{
    void* page = get_page(internal_num);
    
    uint32_t num_keys = *internal_node_num_keys(page);
    

    if(index == num_keys) {
        *internal_node_right_child(page) = *internal_node_child(page,num_keys-1);
    }

    *internal_node_num_keys(page) = num_keys -1;

    //for(int i=num_keys-1;i>index;i--){
    //  memcpy(internal_node_cell(page,i-1),internal_node_cell(page,i),INTERNAL_NODE_CELL_SIZE);
    //}

    for(int i=index;i<num_keys-1;++i){
        memcpy(internal_node_cell(page,i),internal_node_cell(page,i+1),INTERNAL_NODE_CELL_SIZE);
    }

    if(*internal_node_num_keys(page)<INTERNAL_NODE_MIN){
        if(is_node_root(page)==1) return;
        uint32_t parent_num = *node_parent(page);
        borrow_or_merge_internal(parent_num,internal_num);
    }
}

void leaf_node_delete(uint32_t,int);

void borrow_or_merge_leaf(uint32_t parent_num,uint32_t child_num)
{
    void* parent = get_page(parent_num);
    void* child = get_page(child_num);
    uint32_t cell_num = *internal_node_num_keys(parent);

    uint32_t sibling_num;


    if(is_node_root(parent)==1&&*internal_node_num_keys(parent)==1){
        if(*internal_node_child(parent,0)==child_num){
            sibling_num = *internal_node_child(parent,1);

            void* sibling = get_page(sibling_num);

            if(*leaf_node_num_cells(sibling)>LEAF_NODE_MIN){

                memcpy(leaf_node_cell(child,*leaf_node_num_cells(child)),
                leaf_node_cell(sibling,0),
                LEAF_NODE_CELL_SIZE);

                *leaf_node_num_cells(child)+=1;
        
                strcpy(internal_node_key(parent,0),leaf_node_key(sibling,0));

                leaf_node_delete(sibling_num,0);
            }

            else{

                memcpy(leaf_node_cell(child,*leaf_node_num_cells(child)),
                leaf_node_cell(sibling,0),
                (*leaf_node_num_cells(sibling))*LEAF_NODE_CELL_SIZE);
            
                *leaf_node_num_cells(child)+=(*leaf_node_num_cells(sibling));

                void* root = get_page(0);
                memcpy(root,child,PAGE_SIZE);
                set_node_root(root,1);
                *leaf_node_next_leaf(root) = 0;

            }
        }
        else{
            sibling_num = *internal_node_child(parent,0);

            void* sibling = get_page(sibling_num);

            if(*leaf_node_num_cells(sibling)>LEAF_NODE_MIN){

                Row* row=malloc(sizeof(Row));
                row->a = *leaf_node_value(sibling,*leaf_node_num_cells(sibling)-1);
                strcpy(row->b,(char*)leaf_node_key(sibling,*leaf_node_num_cells(sibling)-1));

                leaf_node_insert(child_num,0,row);
                leaf_node_delete(sibling_num,*leaf_node_num_cells(sibling)-1);
            
                strcpy((char*)internal_node_key(parent,0),get_real_max(sibling));
            }

            else{

                memcpy(leaf_node_cell(child,*leaf_node_num_cells(child)),
                leaf_node_cell(sibling,0),
                (*leaf_node_num_cells(sibling))*LEAF_NODE_CELL_SIZE);

                *leaf_node_num_cells(child)+=(*leaf_node_num_cells(sibling));
                void* root = get_page(0);
                memcpy(root,child,PAGE_SIZE);
                set_node_root(root,1);
                *leaf_node_next_leaf(root)=0;
            }
        }
        return;
    }
    // parent == root not considered

    if(*internal_node_right_child(parent)==child_num)
    {
        sibling_num = *internal_node_child(parent,cell_num-1);

        void* sibling = get_page(sibling_num);

        if(*leaf_node_num_cells(sibling)>LEAF_NODE_MIN){

            Row* row = malloc(sizeof(Row));
            row->a = *leaf_node_value(sibling,*leaf_node_num_cells(sibling)-1);
            strcpy(row->b,(char*)leaf_node_key(sibling,*leaf_node_num_cells(sibling)-1));

            leaf_node_insert(child_num,0,row);
            leaf_node_delete(sibling_num,*leaf_node_num_cells(sibling)-1);
        
            strcpy((char*)internal_node_key(parent,cell_num-1),get_real_max(sibling));

        }

        else{
            memcpy(leaf_node_cell(sibling,*leaf_node_num_cells(sibling)),
                leaf_node_cell(child,0),
                (*leaf_node_num_cells(child))*LEAF_NODE_CELL_SIZE);
            
            *leaf_node_num_cells(sibling)+=(*leaf_node_num_cells(child));
            *internal_node_right_child(parent) = sibling_num;
            *leaf_node_next_leaf(sibling)=*leaf_node_next_leaf(child);
            internal_node_delete(parent_num,cell_num-1);
        }
    }
    else{
        int i;
        for(i =0;i<cell_num;++i)
        {
            if(*internal_node_child(parent,i)==child_num) {
                sibling_num = *internal_node_child(parent,i+1);
                break;
            }
        }

        void* sibling = get_page(sibling_num);

        if(*leaf_node_num_cells(sibling)>LEAF_NODE_MIN){
            Row* row=malloc(sizeof(Row));
            row->a = *leaf_node_value(sibling,0);
            strcpy(row->b,(char*)leaf_node_key(sibling,0));

            leaf_node_insert(child_num,*leaf_node_num_cells(child),row);
            leaf_node_delete(sibling_num,0);
        
            strcpy((char*)internal_node_key(parent,i),row->b);

        }
        else{
            memcpy(leaf_node_cell(child,*leaf_node_num_cells(child)),
                leaf_node_cell(sibling,0),
                (*leaf_node_num_cells(sibling))*LEAF_NODE_CELL_SIZE);

            *leaf_node_num_cells(child)+=(*leaf_node_num_cells(sibling));
            strcpy(internal_node_key(parent,i),get_real_max(child));
            *leaf_node_next_leaf(child)=*leaf_node_next_leaf(sibling);
            internal_node_delete(parent_num,i+1);
        }

    }

    

}

void leaf_node_delete(uint32_t leaf_num,int index){
    void* page = get_page(leaf_num);
    uint32_t num_cells = *leaf_node_num_cells(page);
    *leaf_node_num_cells(page)=num_cells-1;


    for(int i=index;i<num_cells-1;++i)
        memcpy(leaf_node_cell(page,i),leaf_node_cell(page,i+1),LEAF_NODE_CELL_SIZE);

    if(*leaf_node_num_cells(page)<LEAF_NODE_MIN)
    {
        if(is_node_root(page)==1) return;
        uint32_t parent_num = *node_parent(page);
        borrow_or_merge_leaf(parent_num,leaf_num);
    }
}

int delete_key(char* key){
    uint32_t leaf_num = find_leaf_node(0,key);
    void* page = get_page(leaf_num);

    for(int i=0;i<*leaf_node_num_cells(page);++i) 
    {
        if(strcmp(key,(char*)leaf_node_key(page,i))==0){
            leaf_node_delete(leaf_num,i);
            return 1;
        }
        else if(strcmp(key,(char*)leaf_node_key(page,i))<0) return 0;
    }
    return 0;

}

void execute_delete(){
    char* keyword = strtok(input_buffer.buffer," ");
    char* a = strtok(NULL," ");
    char* b = strtok(NULL," ");

    if(a == NULL && b == NULL){
        printf("error delete.\n");
        return;
    }

    char* key = malloc(LEAF_NODE_KEY_SIZE);
    strcpy(key,a);

    while(delete_key(key)) ;

    free(key);
    printf("\nExecuted.\n\n");

}

int main(int argc,char* argv[]){

    open_file(argv[1]);

    while(1)
    {
        print_prompt();
        read_input();
        if(input_buffer.buffer[0]=='.') meta_command();

        if(strlen(input_buffer.buffer)==0) printf("error input.\n");
        else if(strncmp(input_buffer.buffer,"insert",6)==0) execute_insert();
        else if(strncmp(input_buffer.buffer,"select",6)==0) execute_select();
        else if(strncmp(input_buffer.buffer,"delete",6)==0) execute_delete();
        else printf("error input.\n");
    }
}