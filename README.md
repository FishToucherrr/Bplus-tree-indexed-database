# Project2

> refer to: https://cstack.github.io/db_tutorial/

## database

+ B+树索引
+ 有一个全局变量cache，和函数get_page() get_new_page() 来负责内存读写和替换问题
+ B+树的每个结点为一页，每一页的页号为在磁盘文件中的位置。get_page()根据页号获得指向该页的指针。
+ get_new_page（）获得一个空白的空页，在内存未满时用内存的空闲页（上限100页），满时用随机替换（更改为FIFO ）选择一页写回文件，再返回该页指针进行操作。
+ cache保存file_descriptor,指针数组，另一个数组保存每一个指针指向页数
+ 保证第0页始终为根节点
+ 叶节点内存格式：头部（meta）14 bytes，每个cell 16 bytes (4+12)。头部包含next_leaf。
+ 内部节点内存格式：头部（meta）14bytes，每个cell 16 bytes(4+12)，其中4为孩子。头部包含右孩子。
+ 没有特别优化的部分，下面只是写代码的部分思路。

## insert

+ 原则是每个内部节点的键值是左孩子子树的最大键值。
+ 键值相同时，认为后插入的键值小，排在前面
+ 首先找到应插入的叶节点。如果键值对数未达上限，则正常插入。
+ 否则先分裂后插入分裂的叶节点；其次将新的叶节点插入上一级内部节点。
+ 内部节点同上述操作。
+ 如果分裂的是根节点，则需新分配一页，将原根节点写入新页，然后初始化，新页作为根页的左孩子，根页始终保持为0页。

## delete

1. 首先从leaf_node 开始删，不够则 borrow_or_merge_leaf（非root）
2. 如果是左一，则与左二/右一操作
3. 如果是右一，则与右二操作
4. 现在考虑会不会出现一个内部节点只有一个孩子：

+ 首先正常insert之后不会出现；
+ 在delete操作时，首先合并两个leaf_node
+ 页数多的时候父节点肯定不止一个，如果此时少了就继续向上borrow_or_merge
+ 肯定会在孩子数小于等于2之前解决问题 所以不会出现这个问题
+ 此时要实现的是内部节点borrow_or_merge
+ 如果merge到了root，到root才会出现合并完只剩一个节点的情况
+ 此时合并剩下的节点为新的根节点



5. 考虑一个细节问题：父节点更新

+ borrow不用考虑
+ merge，具体操作是：
+ 将其中一页的cell全部插入另一个中；
+ 删除原来的节点（在上述前提下不会是右孩子）
+ 更新键值（如果是右孩子则不用）
+ num_keys --；
+ 在上述过程中 不需要更新父节点
+ 此后新一轮borrow_or_merge
+ 要更新父节点的情况只有一个，改变root时（是因为一直保持root在page[0]）

## next_leaf

1. insert 过程中需要处理；
2. delete过程中
   + 首先一个leaf_node 不够，borrow_or_merge_leaf
   + borrow时不变
   + merge的几种情况：
   + parent总共就两个节点（root）合并完不用考虑。
   + 多于两个节点：
   + child为中间结点：与右节点合并，保留左节点，后续为下一个
   + child为右节点：与右二节点合并，保留右二，更改右二后续节点
3. 上述合并策略和我们一开始的写法并不一样。好在只有叶节点的合并需要考虑next_leaf,因此只调整叶节点的合并策略。

## run

在linux环境下编译运行，参数为数据文件 xxx.db
