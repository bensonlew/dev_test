格式说明
==========================

Path
-----------

**meta.reads_length_info_dir**


功能和用途描述
-----------------------------------

用于存储以某种规则放置在一起reads_length_info文件


格式定义文档
-----------------------------------

规定允许的步长为1,20,50,100，因此文件夹下的reads_len_info文件数目应该是四个

格式举例
-----------------------------------

::
 ./
 ../
 step_1.reads_length_info
 step_20.reads_length_info
 step_50.reads_length_info
 step_100.reads_length_info


属性及其含义
-----------------------------------

* ``file_number``   文件夹中reads_length_info文件的数目

相关方法
-----------------------------------

``get_reads_len_info_number`` 获取文件夹下reads_len_info文件的数目
