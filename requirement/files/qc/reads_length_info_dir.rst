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

pass

格式举例
-----------------------------------

::
 ./
 ../
 step1.reads_length_info
 step20.reads_length_info
 step50.reads_length_info
 step100.reads_length_info


属性及其含义
-----------------------------------

* ``file_number``   文件夹中reads_length_info文件的数目

相关方法
-----------------------------------

``type_check``  检查这个文件夹下的文件类型是否正确
``number_check``    检查这个文件夹下的文件数目是否正确
