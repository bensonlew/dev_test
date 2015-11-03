格式说明
==========================

Path
-----------

**meta.tax_summary_abs_dir**


功能和用途描述
-----------------------------------

用于存储由一个fasta_dir分析得到的各个分类等级的otu_table和otu_biom,共计14个文件
otu_table里的数值为绝对值


格式定义文档
-----------------------------------

pass

格式举例
-----------------------------------

::
 ./
 ../
 otu_taxa_table_L1.otu_table
 otu_taxa_table_L1.otu_biom
 otu_taxa_table_L2.otu_table
 otu_taxa_table_L2.otu_biom
 ....


属性及其含义
-----------------------------------

* ``file_number``   文件夹中base_info文件的数目

相关方法
-----------------------------------

``type_check``  检查这个文件夹下的文件类型是否正确
``number_check``    检查这个文件夹下的文件数目是否正确
