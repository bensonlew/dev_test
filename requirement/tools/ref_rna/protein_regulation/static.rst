
工具说明
==========================

Path
-----------

**ref_rna**

程序安装路径
-----------------------------------
/mnt/ilustre/users/sanger-dev/app/bioinfo/statistical/scripts/ref-rna_stastic.py

功能和用途描述
----------------------------------

统计某一项标签对应内容的个数（目前适用于转录因子统计和RNA编辑统计）


使用程序
-----------------------------------
/mnt/ilustre/users/sanger-dev/app/bioinfo/statistical/scripts/ref-rna_stastic.py

主要命令及功能模块
-----------------------------------
python ref-rna_stastic row-data

参数设计
-----------------------------------

::

     {"name": "row-data", "type": "infile", "format": "ref_rna.protein_regulation.txt"}, #需要统计的类别

运行逻辑
----------------------------------
输入需要统计类别的列表，计算每个类别的出现次数
