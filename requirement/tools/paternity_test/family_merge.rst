
工具说明
==========================

Path
-----------

**tools.paternity_test**

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger-dev/app/bioinfo/medical/scripts/family_joined.R

功能和用途描述
-----------------------------------

读入父本，母本，胎儿的tab文件，并进行基因分型，注释，形成组合后的家系表


使用程序
-----------------------------------

R脚本文件

主要命令及功能模块
-----------------------------------

Rscript family_joined.R dad_tab mom_tab preg_tab err_min ref_point

参数设计
-----------------------------------


    {"name": "dad_tab", "type": "infile", "format": "paternity_test.tab"},
    {"name": "mom_tab", "type": "infile", "format": "paternity_test.tab"},
    {"name": "preg_tab", "type": "infile", "format": "paternity_test.tab"},
    {"name": "ref_point", "type": "infile","format":"paternity_test.rda"},
    {"name": "err_min", "type": "int", "default": 2},
    {"name": "tab_merged", "type": "infile", "format": "paternity_test.rdata"}


运行逻辑
-----------------------------------
分别读入父本，母本，胎儿三个样本的tab文件，然后进行分型，注释，形成合并的家系表
