
工具说明
==========================

Path
-----------

**tools.paternity_test**

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger-dev/app/bioinfo/medical/scripts/pt_dup.R

功能和用途描述
-----------------------------------

输入的是母本与胎儿的tab文件，然后father_path中保存的是所有的父亲的tab文件，查重的时候会直接去father_path中遍历所有的父亲tab然后完成查重分析


使用程序
-----------------------------------

R脚本文件

主要命令及功能模块
-----------------------------------

Rscript pt_dup.R mom_tab preg_tab 2 ref_point father_path

参数设计
-----------------------------------


            {"name": "mom_tab", "type": "infile", "format": "paternity_test.tab"},
            {"name": "preg_tab", "type": "infile", "format": "paternity_test.tab"},
            {"name": "ref_point", "type": "infile", "format": "paternity_test.rda"},
            {"name": "err_min", "type": "int", "default": 2},
            {"name": "father_path", "type": "string"}  # 输入父本tab文件的所在路径


运行逻辑
-----------------------------------

