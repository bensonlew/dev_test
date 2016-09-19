
工具说明
==========================

Path
-----------

**ref_rna.protein_regulation.PPInetwork**

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger-dev/app/bioinfo/rna/scripts/map.r

功能和用途描述
-----------------------------------

将gene_id与STRINGid进行对应，取代了blastx


使用程序
-----------------------------------

R脚本文件

主要命令及功能模块
-----------------------------------

Rscript --slave map.r DeS1_vs_DeS2.xls 9606 PPI_result 

参数设计
-----------------------------------

::

     {"name": "diff_exp", "type": "infile", "format": "ref_rna.xls"},  #差异基因表达详情表
     {"name": "species", "type": "int", "default": 9606}, #设置物种
     



运行逻辑
-----------------------------------

根据ensamble ID map 到STRINGid
