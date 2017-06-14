
工具说明
==========================

Path
-----------

**tools.protein_regulation**

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger-dev/app/bioinfo/rna/scripts/new_map.r

功能和用途描述
-----------------------------------

实现将ensemble，gene_id等比对到string数据库中，用于后面的蛋白质互作组的预测


使用程序
-----------------------------------

R脚本文件

主要命令及功能模块
-----------------------------------

Rscript --slave new_map.r gene_id.txt 9606 PPI_result

参数设计
-----------------------------------


     {"name": "diff_exp_gene", "type": "infile", "format": "rna.ppi"},
     {"name": "species", "type": "int", "default": 9606}


运行逻辑
-----------------------------------

根据ensamble ID map 到STRINGid
