
工具说明
==========================

Path
-----------

**tools.protein_regulation**

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger-dev/app/bioinfo/rna/scripts/new_PPInetwork_predict.r

功能和用途描述
-----------------------------------

预测蛋白质相互作用组，构建蛋白质相互作用网络，并输出节点属性文件


使用程序
-----------------------------------

R语言包stringdb，http://rpackages.ianhowson.com/bioc/STRINGdb/man/STRINGdb.html

主要命令及功能模块
-----------------------------------

Rscript --slave PPInetwork_predict.r diff_exp_mapped.txt 9606 PPI_result 600 0.2

参数设计
-----------------------------------



     {"name": "diff_exp_mapped", "type": "string"},
     {"name": "species", "type": "int", "default": 9606},
     {"name": "combine_score",  "type": "int", "default": 600}





运行逻辑
-----------------------------------

根据STRINGid来预测蛋白质与蛋白质之间的相互作用关系
