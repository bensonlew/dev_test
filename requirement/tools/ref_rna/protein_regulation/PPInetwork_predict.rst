
工具说明
==========================

Path
-----------

**ref_rna.protein_regulation**

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger-dev/app/bioinfo/rna/scripts/PPInetwork_predict.r

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

::

     {"name": "diff_exp_mapped", "type": "infile", "format": "ref_rna.txt"},  #差异基因ID mapping后文件
     {"name": "species", "type": "int", "default": 9606}, #设置物种
     {"name": "combine_score",  "type": "int", "default": 600}, #蛋白质之间相互作用可能性，值越大越好
     {"name": "logFC",  "type": "float", "default": 0.2}, #logFC>0.2 && logFC <-0.2 的差异基因
     {"name": "species_list", "type": "string"} #读入string数据库中存在的物种taxon_id，用于检测用户输入的taxon是不是正确



运行逻辑
-----------------------------------

根据STRINGid来预测蛋白质与蛋白质之间的相互作用关系
