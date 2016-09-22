
工具说明
==========================

Path
-----------

**ref_rna.protein_regulation**

功能和用途描述
-----------------------------------

用于调用具体的蛋白质互作网络分析的3个tools

主要命令及功能模块
-----------------------------------
1. 将ensamble id map到string数据库中的STRINGid上
2. 根据STRINGid来预测蛋白质互作组
3. 对蛋白质互作组的数据进行分析

参数设计
-----------------------------------

::

     {"name": "diff_exp", "type": "infile", "format": "ref_rna.txt"},  #差异基因ID mapping后文件
     {"name": "species", "type": "int", "default": 9606}, #设置物种
     {"name": "combine_score",  "type": "int", "default": 600}, #蛋白质之间相互作用可能性，值越大越好
     {"name": "logFC",  "type": "float", "default": 0.2}, #logFC>0.2 && logFC <-0.2 的差异基因



运行逻辑
-----------------------------------

先判断Map输出目录中是否有diff_exp_mapped.txt文件
如果没有该文件，就运行map, ppinetwork_predict, ppinetwork_topology
如果有该文件，就运行ppinetwork_predict, ppinetwork_topology
