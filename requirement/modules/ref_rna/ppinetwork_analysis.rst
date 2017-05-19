
工具说明
==========================

Path
-----------

**module.protein_regulation**

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



{"name": "diff_exp_gene", "type": "infile", "format": "rna.ppi"},
{"name": "species", "type": "int", "default": 9606},
{"name": "combine_score", "type": "int", "default": 300}


运行逻辑
-----------------------------------

先判断Map输出目录中是否有diff_exp_mapped.txt文件
如果没有该文件，就运行map, ppinetwork_predict, ppinetwork_topology
如果有该文件，就运行ppinetwork_predict, ppinetwork_topology
