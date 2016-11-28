
工具说明
==========================

Path
-----------

**Mergefeaturecounts**

程序安装路径
-----------------------------------

功能和用途描述
-----------------------------------

合并多个样本生成基因表达矩阵

使用程序
-----------------------------------



主要命令及功能模块
-----------------------------------

调用gene_count.py 脚本合并多个样本的基因表达之
python gene_count.py -s <sample_express> -t <express_type> -m <express_method> 

参数设计
-----------------------------------
::
            {"name": "featurecounts_files", "type":"infile","format": "ref_rna.dir"},  # 输入多个样本featurecounts结果文件
            {"name": "gene_count", "type": "outfile","format": "ref_rna.txt"},  # 输出基因count表
            {"name": "gene_fpkm", "type": "outfile","format": "ref_rna.txt"},  # 输出基因fpkm表
            {"name": "exp_way", "type": "string","default": "both"},  # 默认同时输出基因的count和fpkm表 count, fpkm, both
            {"name": "gene_id", "type": "string","default": "ensembl"},  # 默认输出基因的ensembl id
            {"name": "cpu", "type": "int", "default": 10},  # 设置CPU
            {"name": "max_memory", "type": "string", "default": "100G"}  # 设置内存

运行逻辑
-----------------------------------

默认输出基因的count值和fpkm值



