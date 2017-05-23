工具说明
==========================

Path
-----------

**tools.rna.merge_rsem**

程序安装路径
-----------------------------------

暂无

功能和用途描述
-----------------------------------

合并多个样本的表达量,根据是否有无生物学,分别计算样本和生物学重复的密度分布图;

调用(使用)程序
-----------------------------------
fpkm: "/mnt/ilustre/users/sanger/app/bioinfo/rna/scripts/abundance_estimates_to_matrix.pl"
tpm:
"/mnt/ilustre/users/sanger/app/bioinfo/rna/trinityrnaseq-2.2.0/util/abundance_estimates_to_matrix.pl"

主要命令及功能模块
-----------------------------------
暂无

参数设计
-----------------------------------

::        

            {"name": "gtf_ref", "type": "infile", "format": "gene_structure.gtf"}, #参考基因组gtf文件
            {"name": "gtf_cmp", "type": "infile", "format": "gene_structure.gtf"}, #拼接生成的cmp.annotated.gtf文件
            {"name": "is_class_code", "type":"bool"}, #是否计算class_code信息
            {"name": "rsem_files", "type": "infile", "format": "rna.rsem_dir"},  # SE测序，包含所有样本的fq文件的文件夹
            {"name": "tran_count", "type": "outfile", "format": "rna.express_matrix"},
            {"name": "gene_count", "type": "outfile", "format": "rna.express_matrix"},
            {"name": "tran_fpkm", "type": "outfile", "format": "rna.express_matrix"},
            {"name": "gene_fpkm", "type": "outfile", "format": "rna.express_matrix"},
            {"name": "is_duplicate", "type": "bool"}, # 是否有生物学重复
            {"name": "class_code", "type":"outfile", "format":"rna.express_matrix"}, #class_code 文件基本信息
            {"name": "edger_group", "type":"infile", "format":"sample.group_table"},
            {"name": "exp_way", "type": "string", "default": "fpkm"},

运行逻辑
-----------------------------------

1. 参数is_duplicate判断是否有生物学重复, 如果有, 则需要设置is_duplicate为False;且设置edger_group参数;
2. 设置gtf_ref和gtf_cmp文件，输出class_code信息, 即拼接生成的基因id,转录本id,class_code,参考基因id,参考转录本id之间的对应关系;
