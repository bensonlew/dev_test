
工具说明
==========================

Path
-----------



程序安装路径
-----------------------------------

R软件

功能和用途描述
-----------------------------------

通过样本的表达量分析样本之间的联系紧密度


使用程序
-----------------------------------

R:https://www.r-project.org/

参数设计
-----------------------------------

    {"name": "diff_fpkm", "type": "infile", "format": "denovo_rna.express.express_matrix"},  #输入文件，差异基因表达量矩阵        
	{"name": "distance_method", "type": "string", "default": "euclidean"},  # 计算距离的算法        
	{"name": "method", "type": "string", "default": "all"},  # 聚类方法选择
	{"name": "lognorm", "type": "int", "default": 10}  # 画热图时对原始表进行取对数处理，底数为10或2


运行逻辑
-----------------------------------

通过相关性分析样本之间的差异性，为顾客分析自己样本组与对照组之间的差异性提供了参考。




++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++=




工具说明
==========================

Path
-----------



程序安装路径
-----------------------------------

R软件

功能和用途描述
-----------------------------------

使用Ballgown完成从stringtie中输出的数据分析。


使用程序
-----------------------------------

R:https://www.r-project.org/
Ballgown：http://www.bioconductor.org/packages/release/bioc/vignettes/ballgown/inst/doc/ballgown.html


参数设计
-----------------------------------

    {"name": "diff_fpkm", "type": "infile", "format": "denovo_rna.express.express_matrix"},  #输入文件，差异基因表达量矩阵        
	{"name": "feature", "type": "string", "default": "transcript"},  # 选择分析对象        
	{"name": "meas", "type": "string", "default": "FPKM"},  # 选择分析数据类型


运行逻辑
-----------------------------------

通过FPKM值进行数据之间差异性检验，获得差异基因


++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++



工具说明
==========================

Path
-----------



程序安装路径
-----------------------------------

R软件

功能和用途描述
-----------------------------------

使用DESeq2完成从stringtie中输出的数据分析。(补充，完成了limma和其他差异分析的R包载入)


使用程序
-----------------------------------

R:https://www.r-project.org/
http://www.bioconductor.org/packages/release/bioc/html/DESeq2.html


参数设计
-----------------------------------

    {"name": "count", "type": "infile", "format": "denovo_rna.express.express_matrix"},  # 输入文件，基因技术矩阵
    {"name": "fpkm", "type": "infile", "format": "denovo_rna.express.express_matrix"},  # 输入文件，基因表达量矩阵        
	{"name": "dispersion", "type": "float", "default": 0.1},  # edger离散值
	{"name": "min_rowsum_counts", "type": "int", "default": 2},  # 离散值估计检验的最小计数值
	{"name": "edger_group", "type": "infile", "format": "meta.otu.group_table"},  # 有生物学重复的时候的分组文件
	{"name": "sample_list", "type": "string", "default": ''},  # 选择计算表达量的样本名，多个样本用‘，’隔开,有重复时没有该参数
	{"name": "control_file", "type": "infile", "format": "denovo_rna.express.control_table"},  # 对照组文件，格式同分组文件
	{"name": "diff_ci", "type": "float", "default": 0.05},  # 显著性水平
	{"name": "diff_count", "type": "outfile", "format": "denovo_rna.express.express_matrix"},  # 差异基因计数表
	{"name": "diff_fpkm", "type": "outfile", "format": "denovo_rna.express.express_matrix"},  # 差异基因表达量表
	{"name": "gene_file", "type": "outfile", "format": "denovo_rna.express.gene_list"}
	{"name": "diff_list_dir", "type": "outfile", "format": "denovo_rna.express.gene_list_dir"},
	{"name": "gname", "type": "string"},  # 分组方案名称
	{"name": "diff_rate", "type": "float", "default": 0.01}  # 期望的差异基因比率

运行逻辑
-----------------------------------

通过FPKM值进行数据之间差异性检验，获得差异基因


