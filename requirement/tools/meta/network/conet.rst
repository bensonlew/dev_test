
工具说明
==========================

Path
-----------

**meta.network.conet**

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger/app/meta/CoNet3

功能和用途描述
-----------------------------------

用于构建共相关性网络图

使用程序
-----------------------------------

CoNet.jar

主要命令及功能模块
-----------------------------------

java be.ac.vub.bsb.cooccurrence.cmd.CooccurrenceAnalyser --input myInputMatrix.txt --matrixtype incidence --method distrib --lowerT 0.5 --output coocNetworkHypergeom.gml --format gml


参数设计-----------------------------------


::

            {"name": "data_file", "type": "infile", "format": "meta.otu.otu_table"}  # 输入数据矩阵
            {"name": "feature_file", "type": "infile", "format": "meta.env_table"},  # 输入环境特征文件
            {"name": "method", "type": "string", "default": "correl_spearman"},  # Cooccurrence方法
            {"name": "lower_threshold", "type": "float", "default": 0.6},  # Cooccurrence 阈值，最小值
            {"name": "upper_threshold", "type": "float", "default": 1},  # Cooccurrence 阈值，最大值
            {"name": "network_file", "type": "outfile", "format": "meta.gml"}，  # 输出网络图文件
            {"name": "randomization", "type": "bool", "default": True},  # 是否进行网络图随机化计算
            {"name": "iterations", "type": "int", "default": 100},  # 随机化迭代次数
            {"name": "resamplemethod", "type": "string", "default": "permute"},  # 重抽样方法 
           {"name": "pval_threshold", "type": "float", "default": 0.05},  # 重抽样方法 



运行逻辑
-----------------------------------

当参数 ``randomization``等于True时，可以设置`iterations`,`resamplemethod`,`pval_threshold`


