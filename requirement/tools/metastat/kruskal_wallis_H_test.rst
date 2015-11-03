工具说明
==========================

Path
-----------

**metastat.kruskal_wallis_H_test**

功能和用途描述
-----------------------------------

用于多组样本在任一水平上的显著性差异分析，非参检验

主要命令及功能模块
-----------------------------------

metastat_package


参数设计
-----------------------------------

::

            {"name": "input", "type": "infile", "format": "otuTable"},  # 输入文件
            {"name": "pvalue_filter", "type": "float", "default": "0.05"},  # 显著性水平
            {"name": "group", "type": "infile", "format": "groupfile"},  # 输入分组文件
            {"name": "correction", "type": "string", "default": No correction},  # 多重检验校正
            {"name": "post", "type": "string", "default":Tukey-Kranmer },  # Pos-hoc test参数
            {"name": "hoc", "type": "float", "default":0.95 },  # Pos-hoc test参数
            {"name": "output", "type": "outfile", "format": "metastat"}  # 输出结果


运行逻辑
-----------------------------------
当传入参数input、group、output时，就可以运行此模块
