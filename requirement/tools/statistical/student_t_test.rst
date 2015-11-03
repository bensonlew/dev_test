工具说明
==========================

Path
-----------

**metastat.student_t_test**

功能和用途描述
-----------------------------------

用于两组样本在任一水平上的显著性差异分析，有参检验

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
            {"name": "type", "type": "string", "default":two_side},  # 选择单尾或双尾检验
            {"name": "output", "type": "outfile", "format": "metastat"}  # 输出结果


运行逻辑
-----------------------------------
当传入参数input、group、output时，就可以运行此模块