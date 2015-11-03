工具说明
==========================

Path
-----------

**metastat.lefse**

功能和用途描述
-----------------------------------

用于两组或多组样本在任一水平上的lefse分析

主要命令及功能模块
-----------------------------------

metastat_package


参数设计
-----------------------------------

::

            {"name": "input", "type": "infile", "format": "otuTable"},  # 输入文件
            {"name": "group", "type": "infile", "format": "groupfile"},  # 输入分组文件
            {"name": "lefse_LDA", "type": "outfile", "format": "pdf"}  # 输出结果
            {"name": "lefse_clado", "type": "outfile", "format": "pdf"}  # 输出结果
            {"name": "lefse_xls", "type": "outfile", "format": "lefse"}  # 输出结果


运行逻辑
-----------------------------------
当传入参数input、group、output时，就可以运行此模块