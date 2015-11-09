
工具说明
==========================

Path
-----------

**betadiversity.nmds**  # 工具目录设置，待定

程序安装路径
-----------------------------------

ordination.pl

使用程序
-----------------------------------

R语言，及其vegan包

功能和用途描述
-----------------------------------

进行NMDS分析

主要命令及功能模块
-----------------------------------

ordination.pl -type nmds -outdir outdir -dist distancematrix

参数设计
-----------------------------------

::

            {"name": "input", "type": "infile", "format": "distance_matrix"},  # 输入距离矩阵
            {"name": "output", "type": "outfile", "format": "nmds_outdir"},  # 包含样本坐标


运行逻辑
-----------------------------------

提供距离矩阵进行NMDS分析





