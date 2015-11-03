
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

            {"name": "method", "type": "string", "default": "bray_curtis"},  # 默认计算矩阵的方法，bray_curtis，待定
            {"name": "input1", "type": "infile", "format": "OtuTable"},  # 输入文件,为otutable
            {"name": "input2", "type": "infile", "format": "distancematrix"},  # 如果输入距离矩阵，将覆盖otutable和距离算法。
            {"name": "output1", "type": "outfile", "format": "CoordinateTable"},  # 样本的坐标表


运行逻辑
-----------------------------------

一般直接提供距离矩阵进行NMDS分析，如果不提供，则通过提供otutable和距离算法，计算获得距离矩阵。再进行NMDS分析。





