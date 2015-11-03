
工具说明
==========================

Path
-----------

**betadiversity.pcoa**  # 工具目录设置，待定

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger/app/  # 使用R语言，路径待定

使用程序
-----------------------------------

ordination.pl

功能和用途描述
-----------------------------------

进行PCOA分析

主要命令及功能模块
-----------------------------------

ordination.pl -type pcoa -outdir outdir -dist distancematrix

参数设计
-----------------------------------

::

            {"name": "method", "type": "string", "default": "bray_curtis"},  # 默认计算矩阵的方法，bray_curtis，待定
            {"name": "input1", "type": "infile", "format": "OtuTable"},  # 输入文件,为otutable
            {"name": "input2", "type": "infile", "format": "distancematrix"},  # 如果输入距离矩阵，将覆盖otutable和距离算法。
            {"name": "output1", "type": "outfile", "format": "CoordinateTable"},  # 样本的坐标表
            {"name": "output2", "type": "outfile", "format": "CoordinateTable"},  # 样本权重值表
            {"name": "output4", "type": "outfile", "format": "WeightTable"},  # 各主成分的解释度权重值表。


运行逻辑
-----------------------------------

如果提供距离矩阵，将直接计算PCOA分析得到几个结果表，如果没有，则需要提供otu表和距离算法，计算获得距离矩阵，再进行PCOA分析。





