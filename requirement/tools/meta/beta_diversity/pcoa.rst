
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

            {"name": "input", "type": "infile", "format": "distancematrix"},  # 输入距离矩阵
            {"name": "output", "type": "outfile", "format": "pcoa_outdir"},  # 包含样本的坐标表，样本权重表，主成分解释度表

运行逻辑
-----------------------------------

提供距离矩阵，计算PCOA分析得到几个结果表




