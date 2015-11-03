
工具说明
==========================

Path
-----------

**betadiversity.pca**  # 工具目录设置，待定

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger/app/  # 使用R语言，路径待定

使用程序
-----------------------------------

ordination.pl

功能和用途描述
-----------------------------------

进行PCA分析

主要命令及功能模块
-----------------------------------

ordination.pl -type pca -outdir outdir -community otutable -environment envdata

参数设计
-----------------------------------

::

            {"name": "input1", "type": "infile", "format": "OtuTable"},  # 输入文件,为otutable
            {"name": "input2", "type": "infile", "format": "EnvTable"},  # 可选输入文件，环境因子表。
            {"name": "output1", "type": "outfile", "format": "CoordinateTable"},  # 样本的坐标表
            {"name": "output2", "type": "outfile", "format": "CoordinateTable"},  # otu权重值表
            {"name": "output3", "type": "outfile", "format": "CoordinateTable"},  # 环境因子坐标表，在提供了环境因子表的时候有此表
            {"name": "output4", "type": "outfile", "format": "WeightTable"},  # 各主成分的解释度权重值表。


运行逻辑
-----------------------------------

根据提供的otu表和可选环境因子表，调用脚本计算出样本的坐标表，环境因子的向量，并提供otu权重值表和主成分解释度表。





