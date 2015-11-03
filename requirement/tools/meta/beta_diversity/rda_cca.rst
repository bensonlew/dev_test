
工具说明
==========================

Path
-----------

**betadiversity.rdacca**  # 工具目录设置，待定

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger/app/  # 使用R语言，路径待定

使用程序
-----------------------------------

ordination.pl

功能和用途描述
-----------------------------------

进行RDA/CCA分析

主要命令及功能模块
-----------------------------------

ordination.pl -type rda -outdir outdir -community otutable -environment envdata

参数设计
-----------------------------------

::

            {"name": "DCA", "type": "bool", "default": "TRUE"},  # 是否进行DCA分析
            {"name": "method", "type": "string"},  # 默认分析方法，根据DCA分析结果选择分析方法
            {"name": "input1", "type": "infile", "format": "OtuTable"},  # 输入文件,为otutable
            {"name": "input2", "type": "infile", "format": "EnvTable"},  # 输入文件，环境因子表。
            {"name": "output1", "type": "outfile", "format": "CoordinateTable"},  # 样本的坐标表
            {"name": "output2", "type": "outfile", "format": "CoordinateTable"},  # 物种坐标表
            {"name": "output3", "type": "outfile", "format": "CoordinateTable"},  # 环境因子坐标表
            {"name": "output4", "type": "outfile", "format": "CoordinateTable"},  # DCA分析结果表


运行逻辑
-----------------------------------

如果选择DCA分析，则进行DCA分析，在等RDA或者CCA参数传入，进行计算或者结果，也可直接选择RDA或CCA直接进行分析。





