
工具说明
==========================
生成稀释性表

Path
-----------

**tools.meta.diversity.alpha.rarefaction**

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger/app

功能和用途描述
-----------------------------------

比较测序数据量不同的样本中物种的丰富度；说明样本的测序数据量是否合理。


使用程序
-----------------------------------

otu2shared
mothur

主要命令及功能模块
-----------------------------------

otu2shared.pl -i otu_table.xls -l 0.97 -o otu.shared
rarefaction.single(shared=otu.shared,calc=sobs-chao-shannon,groupmode=f,freq=100,processors=10)"


参数设计
-----------------------------------

::

    {"name": "OTUtable", "type": "infile", "format": "txt"},  # 输入文件
    {"name": "indices", "type": "string", "default": "all"},  # 指数类型
    {"name": "random_number", "type": "int", "default": 100},  # 随机取样数   
    {"name": "rarefaction", "type": "outfile", "format": "txt"}  # 输出结果


运行逻辑
-----------------------------------

传入OTUtable后，根据默认参数值，输出统计结果。