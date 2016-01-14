
工具说明
==========================
fastq文件序列信息统计

Path
-----------

**sequence.fastq_stat**

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger/app

功能和用途描述
-----------------------------------

通过运行fastxtoolkit软件，统计fastq文件每个column的碱基数、最大最小碱基质量得分、Q3、IQR等值。

使用程序
-----------------------------------

fastxtoolkit

主要命令及功能模块
-----------------------------------

/mnt/ilustre/users/sanger/app/fastxtoolkit/bin/fastx_quality_stats -i fastq -o fastq_stat

参数设计
-----------------------------------

::

    {"name": "fastq", "type": "infile", "format": "sequence.fastq"} # 输入文件

运行逻辑
-----------------------------------

传入fastq文件后运行fastxtoolkit软件统计Fastq文件的序列信息

