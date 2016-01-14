
工具说明
==========================

Path
-----------

**sequence.fasta_trim**

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger/app/Python/bin/python

使用程序
-----------------------------------

cutadapt: http://cutadapt.readthedocs.org/en/stable/index.html

功能和用途描述
-----------------------------------

对fasta或者带有qual文件进行trim和过滤操作

主要命令及功能模块
-----------------------------------

python cutadapt -a ADAPTER [options] [-o output.fastq] input.fasta input.qual

参数设计
-----------------------------------

::

    {"name": "fasta", "type": "infile", "format": "sequence.fasta"},  # 输入fasta文件，必须以fasta以及相关扩展名
    {"name": "qual", "type": "infile", "format": "sequence.qual"},  # 输入质量文件，与fasta相对应。以.qual为扩展名
    {"name": "phred", "type": "string", "default": "phred33"},  # 只可以为默认或者phred64
    {"name": "remove_adapter", "type": "bool", "default": False},  # 是否去除adapter
    {"name": "mismatch_rate", "type": "float", "default": 0.1},  # adapter的错配比率，必须在[0-1)之间
    {"name": "border_minmatch", "type": "int", "default": 5},  # 边界最低匹配数
    {"name": "indel", "type": "bool", "default": True},  # 是否考虑匹配的indel
    {"name": "mode", "type": "string", "default": "5\'-end"},  # 默认或者3\'-end，选择去哪一边的接头
    {"name": "adapter", "type": "string", "default":
     "TACACTCTTTCCCTACACGACGCTCTTCCGATCT,GTGACTGGAGTTCAGACGTGTGCTCTTCCGATCT"},  # 接头序列，如果为多个，以逗号隔开，不要有空格
    {"name": "run_head_cut", "type": "bool", "default": False},  # 是否去头一定的碱基数
    {"name": "head_cut", "type": "int", "default": 0},
    {"name": "run_end_cut", "type": "bool", "default": False},  # 是否去尾的一定碱基数
    {"name": "end_cut", "type": "int", "default": 0},
    {"name": "run_start_quality", "type": "bool", "default": False},  # 对开头进行质量过滤
    {"name": "start_quality", "type": "int", "default": 0},
    {"name": "run_end_N", "type": "bool", "default": False},  # 是否去除结尾的N碱基
    {"name": "run_end_quality", "type": "bool", "default": False},  # 对结尾进行质量过滤
    {"name": "end_quality", "type": "int", "default": 0},
    {"name": "run_minlen_fliter", "type": "bool", "default": False},  # 最短碱基过滤
    {"name": "minlen_fliter", "type": "int", "default": 0},
    {"name": "run_maxlen_fliter", "type": "bool", "default": False},  # 最长碱基过滤
    {"name": "maxlen_fliter", "type": "int", "default": 999},
    {"name": "run_count_N", "type": "bool", "default": False},  # 容忍的最大N碱基数量
    {"name": "max_N", "type": "float", "default": 10},
    {"name": "fastq_return", "type": "bool", "default": False},  # 如果提供了相应的qual质量文件，可以选择返回fastq文件还是一个fasta一个qual文件
    {"name": "outfastq", "type": "outfile", "format": "sequence.fastq"},
    {"name": "outfasta", "type": "outfile", "format": "sequence.fasta"},
    {"name": "outqual", "type": "outfile", "format": "sequence.qual"}


运行逻辑
-----------------------------------

根据设定的条件对fasta文件进行trim和过滤，如果提供qual文件，可以进行质量相关过滤，否则报错，结果为fasta，如果提供qual文件，可以选择生成fastq文件。
