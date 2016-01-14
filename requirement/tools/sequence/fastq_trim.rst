
工具说明
==========================

Path
-----------

**sequence.fastq_trim**

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger/app/trim/Trimmomatic-0.35

使用程序
-----------------------------------

trimmomatic-0.35.jar: http://www.usadellab.org/cms/?page=trimmomatic

功能和用途描述
-----------------------------------

主要针对Illumina测序的fastq数据进行去接头，剪切，质量过滤等操作

主要命令及功能模块
-----------------------------------

java -jar trimmomatic-0.35.jar PE [-threads <threads>] [-phred33|-phred64] [-trimlog <trimLogFile>] [-quiet] [-validatePairs] [-basein <inputBase> | <inputFile1> <inputFile2>] [-baseout <outputBase> | <outputFile1P> <outputFile1U> <outputFile2P> <outputFile2U>] <trimmer1>...
   or:
java -jar trimmomatic-0.35.jar SE [-threads <threads>] [-phred33|-phred64] [-trimlog <trimLogFile>] [-quiet] <inputFile> <outputFile> <trimmer1>...

参数设计
-----------------------------------

::

    {"name": "fastq", "type": "infile", "format": "sequence.fastq"},  # 输入的正向fastq，或者单个fastq
    {"name": "fastq_re", "type": "infile", "format": "sequence.fastq"},  # 输入的反向fastq
    {"name": "remove_adapter", "type": "bool", "default": False},  # 是否去接头
    {"name": "mode", "type": "string", "default": "simple"},  # palindrome 去接头的模式两种，一种是普通模式，一种是比对双端模式，单个fastq只有简单模式
    {"name": "adapter_mode", "type": "string", "default": "TruSeq3-PE"},  # TruSeq3-PE,TruSeq2-PE,custom 接头模式已有两种Illumina接头可供选择。如果自定义，需要提供接头，如果采用palindrome模式，必须提供pre_adaptor，pre_adaptor_re，否则需要提供normal_adaptor。
    {"name": "pre_adaptor", "type": "string", "default": "TACACTCTTTCCCTACACGACGCTCTTCCGATCT"},
    {"name": "pre_adaptor_re", "type": "string", "default": "GTGACTGGAGTTCAGACGTGTGCTCTTCCGATCT"},
    {"name": "phred", "type": "string", "default": "Detect"},  # phred33, phred64 质量类型
    {"name": "seedmismatch", "type": "int", "default": 1},  # adapter在局部种子匹配的错配率
    {"name": "palindrome_matchscore", "type": "int", "default": 30},  # palindrome模式下的正反序列匹配得分
    {"name": "simple_matchscore", "type": "int", "default": 10},  # 接头匹配的得分限定
    {"name": "normal_adaptor", "type": "string", "default":
     "TACACTCTTTCCCTACACGACGCTCTTCCGATCT,GTGACTGGAGTTCAGACGTGTGCTCTTCCGATCT"},
    {"name": "run_head_cut", "type": "bool", "default": False},  # 去序列头固定长度
    {"name": "head_cut", "type": "int", "default": 0},
    {"name": "run_start_reserve", "type": "bool", "default": False},  # 从头保留固定长度
    {"name": "start_reserve", "type": "int", "default": 999},
    {"name": "run_start_quality", "type": "bool", "default": False},  # 起始质量控制
    {"name": "start_quality", "type": "int", "default": 0},
    {"name": "run_end_quality", "type": "bool", "default": False},  # 结尾质量控制
    {"name": "end_quality", "type": "int", "default": 0},
    {"name": "run_slidingwindow", "type": "bool", "default": False},  # 滑动质量窗口，过滤
    {"name": "window", "type": "int", "default": 3},  # 窗口大小
    {"name": "window_quality", "type": "int", "default": 0},  # 窗口质量控制
    {"name": "run_len_fliter", "type": "bool", "default": False},  最短长度过滤
    {"name": "len_fliter", "type": "int", "default": 999},
    {"name": "run_quality_fliter", "type": "bool", "default": False},  # 序列平均质量过滤
    {"name": "quality_fliter", "type": "int", "default": 0},
    {"name": "outfastq1", "type": "outfile", "format": "sequence.fastq"},
    {"name": "outfastq2", "type": "outfile", "format": "sequence.fastq"}


运行逻辑
-----------------------------------

根据设定的条件对fastq文件进行trim和过滤，如果提供反向序列文件则一起进行trim，去接头的模式下，有双端回文模式，需要提供前后prefix。
