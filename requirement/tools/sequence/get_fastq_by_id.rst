
工具说明
==========================
根据序列id搜索相应序列信息

Path
-----------

**sequence.get_fastq_by_id**

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger/app

功能和用途描述
-----------------------------------

通过传入序列的id号，搜索fastq文件序列信息，生成只有目标id的fastq文件
使用程序
输出文件为fastq_stat.xls,示例为：
column  count   min max sum mean    Q1  med Q3  IQR lW  rW  A_Count C_Count G_Count T_Count N_Count Max_count
1   250 12  34  8264    33.06   34  34  34  0   34  34  39  48  58  105 0   250
2   250 12  34  8271    33.08   34  34  34  0   34  34  53  135 31  31  0   250
-----------------------------------


主要命令及功能模块
-----------------------------------


参数设计
-----------------------------------

::

    {"name": "fastq", "type": "infile", "format": "sequence.fastq"} # 输入文件
    {"name": "id", "type": "string"}  #序列ID号


运行逻辑
-----------------------------------

通过传入序列的id号，搜索fastq文件序列信息,生成只有目标id的fastq文件

