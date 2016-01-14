
工具说明
==========================
根据序列id搜索相应序列信息

Path
-----------

**sequence.get_fasta_by_id**

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger/app

功能和用途描述
-----------------------------------

通过传入序列的id号，搜索fasta文件序列信息，输出id相对应的序列信息
输出结果文件为fasta_stat.xls，示例为：
Format:              FASTA
Type (of 1st seq):   DNA
Number of sequences: 7
Total # residues:    3488
Smallest:            474
Largest:             513
Average length:      498.3

和fasta_len.xls，其示例为：NAME一行为序列的id名称，LEN一列为ID对应序列的长度，DESCRIPTION一列为描述
NAME    LEN DESCRIPTION 
OTU1    506 N21_H2HPGJ401DS3D3  
OTU2    513 N19_H2HPGJ401ANU2L  
OTU3    474 N25_H2JNDKU01DVSFZ  
OTU4    506 N24_H2HPGJ401DOWEJ  
OTU5    508 N4_HZ2UV8T01A7CAE   
OTU6    497 N7_H0DSY3J02GEHTS   
OTU7    484 N7_HZ2UV8T01BDM61     

使用程序
-----------------------------------


主要命令及功能模块
-----------------------------------


参数设计
-----------------------------------

::

    {"name": "fasta", "type": "infile", "format": "sequence.fasta"} # 输入文件
    {"name": "id", "type": "string"}  #序列ID号


运行逻辑
-----------------------------------

通过传入序列的id号，搜索fasta文件序列信息，输出id相对应的序列信息

