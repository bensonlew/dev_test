
工具说明
==========================

Path
-----------

**ref_rna.assembly.stringtie_merge**

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger-dev/app/bioinfo/rna/stringtie-1.2.4

功能和用途描述
-----------------------------------

新建文本列出转录本路径，合并转录本信息


使用程序
-----------------------------------

StringTie：http://ccb.jhu.edu/software/stringtie/

主要命令及功能模块
-----------------------------------

find -name _./output/*_.gtf > assembly_GTF_list.txt
stringtie --merge assembly_GTF_list.txt  -G <参考序列注释文件> -s <参考序列文件> -p <CPU线程> -o merge_out

参数设计
-----------------------------------

::

            {"name": "sample_bam", "type": "infile", "format": "ref_rna.bam"},  # 所有样本比对之后的bam文件
            {"name": "ref_fa", "type": "infile", "format": "sequence.fasta"},  # 参考基因文件
            {"name": "ref_gtf", "type": "infile", "format": "ref_rna.gtf"},  # 参考基因的注释文件
            {"name": "cpu", "type": "int", "default": 10},  # stringtie软件所分配的cpu数量
            # {"name": "memory", "type": "string", "default": '100G'},  # stringtie软件所分配的内存，单位为GB
            # {"name": "fr-unstranded", "type": "string"},  # 是否链特异性
            # {"name": "fr-firststrand", "type": "string"},  # 链特异性时选择正链
            # {"name": "fr-secondstrand", "type": "string"},  # 链特异性时选择负链
            {"name": "sample_gtf", "type": "outfile", "format": "ref_rna.gtf"}# 输出的gtf文件
            


运行逻辑
-----------------------------------

新建文本列出转录本路径，调用stringtie --merge,合并转录本信息

