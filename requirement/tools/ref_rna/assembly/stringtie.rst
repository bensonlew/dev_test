
工具说明
==========================

Path
-----------

**ref_rna.assembly.stringtie**

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger-dev/app/bioinfo/rna/stringtie-1.2.4

功能和用途描述
-----------------------------------

单个样本拼接，产生各自的转录本注释文件


使用程序
-----------------------------------

StringTie：http://ccb.jhu.edu/software/stringtie/

主要命令及功能模块
-----------------------------------

stringtie <样本bam格式的文件> -p <cpu线程数> -G <参考序列gtf格式文件> -s <参考基因组fa格式文件> -o sample_output 

参数设计
-----------------------------------

::

            {"name": "sample_bam", "type": "infile", "format": "ref_rna.bam"},  # 所有样本比对之后的bam文件
            {"name": "ref_fa", "type": "infile", "format": "sequence.fa"},  # 参考基因文件
            {"name": "ref_gtf", "type": "infile", "format": "ref_rna.gtf"},  # 参考基因的注释文件
            {"name": "cpu", "type": "int", "default": 10},  # stringtie软件所分配的cpu数量
            {"name": "memory", "type": "string", "default": '100G'},  # stringtie软件所分配的内存，单位为GB
            {"name": "fr-unstranded", "type": "logical"},  # 是否链特异性
            {"name": "fr-firststrand", "type": "string"},  # 链特异性时选择正链
            {"name": "fr-secondstrand", "type": "string"},  # 链特异性时选择负链
            {"name": "sample_gtf", "type": "outfile","format":"ref_rna.gtf"},  # 输出的gtf文件
            


运行逻辑
-----------------------------------

调用stringtie,将单个样本分别拼接，产生各自的转录本注释文件*.gtf

