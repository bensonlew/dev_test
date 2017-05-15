
工具说明
==========================

Path
-----------

**assemble.stringtie**

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger-dev/app/bioinfo/rna/stringtie-1.2.4

功能和用途描述
-----------------------------------

单个样本拼接，产生各自的转录本注释文件和对应的序列文件


使用程序
-----------------------------------

StringTie：http://ccb.jhu.edu/software/stringtie/

主要命令及功能模块
-----------------------------------

stringtie <样本bam格式的文件> -p cpu线程数 -G <参考序列gtf格式文件> -s <参考基因组fa格式文件> -o sample_output（文件夹名称）
 
gffread <样本gtf文件> -g <参考基因组序列文件.fa> -w <样本序列文件.fa>

参数设计
-----------------------------------

::

            {"name": "sample_bam", "type": "infile", "format": "align.bwa.bam"},  # 所有样本比对之后的bam文件
            {"name": "ref_fa", "type": "infile", "format": "sequence.fasta"},  # 参考基因文件
            {"name": "ref_gtf", "type": "infile", "format": "gene_structure.gtf"},  # 参考基因的注释文件
            {"name": "cpu", "type": "int", "default": 10},  # stringtie软件所分配的cpu数量
            {"name": "sample_gtf", "type": "outfile", "format": "gene_structure.gtf"}  # 输出的gtf文件
            


运行逻辑
-----------------------------------

1、调用stringtie,将单个样本分别拼接，产生各自的转录本注释文件*.gtf

2、使用gffread软件，得到样本序列文件*.fa
