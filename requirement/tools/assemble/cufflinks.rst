
工具说明
==========================

Path
-----------

**assemble.cufflinks**

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger/app/bioinfo/rna/cufflinks-2.2.1/

功能和用途描述
-----------------------------------

单个样本拼接，产生各自的转录本注释文件


使用程序
-----------------------------------

cufflinks：http://cole-trapnell-lab.github.io/cufflinks/cufflinks/index.html

主要命令及功能模块
-----------------------------------

cufflinks -p <cpu线程数>-g <参考序列gtf格式文件> -b <参考基因组fa格式文件> -m 51 --library-type <fr-unstranded/firststrand/secondstrand> -o sample_output <样本bam格式的文件>

gffread <样本gtf文件> -g <参考基因组序列文件.fa> -w <样本序列文件.fa>

参数设计
-----------------------------------

::

            {"name": "sample_bam", "type": "infile", "format": "align.bwa.bam"},  # 所有样本比对之后的bam文件
            {"name": "ref_fa", "type": "infile", "format": "sequence.fasta"},  # 参考基因文件
            {"name": "ref_gtf", "type": "infile", "format": "gene_structure.gtf"},  # 参考基因的注释文件
            {"name": "cpu", "type": "int", "default": 10},  #cufflinks软件所分配的cpu数量
            {"name": "F", "type": "int", "default": 0.1},  # min-isoform-fraction
            {"name": "fr_stranded", "type": "string", "default": "fr-unstranded"},  # 是否链特异性
            {"name": "strand_direct", "type": "string", "default": "none"},  # 链特异性时选择正负链
            {"name": "sample_gtf", "type": "outfile", "format": "gene_structure.gtf"},  # 输出的转录本文件
            


运行逻辑
-----------------------------------

1、调用cufflinks,将单个样本分别拼接，产生各自的转录本注释文件transcript.gtf

2、使用gffread软件，得到样本序列文件*.fa
