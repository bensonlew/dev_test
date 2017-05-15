
工具说明
==========================

Path
-----------

**assemble.stringtie_merge**

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

find -name \_./output/*\_.gtf > assembly_GTF_list.txt
stringtie --merge assembly_GTF_list.txt  -G <参考序列注释文件> -s <参考序列文件> -p <CPU线程> -o merge_out（文件夹）

 
gffread <样本gtf文件> -g <参考基因组序列文件.fa> -w <样本序列文件.fa>

参数设计
-----------------------------------

::

            {"name": "assembly_GTF_list.txt", "type": "infile", "format": "assembly.merge_txt"},  # 所有样本比对之后的bam文件路径列表
            {"name": "ref_fa", "type": "infile", "format": "sequence.fasta"},  # 参考基因文件
            {"name": "ref_gtf", "type": "infile", "format": "gene_structure.gtf"},  # 参考基因的注释文件
            {"name": "cpu", "type": "int", "default": 10},  # stringtie软件所分配的cpu数
            {"name": "merged_gtf", "type": "outfile", "format": "gene_structure.gtf"},  # 输出的合并文件
            


运行逻辑
-----------------------------------

1、新建文本列出转录本的绝对路径；

2、调用stringtie --merge,合并转录本信息，生成merged.gtf

3、使用gffread软件，得到merged.fa文件

