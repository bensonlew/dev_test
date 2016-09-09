
工具说明
==========================

Path
-----------

**ref_rna.mapping.transcript_abstract**

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger/app/bioinfo/rna/cufflinks-2.2.1/

功能和用途描述
-----------------------------------

提取参考基因组中的转录组序列,并选择其中最长的基因作为代表序列

使用程序
-----------------------------------

gffread：http://manpages.ubuntu.com/manpages/trusty/man1/gffread.1.html

主要命令及功能模块
-----------------------------------

gffread <gff格式文件> -g <参考基因组fa格式文件> -w transcript.fa


参数设计
-----------------------------------

::

            {"name": "gff_file", "type": "infile", "format":"gff"},
            {"name": "ref_genome", "type": "infile", "format": "fasta"},
            {"name": "transcript", "type": "infile", "format": "fasta"},
            


运行逻辑
-----------------------------------

调用gffread输出fa格式文件，从其中提取出最长序列

