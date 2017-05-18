
工具说明
==========================

Path
-----------

**assemble.new_transcripts**

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger/app/program/Python/bin/

功能和用途描述
-----------------------------------

根据输出结果的class_code，调用assembly\_stat.py挑出新转录本，生成新转录本gtf文件和新基因的gtf文件


使用程序
-----------------------------------

https://www.python.org/

主要命令及功能模块
-----------------------------------

python assembly\_stat.py 
-tmapfile <输出的tmap的gtf文件> 

-transcript\_file <样本转录本合并之后的gtf文件>  

-out\_new\_trans <新转录本gtf文件new\_transcripts.gtf> 

-out\_new\_genes <新基因对应的gtf文件new\_genes.gtf> 

-out\_old\_trans <已知转录本gtf文件old\_trans.gtf> 

-out\_old\_genes <已知基因gtf文件old\_genes.gtf>


gffread <样本gtf文件> -g <参考基因组序列文件.fa> -w <样本序列文件.fa>

参数设计
-----------------------------------

::

            {"name": "tmap", "type": "infile", "format": "assembly.tmap"},  # compare后的tmap文件
            {"name": "ref_fa", "type": "infile", "format": "sequence.fasta"},  # 参考基因文件
            {"name": "merged_gtf", "type": "infile", "format": "gene_structure.gtf"},  # 拼接后的注释文件
            {"name": "new_trans_gtf", "type": "outfile", "format": "gene_structure.gtf"},  # 新转录本注释文件
            {"name": "new_genes_gtf", "type": "outfile", "format": "gene_structure.gtf"},  # 新基因gtf文件
            {"name": "new_trans_fa", "type": "outfile", "format": "sequence.fasta"},  # 新转录本注释文件
            {"name": "new_genes_fa", "type": "outfile", "format": "sequence.fasta"}  # 新基因注释文件
            


运行逻辑
-----------------------------------

1、根据输出结果的classcode，调用assembly\_stat.py新转录本和新基因的gtf文件；

2、使用gffread软件，生成新转录本和新基因的fa文件。

