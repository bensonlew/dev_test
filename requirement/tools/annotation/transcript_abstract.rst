工具说明
==========================

Path
-----------

**annotation.transcript_abstrict**

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger-dev/app/program/Python/bin/python
/mnt/ilustre/users/sanger-dev/app/bioinfo/rna/cufflinks-2.2.1/

功能和用途描述
-----------------------------------

用gffread提取exons,再对exons提取最长序列，得到最长序列的列表

使用程序
-----------------------------------

/mnt/ilustre/users/sanger-dev/app/bioinfo/rna/scripts/annotation_longest.py

主要命令及功能模块
-----------------------------------

gffread gtf -g fasta -w exons.fa
python annotation_longest.py -i exons.fa
get_gene_list()

参数设计
-----------------------------------

::

      {"name": "ref_genome_custom", "type": "infile", "format": "sequence.fasta"},  # 参考基因组fasta文件
      {"name": "ref_genome_gtf", "type": "infile", "format": "gene_structure.gtf"},  # 参考基因组gtf文件
      {"name": "ref_genome_gff", "type": "infile", "format": "gene_structure.gff3"},  # 参考基因组gff文件
      {"name": "query", "type": "outfile", "format": "sequence.fasta"},  # 输出做注释的转录本序列
      {"name": "gene_file", "type": "outfile", "format": "rna.gene_list"}  # 输出最长转录本
  ]


运行逻辑
-----------------------------------

输入fasta文件ref_genome_custom及对应的gtf文件ref_genome_gtf或gff文件ref_genome_gff，提取exons及最长序列和最长序列的列表
