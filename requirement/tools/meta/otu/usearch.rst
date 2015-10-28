
工具说明
==========================

Path
-----------

**meta.otu.usearch**

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger/app/meta/usearch.v7.0

功能和用途描述
-----------------------------------



使用程序
-----------------------------------

http://drive5.com/usearch/features_search.html

主要命令及功能模块
-----------------------------------




参数设计
-----------------------------------

::

{'name': 'fasta', 'type': 'infile', 'format': 'Fasta', 'require': True, 'default': 'meta.fasta'},
{'name': 'id', 'type': 'float', 'require': False, 'default': 0.97},
{'name': 'sort_size', 'type': 'int', 'require': False, 'default': 2},
{'name': 'otu_table', 'type': 'outfile', 'format': 'OtuTable', 'default': 'otu_table.xls'},
{'name': 'otu_rep', 'type': 'outfile', 'format': 'Fasta', 'default': 'otu_rep.fasta'},
{'name': 'otu_seqids', 'type': 'outfile', 'format': 'OtuSeqids', 'default': 'otu_seqids.txt'},
{'name': 'otu_biom', 'type': 'outfile', 'format': 'Biom', 'default': 'otu_table.biom'}

运行逻辑
-----------------------------------

