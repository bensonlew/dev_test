
工具说明
==========================

Path
-----------

**taxon.qiime_assign**

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger/app/meta/scripts/assign_taxonomy.py

功能和用途描述
-----------------------------------
对多序列fasta使用qiime基于RDP方法进行物种分类。


使用程序
-----------------------------------

http://drive5.com/usearch/features_search.html

主要命令及功能模块
-----------------------------------
assign_taxonomy.py  -m rdp -i seqs.fasta -c 0.7  -r ref.fasta -t ref.tax -o .  --rdp_max_memory 50000


参数设计
-----------------------------------

::

    {'name': 'fasta', 'type': 'infile', 'format': 'Fasta'},  # 输入fasta文件，序列名称格式为'>sampleID_seqID'.
    {'name': 'identity', 'type': 'float', 'default': 0.97},  # 相似性值，范围0-1.
    {'name': 'otu_table', 'type': 'outfile', 'format': 'OtuTable'},  # 输出结果otu表
    {'name': 'otu_rep', 'type': 'outfile', 'format': 'Fasta'},  # 输出结果otu代表序列
    {'name': 'otu_seqids', 'type': 'outfile', 'format': 'OtuSeqids'},  # 输出结果otu中包含序列列表
    {'name': 'otu_biom', 'type': 'outfile', 'format': 'Biom', 'default': 'otu_table.biom'}  # 输出结果biom格式otu表

运行逻辑
-----------------------------------
单个命令运行完成即可。
