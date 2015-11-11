
工具说明
==========================
由各个OTU代表序列生成相应树文件

Path
-----------

**tools.meta.otu**

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger/app

功能和用途描述
-----------------------------------
生成树文件

使用程序
-----------------------------------

clustalw2
FastTree

主要命令及功能模块
-----------------------------------

clustalw2 -ALIGN -INFILE=otu_reps.fasta -OUTFILE=phylo.clustalw.align  -OUTPUT=FASTA
FastTree -nt phylo.clustalw.align > phylo.tre

参数设计
-----------------------------------

::

    {"name": "otu_reps.fasta", "type": "infile", "format": "fasta"},  # 输入文件
    {"name": "phylo.tre", "type": "outfile", "format": "newick"}  # 输出结果


运行逻辑
-----------------------------------

传入OTU代表序列的fasta文件后运行命令生成树文件

