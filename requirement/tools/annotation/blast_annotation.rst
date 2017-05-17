工具说明
==========================

Path
-----------

**annotation.blast_annotation**

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger-dev/app/program/Python/bin/python

功能和用途描述
-----------------------------------

对blast到数据库的table文件进行evalue、score、similarity、identity参数的筛选及E-value分布和相似度分布的统计

使用程序
-----------------------------------

/mnt/ilustre/users/sanger-dev/biocluster/src/mbio/packages/align/blast/blastout_statistics.py

主要命令及功能模块
-----------------------------------

run_blast_filter()
from mbio.packages.align.blast.blastout_statistics import *
blastout_statistics(blast_table, evalue_path, similarity_path)

参数设计
-----------------------------------

::

      {"name": "blastout_table", "type": "infile", "format": "align.blast.blast_table"},  # 输入文件，blast到数据库的table文件
      {"name": "evalue", "type": "float", "default": 10e-5},  # evalue值
      {"name": "score", "type": "float", "default": 0},  # score值
      {"name": "similarity", "type": "float", "default": 0},  # similarity值
      {"name": "identity", "type": "float", "default": 0},  # identity值


运行逻辑
-----------------------------------

输入注释到数据库的table文件，进行evalue、score、similarity、identity参数的筛选，筛选后用blastout_statistics.py进行E-value分布和相似度分布统计
