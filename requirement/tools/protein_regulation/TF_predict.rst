
工具说明
==========================

Path
-----------

**ref_rna.protein_regulation**

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger-dev/app/bioinfo/align/hmmer 3.1b2

功能和用途描述
----------------------------------

预测转录因子及其家族信息


使用程序
-----------------------------------

hmmer 3.1: http://hmmer.org/

主要命令及功能模块
-----------------------------------

hmmscan planttfdb.hmm amino.fas 

参数设计
-----------------------------------

::

     {"name": "amino", "type": "infile", "format": "fas"}, #从上游gtf文件得来的氨基酸文件
	 {"name": "diff_exp_amino", "type": "infile", "format": "txt"}, #氨基酸与差异基因的对应关系
     {"name": "planttfdb", "type": "reference", "format": "hmm"}, #参考数据的载入，是hmm模型的格式


运行逻辑
----------------------------------
输入待比对的文件，和参考序列的hmm模型。
数据库有AnimalTFDB、PlantTFDB和iTAK
