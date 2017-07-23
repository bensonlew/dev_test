工具说明
==========================

Path
-----------

**gene_fa**

程序安装路径
-----------------------------------
暂无

功能和用途描述
-----------------------------------

输入基因的gff3和ref_new.gtf(ref.gtf+new_transcript.gtf),参考基因组的fa文件
生成基因的fa文件和新基因的bed文件

使用程序
-----------------------------------
暂无

主要命令及功能模块
-----------------------------------
暂无

参数设计
-----------------------------------

::      {"name":"ref_gff3", "type":"string"},  #ref gff文件
        {"name":"new_gtf", "type":"string"},  #新基因的gtf文件
        {"name":"ref_genome_custom","type":"string"}, #ref fa文件
        {"name":"assembly_method","type":"string","default":"stringtie"}, #拼接方法
        {"name":"gene_fa","type":"outfile","format":"sequence.fasta"}, #结果文件 基因的fa文件
        {"name":"new_gene_bed","type":"outfile","format":"gene_structure.bed"} #新基因的bed文件

运行逻辑
------------------------------------

输入ref.gff3和新基因的gtf文件，返回基因的fa文件和新基因的bed文件
