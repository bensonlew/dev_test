
工具说明
==========================

Path
-----------

**featureCounts**

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger-dev/app/bioinfo/align/

功能和用途描述
-----------------------------------

用于计算基因的表达量(count值)

使用程序
-----------------------------------

featureCounts：https://sourceforge.net/projects/subread/files/subread-1.5.0-p3/subread-1.5.0-p3-Linux-x86_64.tar.gz


主要命令及功能模块
-----------------------------------

featureCounts -T 15 -a ref_genome.gtf -g gene_id -p -M -s 0 -o output sample.bam


参数设计
-----------------------------------

::
            {"name": "fq_type", "type": "string","default": "PE"},  # PE OR SE
            {"name": "ref_gtf", "type": "infile", "format": "ref_rna.gtf"},  # 参考基因组的gtf文件
            {"name": "bam", "type": "infile", "format": "ref_rna.bam"},  # 样本比对后的bam文件
            {"name": "strand_specific", "type": "int", "default": 0},  # PE测序，是否链特异性, 默认是0, 无特异性
            {"name": "firststrand", "type": "int", "default": 1},  # 链特异性时选择正链, 默认不设置此参数
            {"name": "secondstrand", "type": "int", "default": 2},  # 链特异性时选择负链, 默认不设置此参数
            {"name": "feature_id", "type": "string", "default": "gene_id"},  # 默认计算基因的count值，可以选择exon，both等
            {"name": "cpu", "type": "int", "default": 10},  #设置CPU
            {"name": "max_memory", "type": "string", "default": "100G"}  #设置内存


运行逻辑
-----------------------------------

输入bam和参考基因组的gtf文件，计算基因的count值；

需要指出是否链特异性 ``strand_specific``，若为链特异性，需要给出是正链或负链 `1`, `2`：



