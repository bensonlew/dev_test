工具说明
==========================

Path
-----------

**ref_rna.mapping.refrna_quality_assessment**

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger-dev/app/program/Python/bin

功能和用途描述
-----------------------------------

用Python包对转录组进行质量评估：reads区域分布、测序饱和度、基因覆盖度、冗余序列等分析

使用程序
-----------------------------------

RSeQC:  http://rseqc.sourceforge.net

主要命令及功能模块
-----------------------------------

python read_distribution.py -i accepted_hits.bam -r refseq.gtf.bed > output
python RPKM_saturation.py -r refseq.gtf.bed -i accepted_hits.bam -o output -q <quality>
python read_duplication.py -i accepted_hits.bam -o output -q <quality>
python geneBody_coverage.py -r refseq.gtf.bed -i accepted_hits.bam -o output 

参数设计
-----------------------------------

::

            {"name": "bed", "type": "infile", "format": ref_rna.bed},  # bed格式文件,参考基因组结构注释文件
            {"name": "bam", "type": "infile", "format": ref_rna.bam},  # bam格式文件，mapping后的accepted_hits.bam文件
            {"name": "quality", "type": "int", "default": 30},         # 质量值


运行逻辑
-----------------------------------

调用Python的RSeQC包，运行脚本，进行质量评估
