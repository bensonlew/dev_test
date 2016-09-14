Path
-----------

**tools.**

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger-dev/app/bioinfo/rna/RDDpred_v1.1/

功能和用途描述
--------------------------------
对bam文件进行RNA编辑位点检测

使用程序
-----------------------------------

RDDpred: http://epigenomics.snu.ac.kr/RDDpred/sources/RDDpred_v1.0.tar.gz


主要命令及功能模块
-----------------------------------
RDDpred.py  -rbl  -rsf  -tdp  -ops  -psl  -nsl 



参数设计
-----------------------------------
{"name": "rna_bam_dir", "type": "infile","format":"align.bwa.bam_dir"} #上传bam文件夹
{"name": "ref_hg19.fa", "type": "infile", "format": "sequence.fasta"}  #上传参考基因组

运行逻辑
-----------------------------------

传入满足要求的bam文件和参考基因组，进行编辑位点的分析