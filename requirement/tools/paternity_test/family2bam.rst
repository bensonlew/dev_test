
工具说明
==========================

Path
-----------

**tools.paternity_test**

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger-dev/app/bioinfo/medical/scripts/fastq2bam.sh

功能和用途描述
-----------------------------------

针对杂交捕获样本（胎儿）实现一个样本从fastq文件转到bam中间文件


使用程序
-----------------------------------

sh脚本文件

主要命令及功能模块
-----------------------------------

fastq2bam.sh WQ12345678-F1 ref_fasta seq_path targets_bedfile cpu_number

参数设计
-----------------------------------


    {"name": "fastq", "type": "string"},  # 输入F/M/S的fastq文件的样本名,fastq_gz_dir/WQ235F
    {"name": "ref_fasta", "type": "infile", "format": "sequence.fasta"}, # hg38.chromosomal_assembly/ref.fa
    {"name": "targets_bedfile","type": "infile","format":"paternity_test.rda"},
    {"name": "seq_path", "type": "infile","format":"sequence.fastq_dir"}, # fastq所在路径
    {"name": "cpu_number", "type": "int", "default": 4}


运行逻辑
-----------------------------------

