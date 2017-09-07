
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

针队多重，实现一个样本从fastq文件转到tab文件


使用程序
-----------------------------------

sh脚本文件

主要命令及功能模块
-----------------------------------

dcpt_zml.sh WQ12345678-F1 cpu_number ref_fasta seq_path targets_bedfile picard_path java_path

参数设计
-----------------------------------


    {"name": "fastq", "type": "string"},  #输入F/M/S的fastq文件的样本名,fastq_gz_dir/WQ235F
    {"name": "ref_fasta", "type": "infile", "format": "sequence.fasta"}, #hg38.chromosomal_assembly/ref.fa
    {"name": "targets_bedfile","type": "infile","format":"paternity_test.rda"},
    {"name": "seq_path", "type": "infile","format":"sequence.fastq_dir"}, #fastq所在路径
    {"name": "cpu_number", "type": "int", "default": 4},
    {"name": "batch_id", "type": "string"}


运行逻辑
-----------------------------------
转为tab后，会对样本进行以下的判断：
1）检查样本的tab文件大小是否为0，为0则会把相关信息写到sg_pt_problem_sample
2）检查qc文件查看深度是不是大于5，小于5的样本会把相关信息写到sg_pt_problem_sample
3）去sg_pt_qc与sg_pt_ref两个数据库中检查该样本是否存在，存在的话就可能是重命名
4）如果该样本是父本则将父本的tab文件移动到查重的库中
