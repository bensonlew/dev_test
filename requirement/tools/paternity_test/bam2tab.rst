
工具说明
==========================

Path
-----------

**tools.paternity_test**

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger-dev/app/bioinfo/medical/scripts/bam2tab.sh

功能和用途描述
-----------------------------------

针对杂交捕获样本（胎儿），将bam中间文件转为tab文件


使用程序
-----------------------------------

sh脚本文件

主要命令及功能模块
-----------------------------------

bam2tab.sh WQ12345678-F1 bam_dir ref_fasta targets_bedfile 

参数设计
-----------------------------------


    {"name": "sample_id", "type": "string"}, #输入F/M/S的样本ID
    {"name": "bam_dir", "type": "string"},  #bam文件路径
    {"name": "ref_fasta", "type": "infile", "format": "sequence.fasta"},  # 参考序列
    {"name": "targets_bedfile", "type": "infile","format":"paternity_test.rda"}, #位点信息
    {"name": "batch_id", "type": "string"}


运行逻辑
-----------------------------------
转为tab后，会对样本进行以下的判断：
1）检查样本的tab文件大小是否为0，为0则会把相关信息写到sg_pt_problem_sample
2）检查qc文件查看深度是不是大于5，小于5的样本会把相关信息写到sg_pt_problem_sample
3）去sg_pt_qc与sg_pt_ref两个数据库中检查该样本是否存在，存在的话就可能是重命名
