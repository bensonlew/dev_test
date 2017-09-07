
工具说明
==========================

Path
-----------

**module.paternity_test**

功能和用途描述
-----------------------------------

该模块实现亲子流程中fastq转tab的功能，其中包含了fastq2bam， bam2tab， fastq2tab。

主要命令及功能模块
-----------------------------------
功能模块：fastq2bam， bam2tab， fastq2tab

参数设计
-----------------------------------

        {"name": "sample_id", "type": "string"},  # 输入F/M/S的样本ID
        {"name": "fastq_path", "type": "infile","format":"sequence.fastq_dir"},  # fastq所在路径
        {"name": "cpu_number", "type": "int", "default": 4}, #cpu个数
        {"name": "ref_fasta", "type": "infile", "format": "sequence.fasta"},  # 参考序列
        {"name": "targets_bedfile", "type": "infile","format":"paternity_test.rda"},  # 位点信息
        {"name":"batch_id", "type": "string"},
        {"name":"type","type":"string","default":'pt'} #不同的实验流程，目前有pt和dcpt两种

运行逻辑
-----------------------------------
1）根据type(pt, ppt, dcpt)判断这个流程是运行那个脚本
2）pt或者ppt的时候，是执行杂交捕获的脚本，运行tool，fastq2bam， bam2tab，其中 bam2tab是依赖于fastq2bam这个结果的
3）dcpt的时候，是执行多重的脚本，运行tool fastq2tab。
