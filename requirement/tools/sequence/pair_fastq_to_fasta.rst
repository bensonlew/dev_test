工具说明
==========================

Path
-----------

**sequence.pair_fastq_to_fasta**

功能和用途描述
-----------------------------------

将成对fastq文件转换成fasta文件

主要命令及功能模块
-----------------------------------




参数设计
-----------------------------------

::

            {"name": "fastq_input1", "type": "infile", "format": "sequence.fastq"}, #输入文件fastq1
            {"name": "fastq_input2", "type": "infile", "format": "sequence.fastq"}, #输入文件fastq2
            {"name": "fq1_to_fasta_id", "type": "string", "default": "none"},  #自定义fastq1序列id，默认将保留fastq文件id，将'@'改成'>'
            {"name": "fq2_to_fasta_id", "type": "string", "default": "none"}   #自定义fastq1序列id，默认将保留fastq文件id，将'@'改成'>'

运行逻辑
-----------------------------------
当传入参数fastq_input1、fastq_input2时，就可以运行此模块,fq1_to_fasta_id、fq2_to_fasta_id为可选参数