工具说明
==========================

Path
-----------

**tools.align.hisat.hisat**

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger-dev/app/bioinfo/align/hisat2/hisat2-2.0.0-beta
/mnt/ilustre/users/sanger-dev/app/bioinfo/align/samtools-1.3.1

功能和用途描述
-----------------------------------

用户传入参考基因组建索引和reads比对参考基因组

使用程序
-----------------------------------
hisat2-build
hisat2
samtools

主要命令及功能模块
-----------------------------------
1.建立索引
hisat2-build -f <参考基因组文件> <索引名称>

2.进行reads比对
——PE:
        hisat2 -q -x <索引名称> -1 reads1.fq -2 reads2.fq -S output.sam
——SE:
        hisat2 -q -x <索引名称> reads1.fq -S output.sam

3.调用samtools进行转换
samtools index output.sam
samtools view output.sam > accepted_hits.bam 

参数设计
-----------------------------------

::

            {"name": "ref_genome", "type": "string"},  # 参考基因组，在页面上呈现为下拉菜单中的选项
            {"name":"ref_genome_custom", "type": "infile", "format": "sequence.fasta"},  # 自定义参考基因组，用户选择customer_mode时，需要传入参考基因组
            {"name": "mapping_method", "type": "string"},  # 测序手段，这里的测序手段为hisat
            {"name":"seq_method", "type": "string"},  # 双端测序还是单端测序
            {"name": "single_end_reads", "type": "infile", "format": "sequence.fastq"},  # 单端read
            {"name": "left_reads", "type": "infile", "format":"sequence.fastq"},  # 双端测序时，左端read
            {"name": "right_reads", "type": "infile", "format":"sequence.fastq"}, # 单端测序时，右端read
            {"name": "bam_output", "type": "outfile", "format": "align.bwa.bam"}, # 生成的bam格式文件


运行逻辑
-----------------------------------

当参数"ref_genome"等于"customer_mode"时，需要客户传入参考基因组,用hisat2-build进行建索引；
用tophat2进行reads比对，根据"seq_method",判断是单端测序还是双端测序
将生成的sam格式文件排序转换为bam供下游调用