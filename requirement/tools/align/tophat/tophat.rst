工具说明
==========================

Path
-----------

**tools.align.tophat.tophat**

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger-dev/app/bioinfo/align/tophat-2.1.1/tophat-2.1.1.Linux_x86_64

功能和用途描述
-----------------------------------

用户传入参考基因组建索引和reads比对参考基因组

使用程序
-----------------------------------

bowtie: http://bowtie-bio.sourceforge.net/bowtie2/manual.shtml#command-line-1
tophat: https://ccb.jhu.edu/software/tophat/manual.shtml#toph

主要命令及功能模块
-----------------------------------

bowtie2-build -f <参考基因组fa> ref
tophat2 ref reads_1.fastq reads_2.fastq(PE)
tophat2 ref reads.fastq(SE)

参数设计
-----------------------------------

::

            {"name": "ref_genome", "type": "string"},  # 参考基因组，在页面上呈现为下拉菜单中的选项
            {"name":"ref_genome_custom", "type": "infile", "format": "sequence.fasta"},  # 自定义参考基因组，用户选择customer_mode时，需要传入参考基因组
            {"name": "mapping_method", "type": "string"},  # 测序手段，这里的测序手段为tophat
            {"name":"seq_method", "type": "string"},  # 双端测序还是单端测序
            {"name": "single_end_reads", "type": "infile", "format": "sequence.fastq"},  # 单端read
            {"name": "left_reads", "type": "infile", "format":"sequence.fastq"},  # 双端测序时，左端read
            {"name": "right_reads", "type": "infile", "format":"sequence.fastq"}, # 单端测序时，右端read
            {"name": "bam_output", "type": "outfile", "format": "align.bwa.bam"}, # 生成的bam格式文件


运行逻辑
-----------------------------------

当参数"ref_genome"等于"customer_mode"时，需要客户传入参考基因组,用bowtie2进行建索引；
用tophat2进行reads比对，根据"seq_method",判断是单端测序还是双端测序
