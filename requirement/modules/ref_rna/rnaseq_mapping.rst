
工具说明
==========================

Path
-----------

**rnaseq_mapping.rnaseq_mapping**

功能和用途描述
-----------------------------------

用于调用具体的差异性分析的tool

主要命令及功能模块
-----------------------------------
1.构建索引
    hisat2-build -f <参考基因组文件> <索引名称>
    bowtie2-build <参考基因组文件> <索引名称>

2.调用tophat2或hisat2进行比对，生成bam格式的结果文件

3.将用户传入的gff格式文件转换为gtf格式文件供下游使用
gffread <gff格式文件> -T -o <gtf格式文件>

4.使用bedops中的gff2bed将gff文件转换为bed供下游使用
perl gff2bed -d < <gff格式文件> > <bed格式文件>
参数设计
-----------------------------------

::
            {"name": "ref_genome", "type": "string"},  # 参考基因组，在页面上呈现为下拉菜单中的选项
            {"name":"ref_genome_custom", "type": "infile", "format": "sequence.fasta"},  # 自定义参考基因组，用户选择customer_mode时，需要传入参考基因组
            {"name": "mapping_method", "type": "string"},  # 测序手段，分为tophat测序和hisat测序
            {"name":"seq_method", "type": "string"},  # 双端测序还是单端测序
            {"name": "single_end_reads", "type": "infile", "format": "sequence.fastq"},  # 单端序列
            {"name": "left_reads", "type": "infile", "format":"sequence.fastq"},  # 双端测序时，左端序列
            {"name": "right_reads", "type": "infile", "format":"sequence.fastq"},  # 双端测序时，右端序列
            {"name": "gff","type": "infile", "format":"ref_rna.reads_mapping.gff"},  # gff格式文件
            {"name": "bam_output", "type": "outfile", "format": "align.bwa.bam"},  # 输出的bam
            {"name": "gtf", "type": "outfile", "format" : "ref_rna.reads_mapping.gtf"}  # 输出的gtf格式文件
            {"name": "bed", "type": "outfile", "format" : "ref_rna.reads_mapping.bed"}  # 输出的bed格式文件



运行逻辑
-----------------------------------
当"mapping_method"为“tophat”时，使用tophat进行比对
当“mapping_method”为“hisat”时，使用hisat进行比对
