
模块说明
==========================

Path
-----------

**assemble.refrna_assemble**

功能和用途描述
-----------------------------------

对所有样本进行组装合并，并对合并后的结果进行信息统计和新转录本预测



主要命令及功能模块
-----------------------------------
package:mbio.packages.ref_rna.trans\_step

tool:cufflinks、cuffmerge、stringtie、stringtie_merge、gffcompare、new_stranscripts

参数设计
-----------------------------------

::

            {"name": "sample_bam_dir", "type": "infile", "format": "align.bwa.bam_dir"},  # 所有样本的bam文件夹
            {"name": "ref_fa", "type": "infile", "format": "sequence.fasta"},  # 参考基因文件
            {"name": "ref_gtf", "type": "infile", "format": "gene_structure.gtf"},  # 参考基因的注释文件
            {"name": "assembly_GTF_list.txt", "type": "infile", "format": "assembly.merge_txt"},  # 所有样本比对之后的bam文件路径列表
            {"name": "cpu", "type": "int", "default": 10},  # 软件所分配的cpu数量
            {"name": "fr_stranded", "type": "string", "default": "fr-unstranded"},  # 是否链特异性
            {"name": "strand_direct", "type": "string", "default": "none"},  # 链特异性时选择正负链
            {"name": "assemble_method", "type": "string", "default": "cufflinks"},  # 选择拼接软件
            {"name": "sample_gtf", "type": "outfile", "format": "gene_structure.gtf"},  # 输出的gtf文件
            {"name": "merged_gtf", "type": "outfile", "format": "gene_structure.gtf"},  # 输出的合并文件
            {"name": "cuff_gtf", "type": "outfile", "format": "gene_structure.gtf"},  # compare后的gtf文件
            {"name": "new_transcripts_gtf", "type": "outfile", "format": "gene_structure.gtf"},  # 新转录本注释文件
            {"name": "new_gene_gtf", "type": "outfile", "format": "gene_structure.gtf"},  # 新基因注释文件
            {"name": "merged_fa", "type": "outfile", "format": "sequence.fasta"},  # 新转录本注释文件
            


运行逻辑
-----------------------------------
1、对所有的样本单独运行stringtie/cufflinks进行拼接；

2、运行stringtie_merge/cuffmerge对拼接后的结果进行合并，产生merged.gtf文件；

3、用merged.gtf运行软件gffcompare进行比对；

4、根据class_code,运行new_transcripts挑选出新转录本和新基因的gtf文件和fa文件；

5、统计信息：class_code分布信息，序列长度分布信息

