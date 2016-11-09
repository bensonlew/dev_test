
工具说明
==========================

Path
-----------

**ref_rna.gene_structure.rmats_bam**

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger-dev/app/bioinfo/rna/rMATS.3.2.5

功能和用途描述
-----------------------------------

输入每个样品的对应的bam文件和参考基因组注释gtf文件 进行样品间差异可变剪接事件类型鉴定 表达差异分析


使用程序
-----------------------------------

rMATS.3.2.5: http://rnaseq-mats.sourceforge.net/

主要命令及功能模块
-----------------------------------
python RNASeq-MATS.py -b1 A1.bam,A2.bam,A3.bam（A条件下的各重复的比对结果bam文件）  -b2 B1.bam,B2.bam,B3.bam（B条件下的各重复的比对结果bam文件）  -len bam文件中读长的长度   -gtf ref.gtf -o /root/dir/of/output  -t paired|single -novelSS 0|1 -c [0,1)  -analysis P|U 

注意：当-analysis 的值设为'P'（即paired analysis）时，A条件和B条件下的重复数应相等且各大于等于3 


参数设计
-----------------------------------

::
		{"name": "A_condition_bam_file_list_string", "type": "string"},
		{"name": "B_condition_bam_file_list_string", "type": "string"},
		{"name": "read_length", "type": "int"},
		{"name": "sequencing_library_type", "type": "string"},
		{"name": "ref_gtf_file", "type": "infile", "format": "ref_genome_anotation.gtf"},
		{"name": "output_root_dir", "type": "string"},
		{"name": "whether_to_find_novel_AS_sites", "type": "int", "default": 0}
		{"name": "analysis_mode", "type": "string", "default": "U"}
		{"name": "cutoff_splicing_difference", "type": "float", "default": 0.001}
		{"name": "constructing_sequencing_library_type", "type": "string", "default": "fr-unstranded"}
		

            
            


运行逻辑
-----------------------------------
调用rMATS,将各样本的bam文件与注释文件结合在一起分析，找出潜在的AS位点（3.2.5版本也可以通过reads分布发现新的AS位点和exon），并分析相应位置不同样品间的表达差异。形成五种AS类型（A3SS, A5SS, MXE, IR，SE）的注释和差异分析结果文件，如果设定-novelSS参数，则会产生新的剪接位点和外显子的注释文件


