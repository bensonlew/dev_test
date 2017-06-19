
工具说明
==========================

Path
-----------

**tools.rna.featureCounts**

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger-dev/app/bioinfo/align/

功能和用途描述
-----------------------------------

用于计算基因的表达量(count值)

使用程序
-----------------------------------

featureCounts：https://sourceforge.net/projects/subread/files/subread-1.5.0-p3/subread-1.5.0-p3-Linux-x86_64.tar.gz


主要命令及功能模块
-----------------------------------

featureCounts -T 15 -a ref_genome.gtf -g gene_id -p -M -s 0 -o output sample.bam


参数设计
-----------------------------------

::
          {"name": "fq_type", "type": "string","default": "PE"},  # PE OR SE             
          {"name": "ref_gtf", "type": "infile", "format": "gene_structure.gtf"},  # 参考基因组的gtf文件             
          {"name": "bam", "type": "infile", "format": "align.bwa.bam,align.bwa.bam_dir"},  # 样本比对后的bam文件或文件夹             
          {"name": "strand_specific", "type": "bool", "default": False},  # PE测序，是否链特异性, 默认是0, 无特异性             
          {"name": "strand_dir", "type": "string","default": "None"},  # "forward", "reverse" 默认不设置此参数             
          {"name": "count", "type":"outfile", "format": "rna.express_matrix"},  # featurecounts 输出结果总结             
          {"name": "fpkm", "type": "outfile", "format": "rna.express_matrix"}, #fpkm表达量表             
          {"name": "tpm", "type": "outfile", "format": "rna.express_matrix"}, #tpm表达量表             
          {"name": "out_file", "type": "outfile", "format": "rna.express_matrix"}, #featureCounts软件直接生成的表达量文件             
          {"name": "gtf", "type":"string"}, #该物种的参考基因组文件路径  gtf格式而非gff格式             
          {"name": "cpu", "type": "int", "default": 10},  #设置CPU            
          {"name": "is_duplicate", "type": "bool"}, # 是否有生物学重复            
          {"name": "edger_group", "type":"infile", "format":"sample.group_table"},             
          {"name": "max_memory", "type": "string", "default": "100G"},  #设置内存             
          {"name": "exp_way", "type": "string", "default": "fpkm"}, #fpkm水平表达量  fpkm tpm all             
          {"name": "all_gene_list", "type": "outfile", "format": "rna.express_matrix"}  #所有基因list列表，提供给下游差异分析module


运行逻辑
-----------------------------------

输入bam或者bam文件夹和参考基因组ref.gtf或拼接生成的merged.gtf文件，计算基因的count，fpkm或tpm值；
