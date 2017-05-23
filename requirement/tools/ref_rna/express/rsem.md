
工具说明
==========================

Path
-----------

**rsem**

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger-dev/app/bioinfo/rna/

功能和用途描述
-----------------------------------

有参和无参用于计算基因、转录本的表达量(count，fpkm或tpm值)

使用程序
-----------------------------------

rsem：
https://github.com/deweylab/RSEM/

主要命令及功能模块
-----------------------------------

有参：
*建索引*
`rsem-prepare-reference -p 8 --transcript-to-gene-map {gene2transcript} {transcript.fa} {fa.output} --bowtie2 --bowtie2-path {bowtie2路径}`
*计算表达量(双端测试)*
`rsem-calculate-expression --paired-end -p 8 {fq_l} {fq_r} {fa.output} {express.output} --bowtie2 --bowtie2-path {bowtie2路径}`
无参：
*建索引* `/mnt/ilustre/users/sanger-dev/app/bioinfo/rna/scripts/align_and_estimate_abundance.pl --transcripts {transctipt.fa} --seqType fq --single test.fq --est_method  RSEM --output_dir {结果文件夹} --trinity_mode --aln_method bowtie2 --prep_reference`
*计算表达量*
`/mnt/ilustre/users/sanger-dev/app/bioinfo/rna/scripts/align_and_estimate_abundance.pl  --transcripts {transcript.fa} --seqType fq --right {fq.r} --left {fq.l} --est_method  RSEM --output_dir {结果目录} --thread_count 6 --trinity_mode --aln_method bowtie2 --output_prefix {样本名称}`

参数设计
-----------------------------------

::          {"name": "fq_type", "type": "string"}, #PE OR SE
            {"name": "ref_denovo","type":"string", "default": "ref"}, #ref or denovo，判断是有参还是无参
            {"name": "transcript_fa", "type": "infile", "format": "sequence.fasta"},  # 转录本fasta文件
            {"name": "fa_build", "type": "outfile", "format": "sequence.fasta"},  # 转录本fasta构建好索引文件
            {"name": "fq_l", "type": "infile", "format": "sequence.fastq"},  # PE测序，包含所有样本的左端fq文件的文件夹  不压缩的fq文件
            {"name": "fq_r", "type": "infile", "format": "sequence.fastq"},  # PE测序，包含所有样本的左端fq文件的文件夹
            {"name": "fq_s", "type": "infile", "format": "sequence.fastq"},  # SE测序，包含所有样本的fq文件的文件夹
            {"name": "ref_gtf", "type": "infile", "format": "ref_rna.reads_mapping.gtf, ref_rna.reads_mapping.gff" }, # gtf/gff文件
            {"name": "sample_name", "type":"string"}, # 样本名称
            {"name": "cpu", "type": "int", "default": 8},  # 设置CPU
            {"name": "max_memory", "type": "string", "default": "100G"}, # 设置内存
            {"name": "only_bowtie_build", "type": "bool", "default": False},  # 为true时该tool只建索引
            {"name": "bowtie_build_rsem", "type": "bool", "default": False},  # 为true时该tool需要建索引   

运行逻辑
-----------------------------------

*有参*
输入transcript.fa和基因与转录本对应关系文件(共两列，第一列是基因名称，第二列是转录本名称), 建立索引;
根据建好的索引, 输入fq文件计算表达量;

*无参*
输入transcript.fa文件建立索引;
根据索引,输入fq文件计算表达量;
