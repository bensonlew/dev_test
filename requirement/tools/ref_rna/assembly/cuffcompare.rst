
工具说明
==========================

Path
-----------

**ref_rna.assembly.cuffcompare**

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger/app/bioinfo/rna/cufflinks-2.2.1/

功能和用途描述
-----------------------------------

调用cuffcompare，根据输出结果的class_code，调用assembly_stat.py和gtf_to_fasta.pl挑出新转录本，生成转录本gtf文件和fa文件

使用程序
-----------------------------------

cuffcompare：http://cole-trapnell-lab.github.io/cufflinks/cuffcompare/index.html

主要命令及功能模块
-----------------------------------

cuffcompare -s <参考基因组fa格式文件> -C -o <生成文件的前缀> -r <参考序列gtf格式文件> <样本转录本合并之后的gtf文件> 
python assembly_stat.py -combinedfile <输出的combined的gtf文件> -merged_file <样本转录本合并之后的gtf文件>  -o <新转录本的gtf文件>
perl gtf_to_fasta <新转录本的gtf文件> <参考序列gtf格式文件> <新转录本的fa文件>

参数设计
-----------------------------------

::

            {"name": "merged.gtf", "type": "infile","format":"ref_rna.gtf"},#拼接合并之后的转录本文件
            {"name": "ref_fa", "type": "infile", "format": "sequence.fasta"},  # 参考基因文件
            {"name": "ref_gtf", "type": "infile", "format": "ref_rna.gtf"},  # 参考基因的注释文件
            {"name": "new_transcripts.gtf", "type": "outfile", "format": "ref_rna.gtf"}, #新转录本的gtf文件
            {"name": "new_transcripts.fa", "type": "outfile", "format": "ref_rna.fasta"}, #新转录本的fa文件


运行逻辑
-----------------------------------

调用cuffcompare，根据输出结果的class_code，调用assembly_stat.py和gtf_to_fasta.pl挑出新转录本，生成转录本gtf文件和fa文件

