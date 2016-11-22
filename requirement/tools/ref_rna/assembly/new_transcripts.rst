
工具说明
==========================

Path
-----------

**ref_rna.assembly.new_transcripts**

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger/app/program/Python/bin/

功能和用途描述
-----------------------------------

根据输出结果的class_code，调用assembly_stat.py和gtf_to_fasta.pl挑出新转录本，生成转录本gtf文件和fa文件


使用程序
-----------------------------------

https://www.python.org/

主要命令及功能模块
-----------------------------------

python assembly_stat.py -tmapfile <输出的tmap的gtf文件> -transcript_file <样本转录本合并之后的gtf文件>  -o <新转录本的gtf文件>
gtf_to_fasta <新转录本的gtf文件> <参考序列fa格式文件> <新转录本的fa文件>

参数设计
-----------------------------------

::

            {"name": "tmap", "type": "infile","format":"ref_rna.tmp"}, #compare后的tmap文件
            {"name": "ref_fa", "type": "infile", "format": "sequence.fasta"},  # 参考基因文件
            {"name": "merge_gtf", "type": "infile", "format": "ref_rna.gtf"},  # 拼接后的注释文件
            {"name": "new_gtf", "type": "outfile", "format": "ref_rna.gtf"}, #新转录本注释文件
            {"name": "new_fa", "type": "outfile", "format": "ref_rna.fasta"}  # 新转录本注释文件
            


运行逻辑
-----------------------------------

根据输出结果的class_code，调用assembly_stat.py和gtf_to_fasta.pl挑出新转录本，生成转录本gtf文件和fa文件

