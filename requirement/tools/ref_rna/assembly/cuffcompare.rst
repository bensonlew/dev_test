
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

调用cuffcompare，比较参考注释文件和merge后的注释文件

使用程序
-----------------------------------

cuffcompare：http://cole-trapnell-lab.github.io/cufflinks/cuffcompare/index.html

主要命令及功能模块
-----------------------------------

cuffcompare -s <参考基因组fa格式文件> -C -o <生成文件的前缀> -r <参考序列gtf格式文件> <样本转录本合并之后的gtf文件> 


参数设计
-----------------------------------

::

            {"name": "merged.gtf", "type": "infile","format":"ref_rna.gtf"},#拼接合并之后的转录本文件
            {"name": "ref_fa", "type": "infile", "format": "sequence.fasta"},  # 参考基因文件
            {"name": "ref_gtf", "type": "infile", "format": "ref_rna.gtf"},  # 参考基因的注释文件
            {"name": "tmap", "type": "outfile", "format": "ref_rna.tmp"},  # compare后的tmap文件
            {"name": "refmap", "type": "outfile", "format": "ref_rna.tmp"},  # compare后的refmap文件
            {"name": "combined.gtf", "type": "outfile", "format": "ref_rna.gtf"},  # compare后的combined.gtf文件
            {"name": "loci", "type": "outfile", "format": "ref_rna.loci"},  # compare后的loci文件
            {"name": "stats", "type": "outfile", "format": "ref_rna.stats"},  # compare后的stats文件
            {"name": "tracking", "type": "outfile", "format": "ref_rna.tracking"},  # compare后的tracking文件


运行逻辑
-----------------------------------

调用cuffcompare，比较参考注释文件和merge后的注释文件

