
工具说明
==========================

Path
-----------

**assemble.gffcompare**

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger-dev/app/bioinfo/rna/gffcompare-0.9.8.Linux_x86_64

功能和用途描述
-----------------------------------

调用gffcompare，比较参考注释文件和merge后的注释文件

使用程序
-----------------------------------

gffcompare：https://github.com/gpertea/gffcompare

主要命令及功能模块
-----------------------------------

gffcompare  <样本转录本合并之后的gtf文件>  -o <生成文件的前缀> -r <参考序列gtf格式文件> 


参数设计
-----------------------------------

::

            {"name": "merged_gtf", "type": "infile", "format": "gene_structure.gtf"},  # 拼接合并之后的转录本文件
            {"name": "ref_gtf", "type": "infile", "format": "gene_structure.gtf"},  # 参考基因的注释文件
            {"name": "cuff_gtf", "type": "outfile", "format": "gene_structure.gtf"},


运行逻辑
-----------------------------------

调用gffcompare，比较参考注释文件和merge后的注释文件

