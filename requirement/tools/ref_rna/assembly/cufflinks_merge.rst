
工具说明
==========================

Path
-----------

**ref_rna.assembly.cufflinks_merge**

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger/app/bioinfo/rna/cufflinks-2.2.1/

功能和用途描述
-----------------------------------

单个样本拼接，产生各自的转录本注释文件


使用程序
-----------------------------------

cuffmerge：http://cole-trapnell-lab.github.io/cufflinks/cuffmerge/index.html

主要命令及功能模块
-----------------------------------

find -name _./output/*_.gtf > assembly_GTF_list.txt
cufflinks -p <cpu线程数> -g <参考序列gtf格式文件> -s <参考基因组fa格式文件> -o merge_output assembly_GTF_list.txt

参数设计
-----------------------------------

::

            {{"name": "assembly_GTF_list.txt", "type": "infile", "format": "ref_rna.txt"},  # 所有样本的转录本存放的路径
            {"name": "ref_fa", "type": "infile", "format": "sequence.fa"},  # 参考基因文件
            {"name": "ref_gtf", "type": "infile", "format": "ref_rna.gtf"},  # 参考基因的注释文件
            {"name": "cpu", "type": "int", "default": 10},  # stringtie软件所分配的cpu数量
            {"name": "sample_gtf", "type": "outfile","format":"ref_rna.gtf"},  # 输出的gtf文件
            


运行逻辑
-----------------------------------

新建文本列出转录本路径，调用cuffmerge,合并转录本信息

