
工具说明
==========================

Path
-----------

**ref_rna.assembly.stringtie**

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger-dev/app/bioinfo/rna/stringtie-1.2.4

功能和用途描述
-----------------------------------

新建文本列出转录本路径，合并转录本信息


使用程序
-----------------------------------

StringTie：http://ccb.jhu.edu/software/stringtie/

主要命令及功能模块
-----------------------------------

finf -name _./output/*_.gtf > assembly_GTF_list.txt
stringtie --merge assembly_GTF_list.txt  -G <参考序列注释文件> -s <参考序列文件> -p <CPU线程> -o merge_out

参数设计
-----------------------------------

::

            {"name": "assembly_GTF_list.txt", "type": "infile", "format": "ref_rna.txt"},  # 所有样本的转录本存放的路径
            {"name": "ref_fa", "type": "infile", "format": "sequence.fa"},  # 参考基因文件
            {"name": "ref_gtf", "type": "infile", "format": "ref_rna.gtf"},  # 参考基因的注释文件
            {"name": "cpu", "type": "int", "default": 10},  # stringtie软件所分配的cpu数量
            {"name": "sample_gtf", "type": "outfile","format":"ref_rna.gtf"},  # 输出的gtf文件
            


运行逻辑
-----------------------------------

新建文本列出转录本路径，调用stringtie --merge,合并转录本信息

