
工具说明
==========================

Path
-----------

**meta.qc.reads_len_info**

程序安装路径
-----------------------------------

待定

功能和用途描述
-----------------------------------

用于统计一个fasta_dir下的所有文件的碱基长度分布信息

使用程序
-----------------------------------


主要命令及功能模块
-----------------------------------



参数设计
-----------------------------------

::
   {"name": "fasta_path", "type": "infile", "format": "fasta_dir"},  # 输入的fasta文件夹
   {"name": "sample_number", "type": "string"},  # 项目中包含的样本的数目，应当和输入文件夹中的fsta或者fastq文件的数目一致，用于检查是否有样本遗漏
   {"name": "reads_len_info", "type": "outfile", "format": "reads_len_info_dir"}]  # 输出的reads_len_info文件夹

运行逻辑
-----------------------------------

由外部提供fasta_dir, 生成相应的reads_length_info_dir文件夹，对文件夹里的每个fasta文件做统计，返回对应的reads_length_info文件,放在reads_length_info_dir文件夹当中
