
工具说明
==========================

Path
-----------

**meta.qc.info**

程序安装路径
-----------------------------------

N/A

功能和用途描述
-----------------------------------

用于统计一个fastq_dir/或者fasta_dir的样本信息

使用程序
-----------------------------------

N/A

主要命令及功能模块
-----------------------------------



参数设计
-----------------------------------

::
  {"name": "fasta_path", "type": "infile", "format": "fasta_dir"}, # 输入文件，与fastq_path二选一
  {"name": "fastq_path", "type": "infile", "format": "fastq_dir"},  # 输入文件，与fasta_path二选一
  {"name": "sample_number", "type": "string"},  # 项目中包含的样本的数目，应当和输入文件夹中的fsta或者fastq文件的数目一致，用于检查是否有样本遗漏
  {"name": "out_fasta_path", "type": "outfile", "format": "fasta_dir"},  # 输出的fasta_dir文件夹,供后续分析
  {"name": "samples_info", "type": "outfile", "format": "samples_info"}  # 输出的samples_info文件

运行逻辑
-----------------------------------

当数据拆分完成之后自动生成 fastq_dir/ 文件夹,此工具模块中，由外界输入一个路径,由模块检测路径的类型
当检测到类型为fastq_dir时，将他转化为fasta_dir并生成相应的文件夹，检测到是fasta_dir时不做转化
然后根据fasta_dir的fasta文件,生成sample_info格式的统计文件。

