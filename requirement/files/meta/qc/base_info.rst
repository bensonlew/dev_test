
工具说明
==========================

Path
-----------

**meta.qc.base_info**

程序安装路径
-----------------------------------

待定

功能和用途描述
-----------------------------------

用于统计一个fastq_dir文件夹下所有fastq的文件的碱基质量

使用程序
-----------------------------------

fastx_quality_status

主要命令及功能模块
-----------------------------------



参数设计
-----------------------------------

::
  {"name": "fastq_path", "type": "infile", "format": "fastq_dir"},  # 输入文件夹
  {"name": "sample_number", "type": "string"},  # 项目中包含的样本的数目，应当和输入文件夹中的fastq文件的数目一致，用于检查是否有样本遗漏
  {"name": "base_info_path", "type": "outfile", "format": "base_info_dir"}]  # 输出的base_info文件夹
运行逻辑
-----------------------------------

由外部提供fastq_dir, 生成相应的qc/base_info_dir文件夹，调用fastx_quality_status对文件夹里的每个fastq文件做统计，返回对应的base_info文件夹,
