
工具说明
==========================

Path
-----------

**meta.qc.base_quality**

程序安装路径
-----------------------------------

待定

功能和用途描述
-----------------------------------

用于统计一个fastq_dir文件夹下所有fastq的文件的碱基质量

使用程序
-----------------------------------

fasttoolkit

主要命令及功能模块
-----------------------------------



参数设计
-----------------------------------

::

            {"name": "input_path", "type": "infile", "format": "fastq_dir"},  # 输入文件
            {"name": "output", "type": "outfile", "format": "base_info_dir"}  # 输出结果


运行逻辑
-----------------------------------

样本信息统计(sample_info)的目录结构如下

::
 ./
 ../
 fastq_dir/
 fasta_dir/
 qc/base_info_dir

由外部提供fastq_dir, 生成相应的qc/base_info_dir文件夹，对文件夹里的每个fastq文件做统计，返回对应的base_info文件,放在qc/base_info_dir文件夹当中
