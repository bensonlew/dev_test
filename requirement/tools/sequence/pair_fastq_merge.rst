工具说明
==========================

Path
-----------

**sequence.pair_fastq_merge**

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger/app/pear/bin

功能和用途描述
-----------------------------------

调用pear将成对fastq文件根据overlap将read1和read2连接起来

使用程序
-----------------------------------

pear

主要命令及功能模块
-----------------------------------

"pear -f %s -r %s -o merge" % (self.option('fastq_input1').prop["path"],self.option('fastq_input2').prop["path"])

参数设计
-----------------------------------

::

            {"name": "fastq_input1", "type": "infile", "format": "sequence.fastq"}, #输入文件fastq1
            {"name": "fastq_input2", "type": "infile", "format": "sequence.fastq"}, #输入文件fastq2

运行逻辑
-----------------------------------
当传入参数fastq_input1、fastq_input2时，就可以运行此模块