
工具说明
==========================

Path
-----------

**meta.network.conet**

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger/app/meta/CoNet3

功能和用途描述
-----------------------------------

用于构建共相关性网络图

使用程序
-----------------------------------

CoNet.jar

主要命令及功能模块
-----------------------------------

java be.ac.vub.bsb.cooccurrence.cmd.CooccurrenceAnalyser --input myInputMatrix.txt --matrixtype incidence --method distrib --lowerT 0.5 --output coocNetworkHypergeom.gml --format gml


参数设计
-----------------------------------

::

            {"name": "customer_mode", "type": "bool", "default": False},  # customer 自定义数据库
            {"name": "query", "type": "infile", "format": "fasta"},  # 输入文件
            {"name": "database", "type": "string", "default": "nr"},  # 比对数据库 nt nr string GO swissprot uniprot KEGG
            {"name": "reference", "type": "infile", "format": "fasta"},  # 参考序列  选择customer时启用
            {"name": "evalue", "type": "float", "default": 1e-5},  # evalue值
            {"name": "num_threads", "type": "int", "default": 10},  # cpu数
            {"name": "output", "type": "outfile", "format": "blastxml"}  # 输出结果


运行逻辑
-----------------------------------

当参数 ``customer_mode``等于True时，需要给出 ``reference`` 参考序列文件做为数据库

根据 ``query`` 和 数据库类型，判断blast类型

