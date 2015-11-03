
工具说明
==========================

Path
-----------

**betadiversity.hcluster**  # 工具目录设置,待定

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger/app/  # 目录设置待定

使用程序
-----------------------------------

plot-hcluster_tree.pl

功能和用途描述
-----------------------------------

基于距离矩阵获得样本树文件newick tree。

主要命令及功能模块
-----------------------------------

plot-hcluster_tree.pl  -i distancefile -m [average/single/complete] -o outputdir

参数设计
-----------------------------------

::

            {"name": "method", "type": "string", "default": "complete"},  # 选择构建层级聚类树的方法，默认选用complete
            {"name": "input", "type": "infile", "format": "distancematrix"},  # 输入文件,距离矩阵
            {"name": "output", "type": "outfile", "default": "newicktree"},  # 输出文件，newick tree文件


运行逻辑
-----------------------------------

调用脚本完成分析获得newicktree文件。



