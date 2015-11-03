
工具说明
==========================

Path
-----------

**betadiversity.distancecalc**  # 工具目录设置，待定

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger/app/qiime  # qiime软件安装路径

使用程序
-----------------------------------

qiime/beta_diversity.py: http://www.qiime.org/

功能和用途描述
-----------------------------------

用于计算otu表中样本的距离矩阵

主要命令及功能模块
-----------------------------------

beta_diversity.py -i otu_table -m binary_euclidean -o beta_div

参数设计
-----------------------------------

::

            {"name": "method", "type": "string", "default": "bray_curtis"},  # 矩阵计算方法选择
            {"name": "input1", "type": "infile", "format": "OtuTable"},  # 输入文件,为otutable文本格式需要转换为biom格式
            {"name": "output", "type": "outfile", "format": "distancematrix"},  # 输出文件，为距离矩阵
            {"name": "input2", "type": "infile", "format": "newicktree"},  # 当选择UniFrac算法时，启用此参数


运行逻辑
-----------------------------------

提供一个otutable格式的文件，程序转换为biom格式的otu表，并指定距离计算方法（有默认距离算法），如果选用UniFrac算法时需要另外提供一个newicktree文件，计算得出'\t'符间隔的矩阵文本文件。





