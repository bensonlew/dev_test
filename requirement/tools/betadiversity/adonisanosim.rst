
工具说明
==========================

Path
-----------

**betadiversity.adonisanosim**  # 工具目录设置，待定

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger/app/qiime  # qiime软件安装路径

使用程序
-----------------------------------

qiime/compare_categories.py: http://www.qiime.org/

功能和用途描述
-----------------------------------

进行AdonisAnosim分析

主要命令及功能模块
-----------------------------------

compare_categories.py [options] {--method METHOD -i/--input_dm INPUT_DM -m/--mapping_file MAPPING_FILE -c/--categories CATEGORIES -o/--output_dir OUTPUT_DIR}  

参数设计
-----------------------------------

::

            {"name": "method", "type": "string","default":"bray_curtis"},  # 默认距离算法，默认选择待定
            {"name": "input1", "type": "infile", "format": "OtuTable"},  # 输入文件,为otutable
            {"name": "input2", "type": "infile", "format": "distancematrix"},  # 输入文件，距离矩阵，选择提供距离矩阵时，覆盖otutable和距离算法。
            {"name": "input3", "type": "infile", "format": "GroupMaping"},  # 匹配文件，分组信息
            {"name": "field", "type": "string"},  # 选择一种分组方案
            {"name": "output", "type": "outfile", "format": "AdonisAnosimResult"},  # 分析结果表


运行逻辑
-----------------------------------

先通过距离算法和otutable获得距离矩阵，如果提供了距离矩阵，覆盖着两者，根据分组信息进行分析，输出到结果表。





