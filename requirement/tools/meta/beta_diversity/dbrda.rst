
工具说明
==========================

Path
-----------

**betadiversity.dbrda**  # 工具目录设置，待定

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger/app/qiime  # qiime软件安装路径

使用程序
-----------------------------------

qiime/compare_categories.py: http://www.qiime.org/

功能和用途描述
-----------------------------------

进行db-RDA分析

主要命令及功能模块
-----------------------------------

compare_categories.py [options] {--method METHOD -i/--input_dm INPUT_DM -m/--mapping_file MAPPING_FILE -c/--categories CATEGORIES -o/--output_dir OUTPUT_DIR}  # 此处没有环境因子的导入，输出文件也没有表格

参数设计
-----------------------------------

::

            {"name": "method", "type": "string","default":"bray_curtis"},  # 默认距离算法，默认选择待定
            {"name": "input1", "type": "infile", "format": "OtuTable"},  # 输入文件,为otutable
            {"name": "input2", "type": "infile", "format": "distancematrix"},  # 输入文件，距离矩阵，选择提供距离矩阵时，覆盖otutable和距离算法。
            {"name": "input3", "type": "infile", "format": "EnvTable"},  # 输入文件，环境因子表。
            {"name": "output1", "type": "outfile", "format": "CoordinateTable"},  # 样本的坐标表
            {"name": "output2", "type": "outfile", "format": "CoordinateTable"},  # 环境因子坐标表


运行逻辑
-----------------------------------

先通过距离算法和otutable获得距离矩阵，如果提供了距离矩阵，覆盖着两者，根据距离矩阵和环境因子表进行计算db-RDA.





