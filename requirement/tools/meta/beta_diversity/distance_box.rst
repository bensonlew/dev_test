
工具说明
==========================

Path
-----------

**betadiversity.distancebox**  # 工具目录设置，待定

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger/app/qiime  # qiime软件安装路径

使用程序
-----------------------------------

qiime/make_distance_boxplots.py: http://www.qiime.org/

功能和用途描述
-----------------------------------

进行绘制distanbox

主要命令及功能模块
-----------------------------------

make_distance_boxplots.py [options] {-m/--mapping_fp MAPPING_FP -o/--output_dir OUTPUT_DIR -d/--distance_matrix_fp DISTANCE_MATRIX_FP -f/--fields FIELDS --save_raw_data}

参数设计
-----------------------------------

::

            {"name": "method", "type": "string","default":"bray_curtis"},  # 默认距离算法，默认选择待定
            {"name": "input1", "type": "infile", "format": "OtuTable"},  # 输入文件,为otutable
            {"name": "input2", "type": "infile", "format": "distancematrix"},  # 输入文件，距离矩阵，选择提供距离矩阵时，覆盖otutable和距离算法。
            {"name": "input3", "type": "infile", "format": "GroupMaping"},  # 匹配文件，分组信息
            {"name": "field", "type": "string"},  # 选择一种分组方案
            {"name": "output", "type": "outfile", "format": "boxplot"},  # 原始绘图数据


运行逻辑
-----------------------------------

先通过距离算法和otutable获得距离矩阵，如果提供了距离矩阵，覆盖着两者，根据分组信息进行分析，输出得到原始的绘图数据文件boxplot。





