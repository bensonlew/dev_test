
工具说明
==========================

Path
-----------

**betadiversity.distanceTtest**  # 工具目录设置，待定

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger/app/  # 调用邱萍的T_test.py脚本进行计算，目录待定

使用程序
-----------------------------------

T_test.py  # 程序没有完成，待定

功能和用途描述
-----------------------------------

用于计算距离矩阵中两个组的T检验

主要命令及功能模块
-----------------------------------

T_test.py -v vector  # 脚本没完成。待定。

参数设计
-----------------------------------

::

            {"name": "method", "type": "string", "default": "two.sided"},  # 选择T检验的方法，默认双尾("two.sided")
            {"name": "input1", "type": "infile", "format": "distancematrix"},  # 输入文件,距离矩阵
            {"name": "input2", "type": "infile", "format": "GroupMaping"},  # 输入文件,分组信息
            {"name": "output", "type": "outfile", "default": "TResult"},  # 输出文件，T检验结果文件


运行逻辑
-----------------------------------

程序获取矩阵信息和分组信息，调用邱萍写的T检验模块完成计算，获得检验结果。



