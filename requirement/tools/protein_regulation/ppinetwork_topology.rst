
工具说明
==========================

Path
-----------

**tools.protein_regulation**

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger-dev/app/bioinfo/rna/scripts/calc_ppi.py

功能和用途描述
-----------------------------------

使用networkx来对蛋白质互作网络进行拓扑属性分析


使用程序
-----------------------------------

Python脚本：calc_ppi.py

主要命令及功能模块
-----------------------------------

Python calc_ppi.py interaction.txt 700 ppi_network

参数设计
-----------------------------------



      {"name": "ppitable", "type": "string"},  #输入网络互作边的矩阵数据
      {"name": "cut", "type": "string", "default": "-1"} #选择combined_score > cut的边



运行逻辑
-----------------------------------

使用calc_ppi.py来计算网络的拓扑属性值，该脚本调用了networkx来计算，度，介数等
