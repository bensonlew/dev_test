
工具说明
==========================

Path
-----------

**tools.paternity_test**

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger-dev/app/bioinfo/medical/scripts/data_analysis.R 

功能和用途描述
-----------------------------------

组合后的家系表进行分析，主要是计算胎儿浓度，有效值，无效值，父权制等等，全部在这里里面计算了


使用程序
-----------------------------------

R脚本文件

主要命令及功能模块
-----------------------------------

Rscript data_analysis.R  tab_merged self.work_dir

参数设计
-----------------------------------


    {"name": "tab_merged", "type": "infile", "format": "paternity_test.rdata"}, #format:Rdata合并后的家系表
    {"name": "analysis_result", "type": "string"}


运行逻辑
-----------------------------------

