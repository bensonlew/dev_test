
工具说明
==========================

Path
-----------

**module.paternity_test**

功能和用途描述
-----------------------------------

可以进行亲子鉴定的分析，生成父权制，有效率，无效率等。
包含tool：family_merge、family_analysiss

主要命令及功能模块
-----------------------------------
做亲子鉴定的分析。
包含tool：family_merge、family_analysis

参数设计
-----------------------------------

            {"name": "dad_tab", "type": "infile", "format": "paternity_test.tab"},  # 输入F/M/S的样本ID
            {"name": "mom_tab", "type": "infile", "format": "paternity_test.tab"},  # fastq所在路径
            {"name": "preg_tab", "type": "infile", "format": "paternity_test.tab"},
            {"name": "ref_point", "type": "infile","format":"paternity_test.rda"},
            {"name": "err_min", "type": "int", "default": 2},

运行逻辑
-----------------------------------
1）先使用family_merge这个tool，将dad_tab， mom_tab， preg_tab进行合并，分型，注释
2）依赖于第一步后的家系结果，然后进行计算分析，生成父权值等