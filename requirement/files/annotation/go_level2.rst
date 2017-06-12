格式说明
==========================

Path
-----------

**annotation.go.level2**

功能和用途描述
-----------------------------------

定义go注释的level2的结果文件

格式定义文档
-----------------------------------

表头：term_type term GO number percent sequence
每行六列，以"\t"分隔
第一列要在['biological_process', 'cellular_component', 'molecular_function']内

格式举例
-----------------------------------

::

biological_process      cell killing    GO:0001906      3       0.00018411      ENSONIT00000018657(GO:0045953);ENSONIT00000022822(GO:0052331);ENSONIT00000024584(GO:0042267)
biological_process      cellular process        GO:0009987      8698    0.53378337      ENSONIT00000014393(GO:0007186);ENSONIT00000014392(GO:0006412);ENSONIT00000014391(GO:0042981,GO:0097190)
cellular_component      synapse part    GO:0044456      138     0.00846886      ENSONIT00000004406(GO:0045211);ENSONIT00000004404(GO:0045211);ENSONIT00000004405(GO:0045211);ENSONIT00000004400(GO:0045211)

# 制表符"\t"做间隔

属性及其含义
-----------------------------------

''header''    表头
''count''     文件除去表头的行数

相关方法
-----------------------------------

``get_gene``       获取基因列表
