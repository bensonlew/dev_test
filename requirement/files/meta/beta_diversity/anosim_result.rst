
格式说明
==========================

Path
-----------

**betadiversity.adonisanosimresult**  # 目录设置待定

功能和用途描述
-----------------------------------

定义AdonisAnosimResult文档，存储AdonisAnosim的结果。

格式定义文档
-----------------------------------

自定义结果格式，存储两个结果有R statistic,p-value,permutations.

格式举例
-----------------------------------

::
    #method     R statistic     p-value     permutations
    Adonis      0.95            0.004       999
    Anosim      1               0.0034      999

# 制表符"\t"做间隔

属性及其含义
-----------------------------------

*``adonis_Rstatistic``         adonisR值
*``adonis_Pvalue``             adonisP值
*``adonis_permu``              adonis随机次数
*``anosim_Rstatistic``         anosimR值
*``anosim_Pvalue``             anosimP值
*``anosim_permu``              anosim随机次数

相关方法
-----------------------------------

``similarity``    显示相似性是否显著
``check``         继承重写file的check，检查格式中是否有元素缺失，格式错误