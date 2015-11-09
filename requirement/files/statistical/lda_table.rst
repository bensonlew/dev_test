
格式说明
==========================

Path
-----------

**metastat.lefse**


功能和用途描述
-----------------------------------

用于存储lefse分析的LDA判别分析结果


格式定义文档
-----------------------------------


第一列为物种名字，第二列为丰富度的均值，第三列为LAD判别结果有意义的分组名，第四列为square值，第五列为小于0.05的P值


格式举例
-----------------------------------

::

Bacteria.Tenericutes.Mollicutes.Anaeroplasmatales.Anaeroplasmataceae.Anaeroplasma   1.29924258109           -
Bacteria.Firmicutes.Clostridia.Clostridiales.Lachnospiraceae.Lachnospiraceae_g_uncultured   4.77512436919           -
Bacteria.Firmicutes.Bacilli.Lactobacillales.Streptococcaceae    3.80605459008           -
Bacteria.Firmicutes.Clostridia.Clostridiales.Eubacteriaceae 1.22912958644           -
Bacteria.Firmicutes.Clostridia.Clostridiales.Clostridiales_f_XIII_Incertae_Sedis.Clostridiales_f_unclassified   2.63739570198           -
Bacteria.Actinobacteria.Actinobacteria.Micrococcales.Micrococcaceae 2.88725987215           -
Bacteria.Proteobacteria.Betaproteobacteria.Burkholderiales.Comamonadaceae.Comamonas 0.964794471672          -


属性及其含义
-----------------------------------

* ``organism_number``    文件包含的物种的数量
* ``organism_name``    物种名字


相关方法
-----------------------------------

``get_valid_result``  获取文件中LDA判别有效信息的行的内容，并返回该内容


