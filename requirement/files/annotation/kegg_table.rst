格式说明
==========================

Path
-----------

**annotation.kegg.kegg_table**

功能和用途描述
-----------------------------------

定义kegg注释的kegg_table结果文件

格式定义文档
-----------------------------------

第一列为基因或转录本ID，第二列为KO ID，第三列为KO name，第四列为链接，第五列为pathways

格式举例
-----------------------------------

::

XLOC_020309     K12480  RABEP1  http://www.genome.jp/dbget-bin/www_bget?ko:K12480       path:ko04144
XLOC_020308     K06770  MPZ     http://www.genome.jp/dbget-bin/www_bget?ko:K06770       path:ko04514
XLOC_021256     K01899  LSC1    http://www.genome.jp/dbget-bin/www_bget?ko:K01899       path:ko01200;path:ko00640;path:ko00020
# 制表符"\t"做间隔

属性及其含义
-----------------------------------

''gene_list''    基因list

相关方法
-----------------------------------

''get_pathway_koid''       返回字典：ko_gene:ko id对应的geneids;字典：path_ko:pathway id为键，值为koid的列表
