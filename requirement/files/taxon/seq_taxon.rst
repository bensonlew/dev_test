
格式说明
==========================

Path
-----------

**taxon.seq_taxon**


功能和用途描述
-----------------------------------

用于描述序列的分类学信息


格式定义文档
-----------------------------------

样例见格式举例
列之间以制表符分割
第一列为序列名, 第二列为分类信息,分类等级以双下划线‘__’与分类名隔开,各个分类等级之间以‘;’隔开，第三列为分类比对的confidence值，第3列可以选择性有无。


格式举例
-----------------------------------

::
	OTU1    d__Bacteria;p__Bacteroidetes;c__Bacteroidia;o__Bacteroidales;f__Bacteroidaceae;g__Bacteroides;s__Bacteroides_plebeius   1.000
OTU6    d__Bacteria;p__Firmicutes;c__Clostridia;o__Clostridiales;f__Lachnospiraceae;g__Roseburia;s__uncultured_organism 0.850
OTU2    d__Bacteria;p__Bacteroidetes;c__Bacteroidia;o__Bacteroidales;f__Bacteroidaceae;g__Bacteroides;s__Bacteroides_vulgatus_ATCC_8482 1.000
OTU5    d__Bacteria;p__Bacteroidetes;c__Bacteroidia;o__Bacteroidales;f__Prevotellaceae;g__Prevotella;s__uncultured_bacterium    0.930
OTU7    d__Bacteria;p__Firmicutes;c__Clostridia;o__Clostridiales;f__Lachnospiraceae;g__Blautia;s__uncultured_Lachnospiraceae_bacterium  1.000
OTU4    d__Bacteria;p__Bacteroidetes;c__Bacteroidia;o__Bacteroidales;f__Bacteroidaceae;g__Bacteroides;s__Bacteroides_coprocola_DSM_17136        1.000
OTU3    d__Bacteria;p__Fusobacteria;c__Fusobacteriia;o__Fusobacteriales;f__Fusobacteriaceae;g__Fusobacterium;s__Fusobacterium_mortiferum        1.000

                                                                 
属性及其含义
-----------------------------------
* ``seq_num``  序列数量

相关方法
-----------------------------------
暂无

