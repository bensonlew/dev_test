
格式说明
==========================

Path
-----------

**betadiversity.newwicktree**  # 目录设置待定

功能和用途描述
-----------------------------------

定义了newicktree文件，存储树文件内容及相关信息

格式定义文档
-----------------------------------

newick tree： http://evolution.genetics.washington.edu/phylip/newicktree.html
              https://en.wikipedia.org/wiki/Newick_format

格式举例
-----------------------------------

::
    (A:0.1,B:0.2,(C:0.3,D:0.4):0.5);

属性及其含义
-----------------------------------

*``maxdistance``         最远距离
*``mindistance``         最小距离
*``allequal``            是否所有样本等距（最大最小相等）

相关方法
-----------------------------------

``check``         继承重写file的check，检查格式是否符合tree  # 此处可能比较复杂