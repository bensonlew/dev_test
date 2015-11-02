
格式说明
==========================

Path
-----------

**seq.fastq**


功能和用途描述
-----------------------------------

用于存储DNA测序序列及其质量存储


格式定义文档
-----------------------------------

FASTQ : https://en.wikipedia.org/wiki/FASTQ_format


格式举例
-----------------------------------

::

    @SRR001666.1 071112_SLXA-EAS1_s_7:5:1:817:345 length=36
    GGGTGATGGCCGCTGCCGATGGCGTCAAATCCCACC
    +SRR001666.1 071112_SLXA-EAS1_s_7:5:1:817:345 length=36
    IIIIIIIIIIIIIIIIIIIIIIIIIIIIII9IG9IC


属性及其含义
-----------------------------------

* ``isgzip``   是否被gzip压缩
* ``encode``   质量编码方式
* ``seq_number`` 序列数量
* ``bases``      碱基总数
* ``longest``    最长序列碱基数
* ``shortest``   最短序列碱基数


相关方法
-----------------------------------

``gzip`` 将序列压缩保存