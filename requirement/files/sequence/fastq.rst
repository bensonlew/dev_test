
格式说明
==========================

Path
-----------

**seq.fastq**


功能和用途描述
-----------------------------------

用于存储DNA/RNA/Protien序列,序列方向，以及碱基质量等信息


格式定义文档
-----------------------------------

FASTQ : https://en.wikipedia.org/wiki/FASTQ_format


格式举例
-----------------------------------

::

    >SEQUENCE_1 some discription
    IPQFASRKQLSDAILKEAEEKIKEELKAQGKPEKIWDNIIPGKMNSFIADNSQLDSKLTL
    +
    !''*((((***+))%%%++)(%%%%).1***-+*''))**55CCF>>>>>>CCCCCCC65
    >SEQUENCE_2 some discription
    SATVSEINSETDFVAKNDQFIALTKDTTAHIQSNSLQSVEELHSSTINGVKFEEYLKSQI
    +
    !***((((*%%+))%%%++)(%%%%).1***-+*''))**55CCF>cc>>>CC56CCC##



属性及其含义
-----------------------------------

* ``isgzip``    是否被gzip压缩
* ``encode``    质量编码方式
* ``seq_number``    序列数量
* ``bases`` 碱基总数
* ``longest``   最长序列碱基数
* ``shortest``  最短序列碱基数


相关方法
-----------------------------------

``gzip``    将序列压缩保存
``convert_to_fasta``    将这个文件转化成fasta文件
