
格式说明
==========================

Path
-----------

**seq.fastq**


功能和用途描述
-----------------------------------

用于存储DNA/RNA/Protien序列,以及碱基质量等信息


格式定义文档
-----------------------------------

FASTQ : https://en.wikipedia.org/wiki/FASTQ_format
输入文件可以是.fastq,.fq,.fastq.gz或者.fq.gz

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

* ``is_gz``    是否被gzip压缩
* ``seq_number``    序列数量
* ``bases`` 碱基总数
* ``longest``   最长序列碱基数
* ``shortest``  最短序列碱基数


相关方法
-----------------------------------

``gzip``    将序列压缩保存,返回压缩后序列的路径
``gunzip``  将序列解压，返回解压缩后序列的路径
``convert_to_fasta``    将这个文件转化成fasta文件，返回转化后的fasta格式的文件
