
格式说明
==========================

Path
-----------

**seq.fasta**


功能和用途描述
-----------------------------------

用于存储DNA/RNA/Protien序列信息


格式定义文档
-----------------------------------

FASTA : https://en.wikipedia.org/wiki/FASTA_format


格式举例
-----------------------------------

::

    >SEQUENCE_1 some discription
    MTEITAAMVKELRESTGAGMMDCKNALSETNGDFDKAVQLLREKGLGKAAKKADRLAAEG
    LVSVKVSDDFTIAAMRPSYLSYEDLDMTFVENEYKALVAELEKENEERRRLKDPNKPEHK
    IPQFASRKQLSDAILKEAEEKIKEELKAQGKPEKIWDNIIPGKMNSFIADNSQLDSKLTL
    MGQFYVMDDKKTVEQVIAEKEKEFGGKIKIVEFICFEVGEGLEKKTEDFAAEVAAQL
    >SEQUENCE_2 some discription
    SATVSEINSETDFVAKNDQFIALTKDTTAHIQSNSLQSVEELHSSTINGVKFEEYLKSQI
    ATIGENLVVRRFATLKAGANGVVNGYIHTNGRVGVVIAAACDSAEVASKSRDLLRQICMH



属性及其含义
-----------------------------------

* ``seq_type``   序列种类 如DNA/RNA/Protien
* ``seq_number`` 序列数量
* ``bases``      碱基总数
* ``longest``    最长序列碱基数
* ``shortest``   最短序列碱基数


相关方法
-----------------------------------

``split`` 将一个大序列拆分成数个小序列