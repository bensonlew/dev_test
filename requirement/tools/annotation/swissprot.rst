工具说明
==========================

Path
-----------

**annotation.swissprot**

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger-dev/app/program/Python/bin/python

功能和用途描述
-----------------------------------

对blast到swissprot库的xml文件进行swissprot注释及统计

使用程序
-----------------------------------

/mnt/ilustre/users/sanger-dev/biocluster/src/mbio/packages/align/blast/xml2table.py

主要命令及功能模块
-----------------------------------

from mbio.packages.align.blast.xml2table import *
xml2table(xml, table)

参数设计
-----------------------------------

::

      {"name": "blastout", "type": "infile", "format": "align.blast.blast_xml"},  # 输入文件，blast到swissprot的xml文件
      {"name": "swissprot_table", "type": "outfile", "format": "align.blast.blast_table"},  # 输出文件， blast到swissprot的table文件



运行逻辑
-----------------------------------

输入blast到swissprot库的xml文件，将swissprot的xml文件转为table文件
