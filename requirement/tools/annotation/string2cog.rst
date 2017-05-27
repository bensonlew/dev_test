工具说明
==========================

Path
-----------

**annotation.cog.string2cogv9**

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger-dev/app/program/Python/bin/python


功能和用途描述
-----------------------------------

对比对到string数据库的xml文件或table文件进行cog注释及统计

使用程序
-----------------------------------

/mnt/ilustre/users/sanger-dev/app/bioinfo/annotation/scripts/cog_annot.py
/mnt/ilustre/users/sanger-dev/app/bioinfo/annotation/scripts/string2cog_v9.py

主要命令及功能模块
-----------------------------------

python cog_annot.py blast_string.xls output_dir
python string2cog_v9.py blast_string.xml output_dir

参数设计
-----------------------------------

::

      {"name": "blastout", "type": "infile", "format": "align.blast.blast_xml"},  # 输入文件，比对到string数据库的xml文件
      {"name": "string_table", "type": "infile", "format": "align.blast.blast_table"},  # 输入文件，比对到string数据库的table文件
      {"name": "cog_list", "type": "outfile", "format": "annotation.cog.cog_list"},  # 输出文件，cog_list.xls
      {"name": "cog_table", "type": "outfile", "format": "annotation.cog.cog_table"},  # 输出文件，cog_table.xls
      {"name": "cog_summary", "type": "outfile", "format": "annotation.cog.cog_summary"}  # 输出文件，cog_summary.xls


运行逻辑
-----------------------------------

输入文件为blastout时，对比对到string的xml文件用string2cog_v9.py进行进行cog注释及统计
输入文件为string_table时，对比对到string的table文件用cog_annot.py脚本进行cog注释及统计
优先对xml文件进行注释
