工具说明
==========================

Path
-----------

**annotation.merge_annot**

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger-dev/app/program/Python/bin/python

功能和用途描述
-----------------------------------

将go注释的go_list、kegg_table、cog_table文件结果合一起，作为cog、go、kegg富集和分类的输入文件

使用程序
-----------------------------------

/mnt/ilustre/users/sanger-dev/biocluster/src/mbio/packages/align/blast/blastout_statistics.py

主要命令及功能模块
-----------------------------------

python goAnnot.py GO.list localhost biocluster102 sanger-dev-123
python goSplit.py go_detail.xls
merge(dirs, merge_file)
run_merge()

参数设计
-----------------------------------

::

      {"name": "gos_dir", "type": "string", "default": None},  # go_list的路径，以；分割
      {"name": "kegg_table_dir", "type": "string", "default": None},  # kegg_table的路径，以；分割
      {"name": "cog_table_dir", "type": "string", "default": None},  # cog_table的路径，以；分割
      {"name": "database", "type": "string", "default": "go,cog,kegg"},  # 需要合并文件的数据库
      {"name": "go2level_out", "type": "outfile", "format": "annotation.go.level2"},  # go level2的注释文件
      {"name": "golist_out", "type": "outfile", "format": "annotation.go.go_list"},  # go注释的go_list
      {"name": "kegg_table", "type": "outfile", "format": "annotation.kegg.kegg_table"},  # kegg注释的kegg_table
      {"name": "cog_table", "type": "outfile", "format": "annotation.cog.cog_table"}  # cog注释的cog_table


运行逻辑
-----------------------------------

输入已知序列和新序列的go_list、kegg_table、cog_table文件的路径，以";"分隔，进行go_list、kegg_table、cog_table的合并，输出已知序列和新序列的go_list、kegg_table、cog_table文件，方便进行cog、go、kegg的富集和分类
