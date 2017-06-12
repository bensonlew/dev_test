工具说明
==========================

Path
-----------

**annotation.go.go_annotation**

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger-dev/app/program/Python/bin/python
/mnt/ilustre/users/sanger-dev/app//program/sun_jdk1.8.0/bin/java
/mnt/ilustre/users/sanger-dev/app/bioinfo/annotation/b2g4pipe_v2.5/

功能和用途描述
-----------------------------------

对比对到nr数据库的xml文件进行go注释及统计

使用程序
-----------------------------------

/mnt/ilustre/users/sanger-dev/app/bioinfo/annotation/b2g4pipe_v2.5/*
/mnt/ilustre/users/sanger-dev/app/bioinfo/annotation/b2g4pipe_v2.5/ext/*
/mnt/ilustre/users/sanger-dev/app/bioinfo/annotation/b2g4pipe_v2.5/b2gPipe.properties
/mnt/ilustre/users/sanger-dev/app/bioinfo/annotation/scripts/goMerge.py
/mnt/ilustre/users/sanger-dev/app/bioinfo/annotation/scripts/goAnnot.py
/mnt/ilustre/users/sanger-dev/app/bioinfo/annotation/scripts/goSplit.py

主要命令及功能模块
-----------------------------------

命令1 java -Xmx220g -cp /mnt/ilustre/users/sanger-dev/app/bioinfo/annotation/b2g4pipe_v2.5/*:/mnt/ilustre/users/sanger-dev/app/bioinfo/annotation/b2g4pipe_v2.5/ext/*: es.blast2go.prog.B2GAnnotPipe -in temp_blast_nr.xml -prop b2gPipe.properties -annot -out blast2go
命令2 python goMerge.py blast2go.annot GO.list
命令3 python goAnnot.py GO.list localhost biocluster102 sanger-dev-123
命令4 python goSplit.py go_detail.xls

参数设计
-----------------------------------

::

      {"name": "blastout", "type": "infile", "format": "align.blast.blast_xml"},  # 输入文件，比对到nr数据库的xml文件
      {"name": "go2level_out", "type": "outfile", "format": "annotation.go.level2"},  # 输出文件，注释到go数据库第二层级
      {"name": "golist_out", "type": "outfile", "format": "annotation.go.go_list"},  # 输出文件，注释到go数据库的go id的list
      {"name": "blast2go_annot", "type": "outfile", "format": "annotation.go.blast2go_annot"}  # 输出文件，注释到go数据库的blast2go.annot


运行逻辑
-----------------------------------

输入注释到nr数据库的xml文件blastout，运行命令1、2、3、4，得到go注释的level2/3/4层级及go.list文件
