工具说明
==========================

Path
-----------

**annotation.go.go_upload**

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger-dev/app/program/Python/bin/python

功能和用途描述
-----------------------------------

对客户上传的go注释文件进行go注释及统计

使用程序
-----------------------------------

/mnt/ilustre/users/sanger-dev/app/bioinfo/annotation/scripts/goAnnot.py
/mnt/ilustre/users/sanger-dev/app/bioinfo/annotation/scripts/goSplit.py

主要命令及功能模块
-----------------------------------

命令1 python goAnnot.py GO.list localhost biocluster102 sanger-dev-123
命令2 python goSplit.py go_detail.xls

参数设计
-----------------------------------

::

      {"name": "gos_list_upload", "type": "infile", "format": "annotation.upload.anno_upload"},  # 输入文件，客户上传的go注释文件
      {"name": "go2level_out", "type": "outfile", "format": "annotation.go.level2"}  # 输出文件，注释到go数据库第二层级



运行逻辑
-----------------------------------

输入客户上传的go注释文件gos_list_upload，运行命令1、2，得到go注释的level2/3/4层级及go.list文件
