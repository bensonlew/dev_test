工具说明
==========================

Path
-----------

**annotation.kegg.kegg_annotation**

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger-dev/app/program/Python/bin/python

功能和用途描述
-----------------------------------

对比对到kegg数据库的xml文件进行kegg注释及统计

使用程序
-----------------------------------

/mnt/ilustre/users/sanger-dev/biocluster/src/mbio/packages/annotation/kegg_annotation.py

主要命令及功能模块
-----------------------------------

kegg_anno = self.load_package('annotation.kegg_annotation')()
kegg_anno.pathSearch_upload(kegg_ids, kegg_table, taxonomy)
kegg_anno.pathTable(kegg_table, pathway_path, pidpath)
kegg_anno.getPic(pidpath, pathwaydir)
kegg_anno.keggLayer(pathway_table, layerfile, taxonomyfile)

参数设计
-----------------------------------

::

      {"name": "kos_list_upload", "type": "infile", "format": "annotation.upload.anno_upload"},  # 输入文件，客户上传的kegg注释文件
      {"name": "taxonomy", "type": "string", "default": None},   # kegg数据库物种分类, Animals/Plants/Fungi/Protists/Archaea/Bacteria
      {"name": "kegg_table", "type": "outfile", "format": "annotation.kegg.kegg_table"},  # 输出文件，kegg_table.xls



运行逻辑
-----------------------------------

输入客户双穿的kegg注释文件kos_list_upload，taxonomy用package里的kegg_annotation.py进行kegg注释及统计
taxonomy参数是物种的类型，进行筛选，剔除在物种外的pathway，可为空
