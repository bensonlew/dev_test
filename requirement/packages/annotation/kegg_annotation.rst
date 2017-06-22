
工具说明
==========================

Path
-----------

**package.annotation.kegg_annotation**

功能和用途描述
-----------------------------------

连接到mongo数据库，进行kegg注释及统计

参数说明
-----------------------------------

blast_xml：输入文件比对到kegg的xml文件
kegg_table：kegg_table.xls文件路径
taxonomy：kegg数据库物种分类, Animals/Plants/Fungi/Protists/Archaea/Bacteria
kegg_ids: 输入文件客户上传的客户广告注释文件经过file:anno_upload的get_transcript_anno方法分隔出来的list
pathway_path：pathway_table.xls文件路径
pidpath：pid.txt文件路径
pathwaydir：pathway通路图结果文件夹
image_magick：图片格式转了换软件路径（/mnt/ilustre/users/sanger-dev/app/program/ImageMagick/bin/convert）
layerfile: kegg_layer.xls结果文件路径
taxonomyfile: kegg_taxonomy.xls结果文件路径

函数及功能
-----------------------------------

pathSearch：输入blast比对的xml文件，输出kegg_table.xls
pathSearch_upload：输入kegg_ids，输出kegg_table.xls
pathTable：输入文件为kegg_table.xls,输出文件为pathway_table.xls,pid.txt，根据pathSearch生成的kegg_table.xls统计pathway的信息
getPic：画pathway通路图，将pdf格式转为png
keggLayer: 获取分类信息文件kegg_layer.xls、kegg_taxonomy.xls
