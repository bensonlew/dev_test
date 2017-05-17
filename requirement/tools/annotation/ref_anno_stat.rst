工具说明
==========================

Path
-----------

**rna.ref_anno_stat**

程序安装路径
-----------------------------------

/mnt/ilustre/users/sanger-dev/app/program/Python/bin/python

功能和用途描述
-----------------------------------

从转录本的注释结果中（nr,go,cog,pfam,kegg,swissprot）筛选出最长转录本的注释，将ID换成基因的ID，统计注释到nr,go,cog,pfam,kegg,swissprot数据库的转录本和基因的数目及id

使用程序
-----------------------------------

/mnt/ilustre/users/sanger-dev/app/bioinfo/annotation/scripts/goAnnot.py
/mnt/ilustre/users/sanger-dev/app/bioinfo/annotation/scripts/goSplit.py
/mnt/ilustre/users/sanger-dev/app/bioinfo/annotation/scripts/string2cog_v9.py
/mnt/ilustre/users/sanger-dev/app/bioinfo/annotation/scripts/cog_annot.py
/mnt/ilustre/users/sanger-dev/biocluster/src/mbio/packages/align/blast/xml2table.py
/mnt/ilustre/users/sanger-dev/biocluster/src/mbio/packages/align/blast/blastout_statistics.py


主要命令及功能模块
-----------------------------------

命令1 xml2table(xml, table)
命令2 blastout_statistics(blast_table, evalue_path, similarity_path)
命令3 kegg_anno.pathSearch_upload(kegg_ids, kegg_table, taxonomy)
      kegg_anno.pathTable(kegg_table, pathway_path, pidpath)
      kegg_anno.getPic(pidpath, pathwaydir)
      kegg_anno.keggLayer(pathway_table, layerfile, taxonomyfile)
命令4 python goAnnot.py GO.list localhost biocluster102 sanger-dev-123
命令5 python goSplit.py go_detail.xls

参数设计
-----------------------------------

::

      {"name": "nr_xml", "type": "infile", "format": "align.blast.blast_xml"},  # 输入文件，blast比对到nr库的xml结果文件
      {"name": "blast2go_annot", "type": "infile", "format": "annotation.go.blast2go_annot"},  # 输入文件，go注释的结果文件blast2go.annot
      {"name": "gos_list", "type": "infile", "format": "annotation.go.go_list"},  # 输入文件，go注释的结果文件go_list
      {"name": "gos_list_upload", "type": "infile", "format": "annotation.upload.anno_upload"},   # 输入文件，客户上传go注释文件
      {"name": "kegg_xml", "type": "infile", "format": "align.blast.blast_xml"},  # 输入文件，blast到kegg数据库的xml文件
      {"name": "kos_list_upload", "type": "infile", "format": "annotation.upload.anno_upload"},  # 输入文件，客户上传kegg注释文件
      {"name": "string_xml", "type": "infile", "format": "align.blast.blast_xml"},  # 输入文件，blast到string数据库的xml文件
      {"name": "string_table", "type": "infile", "format": "align.blast.blast_table"},  # 输入文件，blast到string数控的table文件
      {"name": "cog_list", "type": "infile", "format": "annotation.cog.cog_list"},  # 输入文件，cog注释的结果文件cog_list.xls
      {"name": "cog_table", "type": "infile", "format": "annotation.cog.cog_table"},  # 输入文件，cog注释的结果文件cog_table.xls
      {"name": "pfam_domain", "type": "infile", "format": "annotation.kegg.kegg_list"},  # 输入文件，pfam注释（orf预测）的结果文件pfam_domain
      {"name": "gene_file", "type": "infile", "format": "rna.gene_list"},  # 输入文件，最长转录本的list
      {"name": "swissprot_xml", "type": "infile", "format": "align.blast.blast_xml"},  # 输入文件，blast到swissprot的xml文件
      {"name": "ref_genome_gtf", "type": "infile", "format": "gene_structure.gtf"},  # 输入文件，参考基因组gtf文件/新基因gtf文件
      {"name": "database", "type": "string", "default": "nr,go,cog,pfam,kegg,swissprot"},  # 需要进行注释统计的数据库
      {"name": "gene_nr_table", "type": "outfile", "format": "align.blast.blast_table"},  # 输出文件，基因序列blast到NR的注释结果table
      {"name": "gene_string_table", "type": "outfile", "format": "align.blast.blast_table"},  # 输出文件，基因序列blast到String的注释结果table
      {"name": "gene_kegg_table", "type": "outfile", "format": "align.blast.blast_table"},  # 输出文件，基因序列blast到kegg的注释结果table
      {"name": "gene_swissprot_table", "type": "outfile", "format": "align.blast.blast_table"},  # 输出文件，基因序列blast到Swiss-Prot的注释结果table
      {"name": "gene_go_level_2", "type": "outfile", "format": "annotation.go.level2"},  # 输出文件，基因序列的go_level2结果文件
      {"name": "gene_go_list", "type": "outfile", "format": "annotation.go.go_list"},  # 输出文件，基因序列的go_list结果文件
      {"name": "gene_kegg_anno_table", "type": "outfile", "format": "annotation.kegg.kegg_table"},  # 输出文件，基因序列的kegg_table结果文件
      {"name": "gene_pfam_domain", "type": "outfile", "format": "annotation.kegg.kegg_list"},  # 输出文件，基因序列的pfam_domain结果文件



运行逻辑
-----------------------------------
提取基因注释必须有ref_genome_gtf、gene_file作为输入文件，database根据提取基因注释的数据库确定
nr提取基因注释的输入文件：nr_xml
go提取基因注释的输入文件：blast2go_annot和gos_list或者gos_list_upload
kegg提取基因注释的输入文件：kegg_xml或kos_list_upload
cog提取基因注释的输入文件：string_xml、cog_list、cog_table或string_table、cog_list、cog_table
pfam提取基因注释的输入文件：pfam_domain
swissprot提取基因注释的输入文件：swissprot_xml
根据gene_file文件提取出最长转录本的注释，根据ref_genome_gtf将最长转录本的ID换为基因ID
最后统计注释到每个数据库的基因和转录本的数目及比例
