
工具说明
==========================

Path
-----------

**module.annotation.ref_annotation**

功能和用途描述
-----------------------------------

调用注释的tool，进行nr/swissprot/pfam/go/cog/kegg注释统计

主要命令及功能模块
-----------------------------------
1. 调用xml2table，将blast的xml文件转为table文件
2. 调用tool:go_annotation.py，对blast到nr的xml进行go注释及统计
3. 调用tool:go_upload.py，对客户上传的go注释文件进行go注释及统计
4. 调用tool:string2cogv9.py，对blast到string的xml或table文件进行cog注释及统计
5. 调用tool:kegg_annotation.py，对blast到kegg的xml文件进行kegg注释及统计
6. 调用tool:kegg_upload.py，对客户上传的kegg注释文件进行kegg注释及统计
7. 调用tool：ref_anno_stat.py，从转录本注释中提取出最长转录本的注释结果，并将转录本ID替换成基因ID，作为基因注释结果
8. 调用package：ref_annotation_query.py，得到注释查询到总表

参数设计
-----------------------------------



      {"name": "blast_nr_xml", "type": "infile", "format": "align.blast.blast_xml"},  # blast到nr的xml文件
      {"name": "blast_string_xml", "type": "infile", "format": "align.blast.blast_xml"},  # blast到string的xml文件
      {"name": "blast_kegg_xml", "type": "infile", "format": "align.blast.blast_xml"},  # blast到kegg的xml文件
      {"name": "blast_swissprot_xml", "type": "infile", "format": "align.blast.blast_xml"},  # blast到swissprot的xml文件
      {"name": "blast_nr_table", "type": "infile", "format": "align.blast.blast_table"},  # blast到nr的table文件
      {"name": "blast_string_table", "type": "infile", "format": "align.blast.blast_table"},  # blast到string的table文件
      {"name": "blast_kegg_table", "type": "infile", "format": "align.blast.blast_table"},  # blast到kegg的table文件
      {"name": "blast_swissprot_table", "type": "infile", "format": "align.blast.blast_table"},  # blast到swissprot的table文件
      {"name": "pfam_domain", "type": "infile", "format": "annotation.kegg.kegg_list"},  # orf预测的pfam_domain结果文件
      {"name": "gos_list_upload", "type": "infile", "format": "annotation.upload.anno_upload"},   # 客户上传go注释文件
      {"name": "kos_list_upload", "type": "infile", "format": "annotation.upload.anno_upload"},  # 客户上传kegg注释文件
      {"name": "gene_file", "type": "infile", "format": "rna.gene_list"},  # 最长转录本的list
      {"name": "ref_genome_gtf", "type": "infile", "format": "gene_structure.gtf"},  # 参考基因组gtf文件/新基因gtf文件，功能:将参考基因组转录本ID替换成gene ID
      {"name": "anno_statistics", "type": "bool", "default": True},  # 是否提取基因注释结果
      {"name": "go_annot", "type": "bool", "default": True},  # 是否进行go注释及统计
      {"name": "nr_annot", "type": "bool", "default": True},  # 是否进行nr注释及统计
      {"name": "taxonomy", "type": "string", "default": None},   # kegg数据库物种分类, Animals/Plants/Fungi/Protists/Archaea/Bacteria
      {"name": "gene_go_list", "type": "outfile", "format": "annotation.go.go_list"},
      {"name": "gene_kegg_table", "type": "outfile", "format": "annotation.kegg.kegg_table"},
      {"name": "gene_go_level_2", "type": "outfile", "format": "annotation.go.level2"},


运行逻辑
-----------------------------------

nr/string/swissprot的输入文件可为xml，也可为table，kegg、go的输入文件可为xml、table或客户上传kegg注释文件
go_annot、nr_annot、anno_statistics：代表进行go、nr注释及统计、提取基因蛛丝结果
gene_file、ref_genome_gtf：提取基因注释结果的必备文件
pfam_domain：是否进行提取基因的pfam注释结果
taxonomy：kegg分类
