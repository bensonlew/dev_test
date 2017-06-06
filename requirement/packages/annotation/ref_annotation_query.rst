
工具说明
==========================

Path
-----------

**package.annotation.ref_annotation_query**

功能和用途描述
-----------------------------------

传入各个数据库的部分注释结果文件，统计功能注释信息表（即应注释查询模块的功能注释信息表）

参数说明
-----------------------------------

outpath：输出结果路径：功能注释信息表的文件路径
gtf_path：gtf文件，提取转录本对应的基因ID和基因名称
cog_list：string2cog注释tool统计得到的cog_list.xls,提取cog/nog/kog及对应的功能分类信息
kegg_table：kegg_annotation注释tool统计得到的kegg_table.xls
gos_list：go_annotation注释tool统计得到的query_gos.list
blast_nr_table：blast比对nr库得到的结果文件(blast输出文件格式为6：table)
blast_swissprot_table: blast比对swissprot库得到的结果文件（blast输出文件格式为6：table）
pfam_domain: orf预测的结果pfam_domain

函数及功能
-----------------------------------

get_anno_stat:执行get_gene、get_kegg、get_go、get_cog、get_nr、get_swissprot、get_pfam方法，输出统计功能的注释信息表
get_gene: 找到转录本ID对应的基因ID及基因名称
get_kegg: 找到转录本ID对应的KO、KO_name、Pathway、Pathway_definition
get_go: 找到转录本ID对应的goID及term、term_type
get_cog: 找到转录本ID对应的cogID、nogID、kogID及功能分类
get_cog_group_categories: 找到cog/nog/kogID对应的功能分类
get_nr: 找到转录本ID对应NR库的最佳hit_name和描述
get_swissprot: 找到转录本ID对应swissprot库的最佳hit_name及描述
get_pfam: 找到转录本ID对应的最佳pfamID及domain
