
工具说明
==========================

Path
-----------

**package.annotation.transcript_gene**

功能和用途描述
-----------------------------------

根据gtf文件得到转录本ID和对应的基因ID，将注释中的最长转录本ID替换成基因ID

参数说明
-----------------------------------

tran_list：列表，所有的转录本ID
tran_gene：字典，转录本ID和对应的基因ID
gtf_path：序列的gtf文件
xml_path：blast的xml文件
gene_xml_path：将xml_path中转录本ID更换成基因ID的xml文件
table_path：blast的table文件
gene_table_path：将table_path中转录本ID更换成基因ID的table文件
go_list：go list文件
gene_go_list：将go_list文件的转录本ID替换成基因ID的list文件

函数及功能
-----------------------------------

get_gene_transcript： 得到转录本ID和对应的基因ID
get_gene_blast_xml：根据提供的基因和转录本对应关系，查找xml中的查询序列，将转录本ID替换成基因ID,生成新的xml
get_gene_blast_table： 根据提供的基因和转录本对应关系，查找table中的查询序列，将转录本ID替换成基因ID,生成新的table
get_gene_go_list：根据提供的基因和转录本对应关系，将go注释的go.list转录本ID替换成基因ID
