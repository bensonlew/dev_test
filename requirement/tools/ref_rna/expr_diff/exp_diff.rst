
����˵��
==========================

Path
-----------



����װ·��
-----------------------------------

R���

���ܺ���;����
-----------------------------------

ͨ�������ı������������֮�����ϵ���ܶ�


ʹ�ó���
-----------------------------------

R:https://www.r-project.org/

�������
-----------------------------------

    {"name": "diff_fpkm", "type": "infile", "format": "denovo_rna.express.express_matrix"},  #�����ļ������������������        
	{"name": "distance_method", "type": "string", "default": "euclidean"},  # ���������㷨        
	{"name": "method", "type": "string", "default": "all"},  # ���෽��ѡ��
	{"name": "lognorm", "type": "int", "default": 10}  # ����ͼʱ��ԭʼ�����ȡ������������Ϊ10��2


�����߼�
-----------------------------------

ͨ������Է�������֮��Ĳ����ԣ�Ϊ�˿ͷ����Լ��������������֮��Ĳ������ṩ�˲ο���




++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++=




����˵��
==========================

Path
-----------



����װ·��
-----------------------------------

R���

���ܺ���;����
-----------------------------------

ʹ��Ballgown��ɴ�stringtie����������ݷ�����


ʹ�ó���
-----------------------------------

R:https://www.r-project.org/
Ballgown��http://www.bioconductor.org/packages/release/bioc/vignettes/ballgown/inst/doc/ballgown.html


�������
-----------------------------------

    {"name": "diff_fpkm", "type": "infile", "format": "denovo_rna.express.express_matrix"},  #�����ļ������������������        
	{"name": "feature", "type": "string", "default": "transcript"},  # ѡ���������        
	{"name": "meas", "type": "string", "default": "FPKM"},  # ѡ�������������


�����߼�
-----------------------------------

ͨ��FPKMֵ��������֮������Լ��飬��ò������


++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++



����˵��
==========================

Path
-----------



����װ·��
-----------------------------------

R���

���ܺ���;����
-----------------------------------

ʹ��DESeq2��ɴ�stringtie����������ݷ�����(���䣬�����limma���������������R������)


ʹ�ó���
-----------------------------------

R:https://www.r-project.org/
http://www.bioconductor.org/packages/release/bioc/html/DESeq2.html


�������
-----------------------------------

    {"name": "count", "type": "infile", "format": "denovo_rna.express.express_matrix"},  # �����ļ�������������
    {"name": "fpkm", "type": "infile", "format": "denovo_rna.express.express_matrix"},  # �����ļ���������������        
	{"name": "dispersion", "type": "float", "default": 0.1},  # edger��ɢֵ
	{"name": "min_rowsum_counts", "type": "int", "default": 2},  # ��ɢֵ���Ƽ������С����ֵ
	{"name": "edger_group", "type": "infile", "format": "meta.otu.group_table"},  # ������ѧ�ظ���ʱ��ķ����ļ�
	{"name": "sample_list", "type": "string", "default": ''},  # ѡ���������������������������á���������,���ظ�ʱû�иò���
	{"name": "control_file", "type": "infile", "format": "denovo_rna.express.control_table"},  # �������ļ�����ʽͬ�����ļ�
	{"name": "diff_ci", "type": "float", "default": 0.05},  # ������ˮƽ
	{"name": "diff_count", "type": "outfile", "format": "denovo_rna.express.express_matrix"},  # ������������
	{"name": "diff_fpkm", "type": "outfile", "format": "denovo_rna.express.express_matrix"},  # �������������
	{"name": "gene_file", "type": "outfile", "format": "denovo_rna.express.gene_list"}
	{"name": "diff_list_dir", "type": "outfile", "format": "denovo_rna.express.gene_list_dir"},
	{"name": "gname", "type": "string"},  # ���鷽������
	{"name": "diff_rate", "type": "float", "default": 0.01}  # �����Ĳ���������

�����߼�
-----------------------------------

ͨ��FPKMֵ��������֮������Լ��飬��ò������


