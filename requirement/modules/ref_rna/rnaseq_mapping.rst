
����˵��
==========================

Path
-----------

**rnaseq_mapping.rnaseq_mapping**

���ܺ���;����
-----------------------------------

���ڵ��þ���Ĳ����Է�����tool

��Ҫ�������ģ��
-----------------------------------
1.��������
    hisat2-build -f <�ο��������ļ�> <��������>
    bowtie2-build <�ο��������ļ�> <��������>

2.����tophat2��hisat2���бȶԣ�����bam��ʽ�Ľ���ļ�

3.���û������gff��ʽ�ļ�ת��Ϊgtf��ʽ�ļ�������ʹ��
gffread <gff��ʽ�ļ�> -T -o <gtf��ʽ�ļ�>

4.ʹ��bedops�е�gff2bed��gff�ļ�ת��Ϊbed������ʹ��
perl gff2bed -d < <gff��ʽ�ļ�> > <bed��ʽ�ļ�>
�������
-----------------------------------

::
            {"name": "ref_genome", "type": "string"},  # �ο������飬��ҳ���ϳ���Ϊ�����˵��е�ѡ��
            {"name":"ref_genome_custom", "type": "infile", "format": "sequence.fasta"},  # �Զ���ο������飬�û�ѡ��customer_modeʱ����Ҫ����ο�������
            {"name": "mapping_method", "type": "string"},  # �����ֶΣ���Ϊtophat�����hisat����
            {"name":"seq_method", "type": "string"},  # ˫�˲����ǵ��˲���
            {"name": "single_end_reads", "type": "infile", "format": "sequence.fastq"},  # ��������
            {"name": "left_reads", "type": "infile", "format":"sequence.fastq"},  # ˫�˲���ʱ���������
            {"name": "right_reads", "type": "infile", "format":"sequence.fastq"},  # ˫�˲���ʱ���Ҷ�����
            {"name": "gff","type": "infile", "format":"ref_rna.reads_mapping.gff"},  # gff��ʽ�ļ�
            {"name": "bam_output", "type": "outfile", "format": "align.bwa.bam"},  # �����bam
            {"name": "gtf", "type": "outfile", "format" : "ref_rna.reads_mapping.gtf"}  # �����gtf��ʽ�ļ�
            {"name": "bed", "type": "outfile", "format" : "ref_rna.reads_mapping.bed"}  # �����bed��ʽ�ļ�



�����߼�
-----------------------------------
��"mapping_method"Ϊ��tophat��ʱ��ʹ��tophat���бȶ�
����mapping_method��Ϊ��hisat��ʱ��ʹ��hisat���бȶ�
