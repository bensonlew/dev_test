
����˵��
==========================

Path
-----------

**ref_rna.gene_structure.rmats_bam**

����װ·��
-----------------------------------

/mnt/ilustre/users/sanger-dev/app/bioinfo/rna/rMATS.3.2.5

���ܺ���;����
-----------------------------------

����ÿ����Ʒ�Ķ�Ӧ��bam�ļ��Ͳο�������ע��gtf�ļ� ������Ʒ�����ɱ�����¼����ͼ��� ���������


ʹ�ó���
-----------------------------------

rMATS.3.2.5: http://rnaseq-mats.sourceforge.net/

��Ҫ�������ģ��
-----------------------------------
python RNASeq-MATS.py -b1 A1.bam,A2.bam,A3.bam��A�����µĸ��ظ��ıȶԽ��bam�ļ���  -b2 B1.bam,B2.bam,B3.bam��B�����µĸ��ظ��ıȶԽ��bam�ļ���  -len bam�ļ��ж����ĳ���   -gtf ref.gtf -o /root/dir/of/output  -t paired|single -novelSS 0|1 -c [0,1)  -analysis P|U 

ע�⣺��-analysis ��ֵ��Ϊ'P'����paired analysis��ʱ��A������B�����µ��ظ���Ӧ����Ҹ����ڵ���3 


�������
-----------------------------------

::
		{"name": "A_condition_bam_file_list_string", "type": "string"},
		{"name": "B_condition_bam_file_list_string", "type": "string"},
		{"name": "read_length", "type": "int"},
		{"name": "sequencing_library_type", "type": "string"},
		{"name": "ref_gtf_file", "type": "infile", "format": "ref_genome_anotation.gtf"},
		{"name": "output_root_dir", "type": "string"},
		{"name": "whether_to_find_novel_AS_sites", "type": "int", "default": 0}
		{"name": "analysis_mode", "type": "string", "default": "U"}
		{"name": "cutoff_splicing_difference", "type": "float", "default": 0.001}
		{"name": "constructing_sequencing_library_type", "type": "string", "default": "fr-unstranded"}
		

            
            


�����߼�
-----------------------------------
����rMATS,����������bam�ļ���ע���ļ������һ��������ҳ�Ǳ�ڵ�ASλ�㣨3.2.5�汾Ҳ����ͨ��reads�ֲ������µ�ASλ���exon������������Ӧλ�ò�ͬ��Ʒ��ı����졣�γ�����AS���ͣ�A3SS, A5SS, MXE, IR��SE����ע�ͺͲ����������ļ�������趨-novelSS�������������µļ���λ��������ӵ�ע���ļ�


