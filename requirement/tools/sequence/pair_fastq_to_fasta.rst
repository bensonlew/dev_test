����˵��
==========================

Path
-----------

**sequence.pair_fastq_to_fasta**

���ܺ���;����
-----------------------------------

���ɶ�fastq�ļ�ת����fasta�ļ�

��Ҫ�������ģ��
-----------------------------------




�������
-----------------------------------

::

            {"name": "fastq_input1", "type": "infile", "format": "sequence.fastq"}, #�����ļ�fastq1
            {"name": "fastq_input2", "type": "infile", "format": "sequence.fastq"}, #�����ļ�fastq2
            {"name": "fq1_to_fasta_id", "type": "string", "default": "none"},  #�Զ���fastq1����id��Ĭ�Ͻ�����fastq�ļ�id����'@'�ĳ�'>'
            {"name": "fq2_to_fasta_id", "type": "string", "default": "none"}   #�Զ���fastq1����id��Ĭ�Ͻ�����fastq�ļ�id����'@'�ĳ�'>'

�����߼�
-----------------------------------
���������fastq_input1��fastq_input2ʱ���Ϳ������д�ģ��,fq1_to_fasta_id��fq2_to_fasta_idΪ��ѡ����