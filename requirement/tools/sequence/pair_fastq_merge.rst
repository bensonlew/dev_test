����˵��
==========================

Path
-----------

**sequence.pair_fastq_merge**

����װ·��
-----------------------------------

/mnt/ilustre/users/sanger/app/pear/bin

���ܺ���;����
-----------------------------------

����pear���ɶ�fastq�ļ�����overlap��read1��read2��������

ʹ�ó���
-----------------------------------

pear

��Ҫ�������ģ��
-----------------------------------

"pear -f %s -r %s -o merge" % (self.option('fastq_input1').prop["path"],self.option('fastq_input2').prop["path"])

�������
-----------------------------------

::

            {"name": "fastq_input1", "type": "infile", "format": "sequence.fastq"}, #�����ļ�fastq1
            {"name": "fastq_input2", "type": "infile", "format": "sequence.fastq"}, #�����ļ�fastq2

�����߼�
-----------------------------------
���������fastq_input1��fastq_input2ʱ���Ϳ������д�ģ��