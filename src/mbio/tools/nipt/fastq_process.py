## !/mnt/ilustre/users/sanger-dev/app/program/Python/bin/python
# -*- coding: utf-8 -*-
# __author__ = "moli.zhou"
#last_modify:20170424

from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from biocluster.config import Config
import os
import re


class FastqProcessAgent(Agent):
    """
    产筛的生信shell部分功能
    处理fastq，得到bed2文件
    version v1.0
    author: moli.zhou
    """
    def __init__(self, parent):
        super(FastqProcessAgent, self).__init__(parent)
        options = [#输入的参数
            {"name": "sample_id", "type": "string"},
            {"name": "fastq_path", "type": "infile", "format": "sequence.fastq_dir"}
        ]
        self.add_option(options)
        self.step.add_steps("fastq2bed")
        self.on('start', self.stepstart)
        self.on('end', self.stepfinish)

    def stepstart(self):
        self.step.fastq2bed.start()
        self.step.update()

    def stepfinish(self):
        self.step.fastq2bed.finish()
        self.step.update()


    def check_options(self):
        """
        重写参数检测函数
        :return:
        """
        # if not self.option('query_amino'):
        #     raise OptionError("必须输入氨基酸序列")
        if not self.option('sample_id'):
            raise OptionError("必须输入样本名")
        return True

    def set_resource(self):
        """
        设置所需资源，需在之类中重写此方法 self._cpu ,self._memory
        :return:
        """
        self._cpu = 10
        self._memory = '50G'

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"]
        ])
        result_dir.add_regexp_rules([
            [".bed.2", "bed.2", "信息表"],
        ])
        super(FastqProcessAgent, self).end()


class FastqProcessTool(Tool):
    """
    蛋白质互作组预测tool
    """
    def __init__(self, config):
        super(FastqProcessTool, self).__init__(config)
        self._version = '1.0.1'
        # self.R_path = 'program/R-3.3.1/bin/'
        self.script_path = 'bioinfo/medical/scripts/'
        self.java_path = self.config.SOFTWARE_DIR +'/program/sun_jdk1.8.0/bin/java'
        self.picard_path = self.config.SOFTWARE_DIR +'/bioinfo/medical/picard-tools-2.2.4/picard.jar'
        self.sam_path = '/bioinfo/align/samtools-1.3.1/samtools'

        self.set_environ(PATH=self.config.SOFTWARE_DIR + '/gcc/5.1.0/bin')
        self.set_environ(LD_LIBRARY_PATH=self.config.SOFTWARE_DIR + '/gcc/5.1.0/lib64')
        self.set_environ(PATH=self.config.SOFTWARE_DIR + '/bioinfo/medical/FastQc')
        self.set_environ(PATH=self.config.SOFTWARE_DIR + '/bioinfo/medical/bwa-0.7.15/bin')
        self.set_environ(PATH=self.config.SOFTWARE_DIR + '/bioinfo/seq/bioawk')
        self.set_environ(PATH=self.config.SOFTWARE_DIR + '/bioinfo/seq/seqtk-master')
        self.set_environ(PATH=self.config.SOFTWARE_DIR + '/bioinfo/align/samtools-1.3.1')
        self.set_environ(PATH=self.config.SOFTWARE_DIR + '/bioinfo/medical/samblaster-0.1.22/bin')
        
        self.ref1 = self.config.SOFTWARE_DIR + '/database/human/hg38.chromosomal_assembly/ref.fa'
        self.ref = self.config.SOFTWARE_DIR + '/database/human/hg38_nipt/nchr.fa'
        self.bed_ref = self.config.SOFTWARE_DIR+ '/database/human/hg38_nipt/nchr.20k.gmn.bed'

    def run_tf(self):
        pre_cmd = '{}nipt_fastq_pre.sh {} {}'.format(self.script_path, self.option("fastq_path").prop['path'], self.option('sample_id'))
        self.logger.info(pre_cmd)
        cmd = self.add_command("pre_cmd", pre_cmd).run()
        self.wait(cmd)

        if cmd.return_code == 0:
            self.logger.info("处理接头成功")
        else:
            raise Exception("处理接头出错")

        seq_merge = '{}nipt_merge_align.sh {} {} '.\
            format(self.script_path,self.option("sample_id"),self.ref1)
        self.logger.info(seq_merge)
        cmd = self.add_command("seq_merge",seq_merge).run()
        self.wait(cmd)
        if cmd.return_code == 0:
            self.logger.info("seqtk mergepe成功")
        else:
            raise Exception("seqtk mergepe出错")

        cut_adapt = '/bioinfo/medical/cutadapt-1.10-py27_0/bin/cutadapt --format fastq --zero-cap -q 1 --trim-n ' \
                    '--minimum-length 30 --times 7 -a GATCGGAAGAGCACACGTCTGAACTCCAGTCAC -o {}_R1.cut.fastq  {}_R1.cutN.fastq'.\
            format(self.option("sample_id"),self.option("sample_id"))
        self.logger.info(cut_adapt)
        cmd = self.add_command("cut_adapt",cut_adapt).run()
        self.wait(cmd)

        if cmd.return_code == 0:
            self.logger.info("cutadapt去接头成功")
        else:
            raise Exception("cutadapt去接头出错")

        cut_50 = '{}nipt_cut50.sh {} {}'.format(self.script_path,self.option('sample_id'), self.ref)
        self.logger.info(cut_50)
        cmd = self.add_command("cut_50",cut_50).run()
        self.wait(cmd)
        if cmd.return_code == 0:
            self.logger.info("cut_50截取成功")
        else:
            raise Exception("cut_50截取出错")

        sam_cutbam="{} view -h -@ 10 {}_R1.cut.bam -o {}.temp.cut.sam".format(self.sam_path,self.option('sample_id'),self.option('sample_id'))
        self.logger.info(sam_cutbam)
        cmd = self.add_command("sam_cutbam",sam_cutbam).run()
        self.wait(cmd)
        if cmd.return_code == 0:
            self.logger.info("sam_cutbam成功")
        else:
            raise Exception("sam_cutbam出错") 


        file = self.option('sample_id') + '.temp.cut.sam'
        with open(file,'r') as f:
            lines = f.readlines()
        with open(file,'w+') as f_w:
            for line in lines:
                if 'XT:A:U' in line or re.search('^@', line):
                    f_w.write(line)
                else:
                    continue

        sam_cut_uniq='{} view -@ 10 -bS {}.temp.cut.sam -o {}_R1.cut.uniq.bam'\
            .format(self.sam_path, self.option('sample_id'),self.option('sample_id'))
        self.logger.info(sam_cut_uniq)
        cmd = self.add_command("sam_cut_uniq",sam_cut_uniq).run()
        self.wait(cmd)
        if cmd.return_code == 0:
            self.logger.info("sam_cut_uniq成功")
        else:
            raise Exception("sam_cut_uniq出错") 

        sam_sort='{} sort -@ 10 {}_R1.cut.uniq.bam -o {}_R1.cut.uniq.sort.bam'.format(self.sam_path, self.option('sample_id'),self.option('sample_id'))
        self.logger.info(sam_sort)
        cmd = self.add_command("sam_sort",sam_sort).run()
        self.wait(cmd)
        if cmd.return_code == 0:
            self.logger.info("sam排序成功")
        else:
            raise Exception("sam排序出错")

        picard_cmd='/program/sun_jdk1.8.0/bin/java -Xmx10g -Djava.io.tmpdir={} -jar {} MarkDuplicates VALIDATION_STRINGENCY=LENIENT INPUT={}_R1.cut.uniq.sort.bam OUTPUT={}_R1.cut.uniq.sort.md.bam METRICS_FILE={}_R1.cut.uniq.sort.md.metrics'\
            .format(self.work_dir, self.picard_path,self.option('sample_id'),self.option('sample_id'),self.option('sample_id'))
        self.logger.info(picard_cmd)
        cmd = self.add_command("picard_cmd",picard_cmd).run()
        self.wait(cmd)
        if cmd.return_code == 0:
            self.logger.info("picard成功")
        else:
            raise Exception("picard出错")

        sam_valid = '{} view -F 1024 -@ 10 -bS {}_R1.cut.uniq.sort.md.bam -o {}_R1.valid.bam'\
            .format(self.sam_path, self.option('sample_id'),self.option('sample_id'))
        self.logger.info(sam_valid)
        cmd = self.add_command("sam_valid",sam_valid).run()
        self.wait(cmd)
        if cmd.return_code == 0:
            self.logger.info("sam_valid成功")
        else:
            raise Exception("sam_valid出错")

        sam_valid_index='{} index {}_R1.valid.bam'.format(self.sam_path, self.option('sample_id'))
        self.logger.info(sam_valid_index)
        cmd = self.add_command("sam_valid_index",sam_valid_index).run()
        self.wait(cmd)
        if cmd.return_code == 0:
            self.logger.info("sam_valid_index成功")
        else:
            raise Exception("sam_valid_index出错")

        sam_map='{} view -bF 4 -@ 10 {}_R1.valid.bam -o {}_R1.map.valid.bam'\
            .format(self.sam_path, self.option('sample_id'),self.option('sample_id'))
        self.logger.info(sam_map)
        cmd = self.add_command("sam_map",sam_map).run()
        self.wait(cmd)
        if cmd.return_code == 0:
            self.logger.info("sam_map成功")
        else:
            raise Exception("sam_map出错")

        sam_map_index='{} index {}_R1.map.valid.bam'.format(self.sam_path, self.option('sample_id'))
        self.logger.info(sam_map_index)
        cmd = self.add_command("re_sam_map_index",sam_map_index).run()
        self.wait(cmd)
        if cmd.return_code == 0:
            self.logger.info("sam_map_index成功")
        else:
            raise Exception("sam_map_index出错") 

        bed_qc='{}nipt_bed.sh {} {} {} {}'\
            .format(self.script_path,self.option('sample_id'),self.bed_ref,self.work_dir,self.option("fastq_path").prop['path'])
        self.logger.info(bed_qc)
        cmd = self.add_command("bed_qc",bed_qc).run()
        self.wait(cmd)
        if cmd.return_code == 0:
            self.logger.info("bed文件生成成功")
        else:
            raise Exception("bed文件生成出错") 

    def set_output(self):
        """
        将结果文件link到output文件夹下面
        :return:
        """
        for root, dirs, files in os.walk(self.output_dir):
            for names in files:
                os.remove(os.path.join(root, names))
        self.logger.info("设置结果目录")
        results = os.listdir(self.work_dir)
        for f in results:
            if re.search(r'.*map.valid.bam$', f):
                os.link(self.work_dir +'/'+ f, self.output_dir + '/' + f)
            elif re.search(r'.*qc$', f):
                os.link(self.work_dir + '/' + f, self.output_dir + '/' + f)
            elif re.search(r'.*bed.2$', f):
                os.link(self.work_dir + '/' + f, self.output_dir + '/' + f)
        self.logger.info('设置文件夹路径成功')

    def run(self):
        super(FastqProcessTool, self).run()
        self.run_tf()
        self.set_output()
        self.end()
