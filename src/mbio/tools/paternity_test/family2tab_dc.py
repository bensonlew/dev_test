## !/mnt/ilustre/users/sanger-dev/app/program/Python/bin/python
# -*- coding: utf-8 -*-
# __author__ = "moli.zhou"
#date:20170410

from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from biocluster.config import Config
import os
import re
import shutil


class Family2tabDcAgent(Agent):
    """
    调用fastq2bam.sh脚本，完成无创亲子鉴定的生信分析流程中将fastq转为bam文件
    version v1.0
    author: hongdongxuan
    modified: moli.zhou
    last_modify: 2016.11.28
    """
    def __init__(self, parent):
        super(Family2tabDcAgent, self).__init__(parent)
        options = [
            {"name": "fastq", "type": "string"},  #输入F/M/S的fastq文件的样本名,fastq_gz_dir/WQ235F
            {"name": "ref_fasta", "type": "infile", "format": "sequence.fasta"}, #hg38.chromosomal_assembly/ref.fa
            {"name": "targets_bedfile","type": "infile","format":"paternity_test.rda"},
            {"name": "seq_path", "type": "infile","format":"sequence.fastq_dir"}, #fastq所在路径
            {"name": "cpu_number", "type": "int", "default": 4},
            {"name": "batch_id", "type": "string"}
        ]
        self.add_option(options)
        self.step.add_steps("family2tab")
        self.on('start', self.stepstart)
        self.on('end', self.stepfinish)

    def stepstart(self):
        self.step.family2tab.start()
        self.step.update()

    def stepfinish(self):
        self.step.family2tab.finish()
        self.step.update()


    def check_options(self):
        """
        重写参数检测函数
        :return:
        """
        if not self.option("fastq"):
            raise OptionError("必须输入fastq文件的样本名")
        if not self.option("ref_fasta").is_set:
            raise OptionError("必须输入参考基因组的fastq文件")
        if not self.option('seq_path'):
            raise OptionError('必须提供fastq文件所在的路径')
        if not self.option('targets_bedfile'):
            raise OptionError('必须提供target_bedfile文件')
        return True

    def set_resource(self):
        """
        设置所需资源，需在之类中重写此方法 self._cpu ,self._memory
        :return:
        """
        self._cpu = 10
        self._memory = '60G'

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"],
                    ])
        result_dir.add_regexp_rules([
            [r".mem.sort.hit.filter.bam", "bam", "所有位点的信息"],
            [r".qc", "qc", "数据质控文件"]

        ])
        super(Family2tabDcAgent, self).end()


class Family2tabDcTool(Tool):
    """
    运行fastq2bam.sh sample_id cpu_num ref fastq_dir targets_bedfile
    """
    def __init__(self, config):
        super(Family2tabDcTool, self).__init__(config)
        self._version = '1.0.1'
        self.cmd_path = "bioinfo/medical/scripts/"
        self.set_environ(LD_LIBRARY_PATH=self.config.SOFTWARE_DIR + '/gcc/5.1.0/lib64')
        self.set_environ(PATH=self.config.SOFTWARE_DIR + '/gcc/5.1.0/bin')
        self.set_environ(PATH=self.config.SOFTWARE_DIR + '/program/ruby-2.3.1')#测试机
        # self.set_environ(PATH=self.config.SOFTWARE_DIR + '/program/ruby-2.4.1/bin')#正式机
        self.set_environ(PATH=self.config.SOFTWARE_DIR + '/bioinfo/seq/bioruby-vcf-master/bin') #测试机
        self.set_environ(PATH=self.config.SOFTWARE_DIR + '/bioinfo/seq/bioawk')
        self.set_environ(PATH=self.config.SOFTWARE_DIR + '/bioinfo/seq/seqtk-master')
        self.set_environ(PATH=self.config.SOFTWARE_DIR + '/bioinfo/align/bwa-0.7.15')
        self.set_environ(PATH=self.config.SOFTWARE_DIR + '/bioinfo/seq/samblaster-0.1.24')
        self.set_environ(PATH=self.config.SOFTWARE_DIR + '/bioinfo/align/samtools-1.4/bin')
        self.set_environ(PATH=self.config.SOFTWARE_DIR + '/bioinfo/medical/bedtools-2.24.0/bin')
        self.set_environ(PATH=self.config.SOFTWARE_DIR + '/program/sun_jdk1.8.0/bin')
        self.set_environ(PATH=self.config.SOFTWARE_DIR + '/bioinfo/seq/bcftools-1.4/bin')
        self.set_environ(PATH=self.config.SOFTWARE_DIR + '/bioinfo/seq/vt-0.577')
        self.set_environ(PATH=self.config.SOFTWARE_DIR + '/bioinfo/seq/vcflib-master/bin')
        self.set_environ(PATH=self.config.SOFTWARE_DIR + '/bioinfo/medical/picard-tools-2.2.4')
        self.java_path = self.config.SOFTWARE_DIR + '/program/sun_jdk1.8.0/bin/java'


    def run_Family2tab(self):
        fastq2tab_cmd = "{}dcpt_zml.sh {} {} {} {} {} {} {}".format(self.cmd_path, self.option("fastq"), self.option("cpu_number"),
                            self.option("ref_fasta").prop["path"], self.option("seq_path").prop['path'], self.option("targets_bedfile").prop['path']
                            ,self.config.SOFTWARE_DIR+'/bioinfo/medical/picard-tools-2.2.4/picard.jar', self.java_path)
        self.logger.info(fastq2tab_cmd)
        self.logger.info("开始运行转bam文件")
        cmd = self.add_command("fastq2tab_cmd", fastq2tab_cmd).run()
        self.wait(cmd)

        if cmd.return_code == None:
            self.logger.info("返回码问题，重新运行cmd")
            cmd = self.add_command("cmd", cmd).run()
            self.wait(cmd)

        if cmd.return_code == 0:
            self.logger.info("运行转tab文件成功")
        else:
            raise Exception("运行转tab文件出错")

    def set_output(self):
        """
        将结果文件link到output文件夹下面
        :return:
        """
        for root, dirs, files in os.walk(self.output_dir):
            for names in files:
                os.remove(os.path.join(root, names))
        self.logger.info("设置结果目录")
        file_path = self.option("seq_path").prop['path'] + "/"
        print file_path
        results = os.listdir(file_path)
        for f in results:
            if str(f) == "%s.mem.sort.hit.vcf.tab"%(self.option("fastq")) or str(f) == "%s.qc"%(self.option("fastq")):
            #if re.search(r'.*\.filter\.bam$', f) or re.search(r'.*\.qc$', f):
                shutil.move(file_path + f, self.output_dir)
        self.logger.info('设置文件夹路径成功')

        api = self.api.tab_file
        temp = os.listdir(self.output_dir)
        api_read_tab = self.api.tab_file  # 二次判断数据库中是否存在tab文件
        for i in temp:
            m = re.search(r'(.*)\.mem.*tab$', i)
            n = re.search(r'(.*)\.qc', i)
            if m:
                tab_path = self.output_dir + '/' + i
                tab_name = m.group(1)
                # tab_path = self.output_dir + '/' + i
                # tab_name = m.group(1)
                if not api_read_tab.tab_exist(tab_name):
                    api.add_pt_tab(tab_path, self.option('batch_id'))
                    api.add_sg_pt_tab_detail(tab_path)
                else:
                    raise Exception('可能样本重名，请检查！')
            elif n:
                tab_path = self.output_dir + '/' + i
                tab_name = n.group(1)
                if not api_read_tab.qc_exist(tab_name):
                    api.sample_qc_dc(tab_path, tab_name)
                    api.sample_qc_addition_dc(tab_name)
                else:
                    raise Exception('可能样本重名，请检查！')


    def run(self):
        super(Family2tabDcTool, self).run()
        self.run_Family2tab()
        self.set_output()
        self.end()
