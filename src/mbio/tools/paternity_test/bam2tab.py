## !/mnt/ilustre/users/sanger-dev/app/program/Python/bin/python
# -*- coding: utf-8 -*-
# __author__ = "hongdongxuan"
#last_modify:20161125

from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
import os
import re
import shutil


class Bam2tabAgent(Agent):
    """
    调用bam2tab.sh脚本，完成将bam文件转换成*.mem.sort.hit.vcf.tab文件
    version v1.0
    author: hongdongxuan
    last_modify: 2016.11.25
    """
    def __init__(self, parent):
        super(Bam2tabAgent, self).__init__(parent)
        options = [
            {"name": "sample_id", "type": "string"}, #输入F/M/S的样本ID
            {"name": "bam_dir", "type": "string"},  #bam文件路径
            {"name": "ref_fasta", "type": "infile", "format": "sequence.fasta"},  # 参考序列
            {"name": "targets_bedfile", "type": "infile","format":"denovo_rna.gene_structure.bed"} #位点信息
        ]
        self.add_option(options)
        self.step.add_steps("Bam2tab")
        self.on('start', self.stepstart)
        self.on('end', self.stepfinish)

    def stepstart(self):
        self.step.Bam2tab.start()
        self.step.update()

    def stepfinish(self):
        self.step.Bam2tab.finish()
        self.step.update()


    def check_options(self):
        """
        重写参数检测函数
        :return:
        """
        if not self.option("sample_id"):
            raise OptionError("必须输入样本编号")
        if not self.option("bam_dir"):
            raise OptionError("必须输入bam文件的所在路径")
        if not self.option("ref_fasta").is_set:
            raise OptionError("必须输入参考基因组序列fasta文件")
        if not self.option('targets_bedfile'):
            raise OptionError('必须提供target_bedfile文件')
        return True

    def set_resource(self):
        """
        设置所需资源，需在之类中重写此方法 self._cpu ,self._memory
        :return:
        """
        self._cpu = 10
        self._memory = '10G'

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"],
                    ])
        result_dir.add_regexp_rules([
            [r".mem.sort.hit.vcf.tab", "tab", "所有位点的信息"],
            [r".qc", "qc", "质控文件"]
        ])
        super(Bam2tabAgent, self).end()


class Bam2tabTool(Tool):
    """
    运行脚本：bam2tab.sh sample_id bam_dir ref targets_bedfile
    """
    def __init__(self, config):
        super(Bam2tabTool, self).__init__(config)
        self._version = '1.0.1'
        self.cmd_path = "bioinfo/medical/scripts/bam2tab.sh"
        self.set_environ(LD_LIBRARY_PATH=self.config.SOFTWARE_DIR + '/gcc/5.4.0/lib64')
        self.set_environ(PATH=self.config.SOFTWARE_DIR + '/gcc/5.4.0/bin')
        self.set_environ(PATH=self.config.SOFTWARE_DIR + '/program/ruby-2.3.1')
        self.set_environ(PATH=self.config.SOFTWARE_DIR + '/program/lib/ruby/gems/2.3.0/gems/bio-vcf-0.9.2/bin')
        self.set_environ(PATH=self.config.SOFTWARE_DIR + '/bioinfo/seq/bioawk')
        self.set_environ(PATH=self.config.SOFTWARE_DIR + '/bioinfo/seq/seqtk-master')
        self.set_environ(PATH=self.config.SOFTWARE_DIR + '/bioinfo/medical/bwa-0.7.15/bin')
        self.set_environ(PATH=self.config.SOFTWARE_DIR + '/bioinfo/medical/samblaster-0.1.22/bin')
        self.set_environ(PATH=self.config.SOFTWARE_DIR + '/bioinfo/align/samtools-1.3.1')
        self.set_environ(PATH=self.config.SOFTWARE_DIR + '/bioinfo/medical/bedtools-2.24.0/bin')
        self.set_environ(PATH=self.config.SOFTWARE_DIR + '/program/sun_jdk1.8.0/bin')
        self.set_environ(PATH=self.config.SOFTWARE_DIR + '/bioinfo/medical/bcftools-1.3.0/bin')
        self.set_environ(PATH=self.config.SOFTWARE_DIR + '/bioinfo/seq/vt-master')
        self.set_environ(PATH=self.config.SOFTWARE_DIR + '/bioinfo/seq/vcflib-master/bin')

    def run_Bam2tab(self):

        Bam2tab_cmd = self.cmd_path + " %s %s %s %s" % (self.option("sample_id"), self.option("bam_dir"),
                                                     self.option("ref_fasta").prop["path"], self.option("targets_bedfile").prop['path'])
        print Bam2tab_cmd
        self.logger.info(Bam2tab_cmd)
        self.logger.info("开始运行cmd")
        cmd = self.add_command("cmd", Bam2tab_cmd).run()
        self.wait(cmd)
        if cmd.return_code == 0:
            self.logger.info("运行Bam2tab成功")
        else:
            self.logger.info("运行Bam2tab出错")

    def set_output(self):
        """
        将结果文件link到output文件夹下面
        :return:
        """
        for root, dirs, files in os.walk(self.output_dir):
            for names in files:
                os.remove(os.path.join(root, names))
        self.logger.info("设置结果目录")
        file_path = self.option("bam_dir") + "/"
        print file_path
        results = os.listdir(file_path)
        for f in results:
            if re.search(r'.*mem\.sort\.hit\.vcf\.tab$', f) or re.search(r'.*\.qc$', f):
                shutil.move(file_path + f, self.output_dir)
            else:
                pass
        self.logger.info('设置文件夹路径成功')

    def run(self):
        super(Bam2tabTool, self).run()
        self.run_Bam2tab()
        self.set_output()
        self.end()