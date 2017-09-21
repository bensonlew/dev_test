# -*- coding: utf-8 -*-
# __author__ = 'guhaidong'

import os
import shutil
from biocluster.core.exceptions import OptionError
from biocluster.agent import Agent
from biocluster.tool import Tool

class MegahitAgent(Agent):
    """
    进行megahit拼接
    version: v1.0
    author: guhaidong
    last_modify: 2017.09.08
    """
    def __init__(self, parent):
        super(MegahitAgent, self).__init__(parent)
        options = [
            {"name": "fastq1", "type": "infile", "format": "sequence.fastq"},  # 输入文件,l
            {"name": "fastq2", "type": "infile", "format": "sequence.fastq"},  # 输入文件,r
            {"name": "fastqs", "type": "infile", "format": "sequence.fastq"},  # 输入文件,s 可不传
            {"name": "sample_name", "type": "string"},  #输入样品名
            {"name": "cpu", "type": "int", "default": 5},  # 拼接线程数，默认5
            {"name": "mem", "type": "int", "default": 10},  # 拼接使用内存，默认10
            {"name": "mem_mode", "type": "string", "default": "mem"},
            # 拼接内存模式,mem表示根据'mem'参数，minimum表示最小内存，moderate表示普通内存
            {"name": "min_contig", "type": "int", "default": 300},   # 最短contig值
            {"name": "mink", "type": "int", "default": 47},  # 最小kmer值
            {"name": "maxk", "type": "int", "default": 97},  # 最大kmer值
            {"name": "step", "type": "int", "default": 10},  # kmer步长
            {"name": "contig", "type": "outfile", "format": "sequence.fasta"},  # 输出文件,sample.contig.fa
        ]
        self.add_option(options)
        self.step.add_steps("Megahit")
        self.on('start', self.stepstart)
        self.on('end', self.stepfinish)

    def stepstart(self):
        self.step.Megahit.start()
        self.step.update()

    def stepfinish(self):
        self.step.Megahit.finish()
        self.step.update()

    def check_options(self):
        """
        检查参数
        :return:
        """
        if not self.option('fastq1'):
            raise OptionError('必须输入fastq1')
        if not self.option('fastq2'):
            raise OptionError('必须输入fastq2')
        if not self.option('sample_name'):
            raise OptionError('必须输入样本名')
        if self.option('mem_mode') not in ['mem', 'minimum', 'moderate']:
            raise OptionError('内存模式错误，选择[mem|minimum|moderate]之一')
        return True

    def set_resource(self):
        """
        设置所需资源，需在之类中重写此方法 self._cpu ,self._memory
        :return:
        """
        self._cpu = self.option('cpu')
        self._memory = "{}G".format(self.option('mem'))

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"],
        ])
        result_dir.add_regexp_rules([
            ["", "", ""]
        ])
        super(MegahitAgent, self).end()


class MegahitTool(Tool):
    def __init__(self, config):
        super(MegahitTool, self).__init__(config)
        self.gcc = self.config.SOFTWARE_DIR + '/gcc/5.1.0/bin'
        self.gcc_lib = self.config.SOFTWARE_DIR + '/gcc/5.1.0/lib64'
        self.set_environ(PATH=self.gcc, LD_LIBRARY_PATH=self.gcc_lib)
        self.megahit_path = '/bioinfo/metaGenomic/megahit/'

    def megahit_run(self):
        """
        进行megahit拼接
        :return:
        """
        if os.path.exists(self.output_dir + '/' + self.option('sample_name') + '.contig.fa'):
            return
        if os.path.exists(self.work_dir + '/run'):
            shutil.rmtree(self.work_dir + '/run')
        cmd = self.megahit_path + 'megahit -1 %s -2 %s '\
           % (self.option('fastq1').prop['path'], self.option('fastq2').prop['path'],)
        if self.option('fastqs'):
            cmd += '-r %s ' % (self.option('fastqs').prop['path'])
        cmd += '-o %s --k-min %s --k-max %s --k-step %s --min-contig-len %s '\
            % (self.work_dir + '/run',
            self.option('mink'),
            self.option('maxk'),
            self.option('step'),
            self.option('min_contig'))
        if self.option('mem_mode') == 'mem':
            member_byte_format = self.option('mem') * 1000000000
            cmd += '-m %s' % (member_byte_format)
        elif self.option('mem_mode') == 'minimum':
            mem_mode_code = 0
            cmd += '--mem-flag %s' % (mem_mode_code)
        elif self.option('mem_mode') == 'moderate':
            mem_mode_code = 1
            cmd += '--mem-flag %s' % (mem_mode_code)
        self.logger.info("运行megahit拼接")
        command = self.add_command("megahit", cmd)
        command.run()
        self.wait(command)
        if command.return_code == 0:
            self.logger.info("运行megahit完成")
        else:
            self.set_error("megahit运行出错!")

    def set_output(self):
        """
        将结果文件复制到output文件夹下面
        :return:
        """
        self.logger.info("设置结果目录")
        out_fa = self.output_dir + '/' + self.option('sample_name') + '.contig.fa'
        if os.path.exists(out_fa):
            os.remove(out_fa)
        os.link(self.work_dir + '/run/final.contigs.fa', out_fa)
        self.option('contig').set_path(out_fa)
        self.logger.info("设置megahit分析结果目录成功")

    def run(self):
        """
        运行
        :return:
        """
        super(MegahitTool, self).run()
        self.megahit_run()
        self.set_output()
        self.end()