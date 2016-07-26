# -*- coding: utf-8 -*-
# __author__ = 'qiuping'

"""无参转录组基础分析"""

from biocluster.workflow import Workflow
from biocluster.core.exceptions import OptionError
import os
import shutil

class DenovoBaseWorkflow(Workflow):
    def __init__(self, wsheet_object):
        """
        """
        self._sheet = wsheet_object
        super(DenovoBaseWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "fastq_dir", "type": "infile", 'format': "sequence.fastq,sequence.fastq_dir"},  # fastq文件夹
            {"name": "fq_type", "type": "string"}, # PE OR SE
            {"name": "group_file", "type": "infile", "format": "meta.otu.group_table"},  # 有生物学重复的时候的分组文件
            {"name": "control_file", "type": "infile", "format": "denovo_rna.express.control_table"},  #对照组文件，格式同分组文件
            {"name": "sickle_dir", "type": "outfile", "format": "sequence.fastq_dir"},  # 质量剪切输出结果文件夹(包括左右段)
            {"name": "sickle_r_dir", "type": "outfile", "format": "sequence.fastq_dir"},  # 质量剪切右端输出结果文件夹
            {"name": "sickle_l_dir", "type": "outfile", "format": "sequence.fastq_dir"},  # 质量剪切左端输出结果文件夹
            {"name": "fq_s", "type": "outfile", "format": "sequence.fastq"},  # SE所有样本集合
            {"name": "fq_r", "type": "outfile", "format": "sequence.fastq"},  # PE所有右端序列样本集合
            {"name": "fq_l", "type": "outfile", "format": "sequence.fastq"},  # PE所有左端序列样本集合

        ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.filecheck = self.add_tool("denovo_rna.filecheck.file_denovo")
        self.qc = self.add_module("denovo_rna.qc.denovo_qc")
        self.assemble = self.add_tool("denovo_rna.assemble.assemble")
        self.step.add_steps("qcstat", "assemble", "annotation", "express", "gene_structure","map_stat")

    def check_options(self):
        """
        检查参数设置
        """
        if not self.option('fastq_dir').is_set:
            raise OptionError('必须设置输入fastq文件夹')
        if not self.option('control_file').is_set:
            raise OptionError('必须设置输入对照方案文件')
        if not self.option('fq_type'):
            raise OptionError('必须设置测序类型：PE OR SE')
        if self.option('fq_type') not in ['PE', 'SE']:
            raise OptionError('测序类型不在所给范围内')

    def set_step(self, event):
        if 'start' in event['data'].keys():
            event['data']['start'].start()
        if 'end' in event['data'].keys():
            event['data']['end'].finish()
        self.step.update()

    def run_filecheck(self):
        opts = {
                'fastq_dir': self.option('fastq_dir'),
                'fq_type': self.option('fq_type'),
                'control_file': self.option('control_file')
                }
        if self.option('group_file').is_set:
            opts.update({'group_file': self.option('group_file')})
        self.filecheck.set_options(opts)
        self.filecheck.run()

    def run_qc(self):
        self.qc.set_options({
            'fastq_dir': self.option('fastq_dir'),
            'fq_type': self.option('fq_type')
        })
        self.qc.on('end', self.set_output, 'qc')
        self.qc.on('start', self.set_step, {'start': self.step.qcstat})
        self.qc.on('end', self.set_step, {'end': self.step.qcstat})
        self.qc.run()

    def run_assemble(self):
        pass

    def move2outputdir(self, olddir, newname, mode='link'):
        """
        移动一个目录下的所有文件/文件夹到workflow输出文件夹下，如果文件夹名已存在，文件夹会被完整删除。
        """
        if not os.path.isdir(olddir):
            raise Exception('需要移动到output目录的文件夹不存在。')
        newdir = os.path.join(self.output_dir, newname)
        if os.path.exists(newdir):
            if os.path.islink(newdir):
                os.remove(newdir)
            else:
                shutil.rmtree(newdir)  # 不可以删除一个链接
        if mode == 'link':
            shutil.copytree(olddir, newdir, symlinks=True)
        elif mode == 'copy':
            shutil.copytree(olddir, newdir)
        else:
            raise Exception('错误的移动文件方式，必须是\'copy\'或者\'link\'')

    def set_output(self, event):
        obj = event["bind_object"]
        # 设置qc报告文件
        self.option('sickle_dir', obj.option('sickle_dir'))
        if event['data'] == 'qc':
            if self.option('fq_type') == 'SE':
                self.option('fq_s', obj.option('fq_s'))
            else:
                self.option('sickle_l_dir', obj.option('sickle_l_dir'))
                self.option('sickle_r_dir', obj.option('sickle_r_dir'))
                self.option('fq_r', obj.option('fq_r'))
                self.option('fq_l', obj.option('fq_l'))
            self.move2outputdir(obj.output_dir, self.output_dir + 'QC_stat')
