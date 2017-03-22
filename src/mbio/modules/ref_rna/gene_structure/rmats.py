# -*- coding: utf-8 -*-

# __author__ = 'linfang.jin'
# time: 2017/1/22 17:46


# -*- coding: utf-8 -*-

# __author__ = 'linfang.jin'
# time: 2017/1/16 15:47
import importlib
import os
import shutil
from biocluster.core.exceptions import OptionError
from biocluster.module import Module
from mbio.packages.ref_rna.trans_step import *
import re
from mbio.files.sequence.file_sample import FileSampleFile


class RmatsModule(Module):
    '''
    '''

    def __init__(self, work_id):
        super(RmatsModule, self).__init__(work_id)
        options = [
            {"name": "sample_bam_dir", "type": "infile", "format": "ref_rna.assembly.bam_dir"},
            {"name": "rmats_control", "type": "infile", "format": "denovo_rna.express.control_table"},
            {"name": "group_table", "type": "infile", "format": "meta.otu.group_table"},
            {"name": "gname", "type": "string", "default": "group"},  # 分组方案名称
            {"name": "seq_type", "type": "string", "default": "paired"},  # 两个选项：'paired'  or ’single‘
            {"name": "analysis_mode", "type": "string", "default": "P"},
            {"name": "read_length", "type": "int", "default": 150},
            {"name": "ref_gtf", "type": "infile", "format": "ref_rna.assembly.gtf"},  # 一定要设置
            {"name": "novel_as", "type": "int", "default": 1},  # 是否发现新的AS事件，默认为是
            {"name": "lib_type", "type": "string", "default": "fr-unstranded"},  # 建库类型
            {"name": "as_diff", "type": "float", "default": 0.05},
        ]

        self.add_option(options)
        self.rmats_bam_tools = []

    def check_options(self):
        if self.option('sample_bam_dir') is None:
            raise OptionError("必须设置bam文件夹")
        if not self.option('ref_gtf'):
            raise OptionError('必须设置参考基因组注释文件（ref_genome.gtf）')
        if self.option('as_diff') < 0 or self.option('as_diff') >= 1:
            raise OptionError('差异剪接假设检验的置信度p应该: 0=< p <1')
        if self.option('lib_type') not in ('fr-unstranded', 'fr-firststrand', 'fr-secondstrand'):
            raise OptionError('rMATS识别的建库类型只可设置为fr-unstranded或fr-firststrand或fr-secondstrand')
        if self.option('seq_type') not in ('paired', 'single'):
            raise OptionError('rMATS识别的测序读长类型只可为paired（双端测序）或single（单端测序）')
        if self.option('novel_as') not in (1, 0):
            raise OptionError('是否设置发现新的AS事件，参数范围应为（1,0），其中1为是，0为否')
        if self.option('group_table').is_set and not self.option('gname'):
            raise OptionError("有分组文件时必须传入分组方案名字")
        if self.option('group_table').is_set and self.option('gname') not in self.option('group_table').prop['group_scheme']:
            raise OptionError("传入分组方案名字不在分组文件内")
        if not self.option('rmats_control').is_set:
            raise OptionError("必须设置输入文件：上下调对照组参考文件")

        return True

    def finish_update(self, event):
        step = getattr(self.step, event['data'])
        step.finish()
        self.step.update()

    # def link_ref(self):
    #     ref_gtf = self.option('ref_gtf').path
    #     self.ref_gtf_link = self.work_dir + "/" + os.path.basename(ref_gtf)
    #     self.logger.info(self.ref_gtf_link)
    #     if os.path.exists(self.ref_gtf_link):
    #         os.remove(self.ref_gtf_link)
    #     os.symlink(ref_gtf, self.ref_gtf_link)

    def get_list(self):
        list_path = os.path.join(self.option("sample_bam_dir").path, "list.txt")
        file_sample = FileSampleFile()
        file_sample.set_path(list_path)
        samples = file_sample.get_list()
        return samples

    def get_group_str(self):
        '''

        :return: [('a1_1.bam,a1_2.bam', 'b1_1.bam,b1_2.bam'), ('a2_1.bam,a2_2.bam', 'b2_1.bam,b2_2.bam'), ......]
        '''
        a_b_bam_path_tuple_lst = []
        sample_bams = [f for f in os.listdir(self.option('sample_bam_dir').path) if re.match(r'.*\.bam$', f)]
        sample_path_dic = {}
        for sample_bam in sample_bams:
            m = re.match(r'(\S+?)\.\S+\.bam$', sample_bam)
            sample_name = m.group(1)
            '''
            字典格式为：{sample_name: sample_bam_abs_path}
            '''
            sample_path_dic[sample_name] = os.path.join(self.option('sample_bam_dir').path, sample_bam)
        num, vs_list = self.option('rmats_control').get_control_info()
        group_sample_dic = self.option(
            'group_table').get_group_spname()  #:return group_spname:该分组方案下分组类别对应的样本信息详细的字典，eg：{'A': [1,2,3], 'B': [4,5,6]}
        for vs_pair in vs_list:
            a_group_name = vs_pair[0]
            b_group_name = vs_pair[1]
            a_group_samples = group_sample_dic[a_group_name]
            a_group_path_lst = []
            b_group_path_lst = []
            b_group_samples = group_sample_dic[b_group_name]
            for a_group_sample in a_group_samples:
                a_group_path_lst.append(sample_path_dic[a_group_sample])
            for b_group_sample in b_group_samples:
                b_group_path_lst.append(sample_path_dic[b_group_sample])
            a_group_path_str = ','.join(a_group_path_lst)
            b_group_path_str = ','.join(b_group_path_lst)
            a_b_bam_path_tuple_lst.append((a_group_path_str, b_group_path_str))
        return a_b_bam_path_tuple_lst

    def multi_rmats_bam_run(self):
        vs_group_path_pair_lst = self.get_group_str()
        n = 0
        for vs_pair in vs_group_path_pair_lst:
            rmats_bam = self.add_tool('ref_rna.gene_structure.rmats_bam')
            n = n + 1
            self.step.add_steps('rmats_bam_{}'.format(n))
            rmats_bam.set_options({
                "ref_gtf": self.option("ref_gtf").path,
                "seq_type": self.option('seq_type'),
                "analysis_mode": self.option('analysis_mode'),
                "read_length": self.option('read_length'),
                "A_group_bam": vs_pair[0],
                "B_group_bam": vs_pair[1],
                "novel_as": self.option('novel_as'),
                "lib_type": self.option('lib_type'),
                "as_diff": self.option('as_diff'),
                "output_dir": rmats_bam.output_dir,
            })

            step = getattr(self.step, 'rmats_bam_{}'.format(n))
            step.start()
            """绑定下一个将要运行的步骤"""
            rmats_bam.on('end', self.finish_update, 'rmats_bam_{}'.format(n))
            rmats_bam.on('end', self.set_output, 'rmats_bam_{}'.format(n))
            self.rmats_bam_tools.append(rmats_bam)
        for tool in self.rmats_bam_tools:
            tool.run()

    def set_output(self):
        self.logger.info('set output')
        for rmats_bam_tool in self.rmats_bam_tools:
            output_dir = os.path.join(self.output_dir,rmats_bam_tool.name)
            if not os.path.exists(output_dir):
                os.mkdir(output_dir)
            outfiles = os.listdir(rmats_bam_tool.output_dir)
            for f in outfiles:
                f_path = os.path.join(rmats_bam_tool.output_dir, f)
                target = os.path.join(output_dir, f)
                os.symlink(f_path, target)
        self.logger.info("set output done")
        self.end()

    def run(self):
        self.multi_rmats_bam_run()
        super(RmatsModule, self).run()

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [r'.', '', 'rmats结果输出目录']
        ]
        )
        result_dir.add_regexp_rules([
            ["ASevents$", '', '样本非冗余可变剪接事件详情表'],
            ["MATS_output$", '', 'MATS结果目录'],
            ['commands.txt$', '', 'rMATS运行过程指令'],
            ['config.txt', '', '配置详情文件']
        ])
        super(RmatsModule, self).end()
