# -*- coding: utf-8 -*-
# __author__ = 'wangzhaoyue'
import os
import shutil
from biocluster.core.exceptions import OptionError
from biocluster.agent import Agent
from biocluster.tool import Tool
import re


class CuffdiffAgent(Agent):
    """
    有参转录组cuffmerge合并
    version v1.0.1
    author: 
    last_modify: 2016.09.09
    """
    def __init__(self, parent):
        super(CuffdiffAgent, self).__init__(parent)
        options = [
            #{"name": "sample_bam", "type": "infile", "format": "ref_rna.assembly.bam"},  # 所有样本比对之后的bam文件
            {"name": "sample1_bam_dir", "type": "infile","format":"ref_rna.assembly.bam_dir"},  # 所有样本1的bam文件夹
            {"name": "sample2_bam_dir", "type": "infile","format":"ref_rna.assembly.bam_dir"},  # 所有样本2的bam文件夹
            {"name": "ref_fa", "type": "infile", "format": "sequence.fasta"},  # 参考基因文件
            {"name": "ref_gtf", "type": "infile", "format": "ref_rna.assembly.gtf"},  # 参考基因的注释文件
            {"name": "cpu", "type": "int", "default":10},  #cufflinks软件所分配的cpu数量
            {"name": "top_cuff_diff", "type": "infile", "format": "ref_rna.assembly.bam_dir"}
            #{"name": "", "type": "infile","format":"ref_rna.assembly.bam_dir"},  # 所有样本2的bam文件夹
            #{"name": "fr_stranded", "type": "string","default":"fr-unstranded"},  # 是否链特异性
            #{"name": "strand_direct", "type": "string","default":"none"}, # 链特异性时选择正负链
            #{"name": "sample_gtf", "type": "outfile","format":"ref_rna.assembly.gtf"},  # 输出的转录本文件
            #{"name": "sample_genes_fpkm", "type": "outfile", "format": "ref_rna.assembly.fpkm_tracking"},  # 输出的基因表达量文件
            #{"name": "sample_isoforms_fpkm", "type": "outfile", "format": "ref_rna.assembly.fpkm_tracking"},  # 输出的转录本文件
        ]
        self.add_option(options)
        self.step.add_steps("cuffdiff")
        self.on('start', self.stepstart)
        self.on('end', self.stepfinish)

    def stepstart(self):
        self.step.cuffdiff.start()
        self.step.update()

    def stepfinish(self):
        self.step.cuffdiff.finish()
        self.step.update()

    def check_options(self):
        """
        重写参数检测函数
        :return:
        """
        if not self.option('sample1_bam_dir'):
            raise OptionError('必须输入样本1文件为bam文件集')
        if not self.option('sample2_bam_dir'):
            raise OptionError('必须输入样本2文件为bam文件集')
        #if not self.option('ref_fa') :
        #    raise OptionError('必须输入参考序列ref.fa')
        if not self.option('ref_gtf'):
            raise OptionError('必须输入参考序列ref.gtf')
        #if self.option("fr_stranded") !="fr-unstranded" and not self.option("strand_direct").is_set:
        #    raise OptionError("当链特异性时必须选择正负链")
        return True

    def set_resource(self):
        """
        设置所需资源，需在之类中重写此方法 self._cpu ,self._memory
        :return:
        """
        self._cpu = 10
        self._memory = "100G"

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"],
        ])
        #result_dir.add_regexp_rules([
        #    ["_out.gtf", "gtf", "样本拼接之后的gtf文件"]
        #])
        super(CuffdiffAgent, self).end()


class CuffdiffTool(Tool):
    def __init__(self, config):
        super(CuffdiffTool, self).__init__(config)
        self._version = "v1.0.1"
        self.cuffdiff_path = '/bioinfo/rna/cufflinks-2.2.1/'

    def run(self):
        """
        运行
        :return:
        """
        super(CuffdiffTool, self).run()
        self.run_cuffdiff()
        self.set_output()
        self.end()

    def run_cuffdiff(self):
        """
        运行cuffdiff软件，进行拼接组装
        """
        f1 = ''
        for s in os.listdir(self.option('sample1_bam_dir').prop['path']):
            s = os.path.join(self.option('sample1_bam_dir').prop['path'],s)
            f1 = s+','+f1
        f1 = f1[:-1]
        f2 = ''
        for t in os.listdir(self.option('sample2_bam_dir').prop['path']):
            t = os.path.join(self.option('sample2_bam_dir').prop['path'],t)
            f2 = t+','+f2
        f2=f2[:-1]
        f_all = self.option('ref_gtf').prop['path']+'\t'+f1+'\t'+f2
        cmd = self.cuffdiff_path + ('cuffdiff -b %s -o %s -p 10 -L A,C -u %s' %(self.option('ref_fa').prop['path'], self.work_dir+"/out_cuff_diff", f_all))

        #if self.option('fr_stranded') =="fr-unstranded" :
        #     sample_name = os.path.basename(self.option('sample_bam').prop['path']).split('.bam')[0]
        #     cmd = self.cufflinks_path + ('cufflinks -p %s -g %s -b %s --library-type %s -o  ' % (self.option('cpu'), self.option('ref_gtf').prop['path'], self.option('ref_fa').prop['path'],self.option('fr_stranded')) + sample_name)+ ' %s' % (self.option('sample_bam').prop['path'])
        #else:
        #    if self.option('strand_direct')=="firststrand" :
        #         sample_name = os.path.basename(self.option('sample_bam').prop['path']).split('.bam')[0]
        #         cmd = self.cufflinks_path + ('cufflinks -p %s -g %s -b %s --library-type %s -o  ' % (self.option('cpu'), self.option('ref_gtf').prop['path'], self.option('ref_fa').prop['path'],self.option('strand_direct')) + sample_name) + ' %s' % (self.option('sample_bam').prop['path'])
        #    else:
        #        if self.option('strand_direct')=='secondstrand' :
        #             sample_name = os.path.basename(self.option('sample_bam').prop['path']).split('.bam')[0]
        #             cmd = self.cufflinks_path +( 'cufflinks -p %s -g %s -b %s --library-type %s -o  ' % (self.option('cpu'), self.option('ref_gtf').prop['path'],self.option('ref_fa').prop['path'], self.option('strand_direct'))+ sample_name) +' %s'%(self.option('sample_bam').prop['path'])
        self.logger.info('运行cuffdiff软件，进行组装拼接')
        command = self.add_command("cuffdiff_cmd", cmd).run()
        self.wait(command)
        if command.return_code == 0:
            self.logger.info("cuffdiff运行完成")
        else:
            self.set_error("cuffdiff运行出错!")
            
            
    def set_output(self):
        self.logger.info("设置结果目录")
        for root,dirs,files in os.walk(self.output_dir):
            for file in files:
                os.remove(os.path.join(self.output_dir,file))
        results = os.listdir(self.work_dir)
        # try:
        #    shutil.copy2(self.work_dir + "/top_cuff_diff", self.output_dir + "/top_cuff_diff")
        #    self.option('/top_cuff_diff').set_path(self.work_dir + "/top_cuff_diff")
        #    self.logger.info("分析结果目录成功")

        #except Exception as e:
        #    self.logger.info("分析结果目录失败{}".format(e))
        #    self.set_error("分析结果目录失败{}".format(e))

            
