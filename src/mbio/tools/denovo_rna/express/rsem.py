# -*- coding: utf-8 -*-
# __author__ = 'qiuping'
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
import os
import re

class RsemAgent(Agent):
    """
    调用align_and_estimate_abundance.pl脚本，运行rsem，进行表达量计算分析
    version v1.0
    author: qiuping
    last_modify: 2016.06.20
    """
    def __init__(self, parent):
        super(RsemAgent, self).__init__(parent)
        options = [
            {"name": "fq_type", "type": "string"}, # PE OR SE
            {"name": "rsem_fa", "type": "infile", "format": "sequence.fasta"},  #trinit.fasta文件
            {"name": "fq_l", "type": "infile", "format": "sequence.fastq"},  # PE测序，包含所有样本的左端fq文件的文件夹
            {"name": "fq_r", "type": "infile", "format": "sequence.fastq"},  # PE测序，包含所有样本的左端fq文件的文件夹
            {"name": "fq_s", "type": "infile", "format": "sequence.fastq"}  # SE测序，包含所有样本的fq文件的文件夹
        ]
        self.add_option(options)
        self.step.add_steps("rsem")
        self.on('start', self.stepstart)
        self.on('end', self.stepfinish)

    def stepstart(self):
        self.step.rsem.start()
        self.step.update()

    def stepfinish(self):
        self.step.rsem.finish()
        self.step.update()

    def check_options(self):
        """
        重写参数检测函数
        :return:
        """
        if not self.option('fq_type'):
            raise OptionError('必须设置测序类型：PE OR SE')
        if self.option('fq_type') not in ['PE', 'SE']:
            raise OptionError('测序类型不在所给范围内')
        if not self.option("fq_l").is_set and not self.option("fq_r").is_set and not self.option("fq_s").is_set:
            raise OptionError("必须设置PE测序输入文件或者SE测序输入文件")
        if self.option("fq_type") == "PE" and not self.option("fq_r").is_set and not self.option("fq_l").is_set:
            raise OptionError("PE测序时需设置左端序列和右端序列输入文件")
        if self.option("fq_type") == "SE" and not self.option("fq_s").is_set:
            raise OptionError("SE测序时需设置序列输入文件")
        return True

    def set_resource(self):
        """
        设置所需资源，需在之类中重写此方法 self._cpu ,self._memory
        :return:
        """
        self._cpu = 10
        self._memory = ''

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"]
        ])
        result_dir.add_regexp_rules([
            [r"results$", "xls", "rsem结果"]
            ])
        super(RsemAgent, self).end()


class RsemTool(Tool):
    """
    Lefse tool
    """
    def __init__(self, config):
        super(RsemTool, self).__init__(config)
        self._version = '1.0.1'
        self.fpkm = "/bioinfo/rna/scripts/abundance_estimates_to_matrix.pl"
        self.tpm = "/bioinfo/rna/trinityrnaseq-2.2.0/util/abundance_estimates_to_matrix.pl"
        self.rsem = "/bioinfo/rna/trinityrnaseq-2.2.0/util/align_and_estimate_abundance.pl"
        self.rsem_path = self.config.SOFTWARE_DIR + '/bioinfo/rna/RSEM-1.2.31/bin'
        self.bowtie_path = self.config.SOFTWARE_DIR + '/bioinfo/align/bowtie2-2.2.9/'
        self.set_environ(PATH=self.rsem_path)
        self.set_environ(PATH=self.bowtie_path)

    def run_rsem(self):
        if self.option('fq_type') == 'SE':
            sample = os.path.basename(self.option('fq_s').prop['path']).split('_s.fq')[0]
            rsem_cmd = self.rsem + ' --transcripts %s --seqType fq --single %s --est_method  RSEM --output_dir %s --thread_count 6 --trinity_mode --prep_reference --aln_method bowtie2 --output_prefix %s' % (self.option('rsem_fa').prop['path'], self.option('fq_s').prop['path'], self.work_dir, sample)
        else:
            sample = os.path.basename(self.option('fq_l').prop['path']).split('_l.fq')[0]
            rsem_cmd = self.rsem + ' --transcripts %s --seqType fq --right %s --left %s --est_method  RSEM --output_dir %s --thread_count 6 --trinity_mode --prep_reference --aln_method bowtie2 --output_prefix %s' % (self.option('rsem_fa').prop['path'], self.option('fq_r').prop['path'], self.option('fq_l').prop['path'], self.work_dir, sample)
        self.logger.info("开始运行_rsem_cmd")
        cmd = self.add_command("rsem_cmd", rsem_cmd).run()
        self.wait()
        if cmd.return_code == 0:
            self.logger.info("%s运行完成" % cmd)
        else:
            self.set_error("%s运行出错!" % cmd)

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
        try:
            for f in results:
                if re.search(r'results$', f):
                    os.link(self.work_dir + '/' + f, self.output_dir + '/' + f)
            self.logger.info("设置rsem分析结果目录成功")
        except Exception as e:
            self.logger.info("设置rsem分析结果目录失败{}".format(e))

    def run(self):
        super(RsemTool, self).run()
        self.run_rsem()
        self.set_output()
        self.end()
