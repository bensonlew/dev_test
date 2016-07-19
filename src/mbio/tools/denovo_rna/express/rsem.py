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
            {"name": "rsem_bam", "type": "infile", "format": "align.bwa.bam,align.bwa.bam_dir"},  # 输入文件，bam格式的比对文件
            {"name": "rsem_fa", "type": "infile", "format": "sequence.fasta"},  #trinit.fasta文件
            {"name": "fq_l", "type": "infile", "format": "sequence.fastq, sequence.fastq_dir"},  # PE测序，包含所有样本的左端fq文件的文件夹
            {"name": "fq_r", "type": "infile", "format": "sequence.fastq, sequence.fastq_dir"},  # PE测序，包含所有样本的左端fq文件的文件夹
            {"name": "fq_s", "type": "infile", "format": "sequence.fastq, sequence.fastq_dir"},  # SE测序，包含所有样本的fq文件的文件夹
            {"name": "tran_count", "type": "outfile", "format": "denovo_rna.express.express_matrix"},
            {"name": "gene_count", "type": "outfile", "format": "denovo_rna.express.express_matrix"},
            {"name": "tran_fpkm", "type": "outfile", "format": "denovo_rna.express.express_matrix"},
            {"name": "gene_fpkm", "type": "outfile", "format": "denovo_rna.express.express_matrix"},
            {"name": "exp_way", "type": "string", "default": "fpkm"}
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
        # print self.option('fq_s'), self.option("fq_l"), self.option("fq_r")
        if not self.option("fq_l") and not self.option("fq_r") and not self.option("fq_s"):
            raise OptionError("必须设置PE测序输入文件或者SE测序输入文件")
        if self.option("fq_l") and self.option("fq_r") and self.option("fq_s"):
            raise OptionError("不能同时设置PE测序输入文件和SE测序输入文件的参数")
        if self.option("fq_l") and not self.option("fq_r"):
            raise OptionError("要同时设置PE测序左端fq和右端fq，缺少右端fq")
        if not self.option("fq_l") and self.option("fq_r"):
            raise OptionError("要同时设置PE测序左端fq和右端fq，缺少左端fq")
        if self.option("exp_way") not in ['fpkm', 'tpm']:
            raise OptionError("所设表达量的代表指标不在范围内，请检查")
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
            [r"results$", "xls", "rsem结果"],
            [r"matrix$", "xls", "表达量矩阵"]
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
        self.tpm = "/bioinfo/seq/trinityrnaseq-2.1.1/util/abundance_estimates_to_matrix.pl"
        self.rsem = "/bioinfo/seq/trinityrnaseq-2.1.1/util/align_and_estimate_abundance.pl"

    def fq_bam(self, bamdir):
        bamfiles = os.listdir(bamdir)
        fq_bam = {}
        for bam in bamfiles:
            sample = bam.split('.')[0]
            if self.option('fq_s'):
                # fq_bam[sample] = [bam, sample + '_sickle_s.fq']
                fq_bam[sample] = [bam, sample + '.fq']
            else:
                # fq_bam[sample] = [bam, sample + '_sickle_r.fq', sample + '_sickle_l.fq']
                fq_bam[sample] = [bam, sample + '_r.fq', sample + '_l.fq']
        return fq_bam

    def run_rsem(self):
        if self.option('rsem_bam').format == "align.bwa.bam_dir":
            fq_bam = self.fq_bam(self.option('rsem_bam').prop['path'])
            comm_list = []
            for sample in fq_bam.keys():
                os.link(self.option('rsem_bam').prop['path'] + '/' + fq_bam[sample][0], self.work_dir + '/' + fq_bam[sample][0])
                if self.option('fq_s'):
                    rsem_cmd = self.rsem + ' --transcripts %s --seqType fq --single %s --est_method  RSEM --output_dir %s --thread_count 6 --trinity_mode --prep_reference --aln_method %s --output_prefix %s' % (self.option('rsem_fa').prop['path'], self.option('fq_s').prop['path'] + '/' + fq_bam[sample][1], self.work_dir, fq_bam[sample][0], sample)
                else:
                    rsem_cmd = self.rsem + ' --transcripts %s --seqType fq --right %s --left %s --est_method  RSEM --output_dir %s --thread_count 6 --trinity_mode --prep_reference --aln_method %s --output_prefix %s' % (self.option('rsem_fa').prop['path'], self.option('fq_r').prop['path'] + '/' + fq_bam[sample][1], self.option('fq_r').prop['path'] + '/' + fq_bam[sample][2], self.work_dir, fq_bam[sample][0], sample)
                self.logger.info("开始运行%s_rsem_cmd" % sample)
                cmd = str(("%srsem_cmd" % sample).lower())
                self.logger.info(rsem_cmd)
                comm_list.append(self.add_command(cmd, rsem_cmd).run())
            self.wait()
            for cmd in comm_list:
                if cmd.return_code == 0:
                    self.logger.info("%s运行完成" % cmd)
                else:
                    self.set_error("%s运行出错!" % cmd)
                    raise Exception("%s运行出错!" % cmd)

    def merge_rsem(self):
        files = os.listdir(self.work_dir)
        if self.option('exp_way') == 'fpkm':
            merge_gene_cmd = self.fpkm + ' --est_method RSEM --out_prefix genes '
            merge_tran_cmd = self.fpkm + ' --est_method RSEM --out_prefix transcripts '
        else:
            merge_gene_cmd = self.tpm + ' --est_method RSEM --out_prefix genes '
            merge_tran_cmd = self.tpm + ' --est_method RSEM --out_prefix transcripts '
        for f in files:
            if re.search(r'genes\Wresults$', f):
                merge_gene_cmd += '%s ' % f
            elif re.search(r'isoforms\Wresults$', f):
                merge_tran_cmd += '%s ' % f
        self.logger.info(merge_tran_cmd)
        self.logger.info(merge_gene_cmd)
        self.logger.info("开始运行merge_gene_cmd")
        self.logger.info("开始运行merge_tran_cmd")
        gene_com = self.add_command("merge_gene_cmd", merge_gene_cmd).run()
        self.wait(gene_com)
        if gene_com.return_code == 0:
            self.logger.info("运行merge_gene_cmd成功")
        else:
            self.logger.info("运行merge_gene_cmd出错")
            raise Exception("运行merge_gene_cmd出错")
        tran_com = self.add_command("merge_tran_cmd", merge_tran_cmd).run()
        self.wait(tran_com)
        if tran_com.return_code == 0:
            self.logger.info("运行merge_tran_cmd成功")
        else:
            self.logger.info("运行merge_tran_cmd出错")
            raise Exception("运行merge_tran_cmd出错")
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
                elif re.search(r'^(transcripts\.TMM)(.+)(matrix)$', f):
                    os.link(self.work_dir + '/' + f, self.output_dir + '/' + f)
                    self.option('tran_fpkm').set_path(self.output_dir + '/' + f)
                elif re.search(r'^(genes\.TMM)(.+)(matrix)$', f):
                    os.link(self.work_dir + '/' + f, self.output_dir + '/' + f)
                    self.option('gene_fpkm').set_path(self.output_dir + '/' + f)
                elif re.search(r'^(transcripts\.count)(.+)(matrix)$', f):
                    os.link(self.work_dir + '/' + f, self.output_dir + '/' + f)
                    self.option('tran_count').set_path(self.output_dir + '/' + f)
                elif re.search(r'^(genes\.count)(.+)(matrix)$', f):
                    os.link(self.work_dir + '/' + f, self.output_dir + '/' + f)
                    self.option('gene_count').set_path(self.output_dir + '/' + f)
            self.logger.info("设置rsem分析结果目录成功")
        except:
            self.logger.info("设置rsem分析结果目录失败")

    def run(self):
        super(RsemTool, self).run()
        self.run_rsem()
        self.merge_rsem()
        self.set_output()
        self.end()
