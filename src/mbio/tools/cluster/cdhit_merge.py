# -*- coding: utf-8 -*-
# __author__ = 'zouxuan
from biocluster.agent import Agent
from biocluster.tool import Tool
from mbio.files.sequence.fasta import FastaFile
import os
import subprocess
from biocluster.core.exceptions import OptionError
import shutil


class CdhitMergeAgent(Agent):
    """
    version v1.0
    author: zouxuan
    last modified:2017.8.18
    """

    def __init__(self, parent):
        super(CdhitMergeAgent, self).__init__(parent)
        options = [
            {"name": "compare_dir", "type": "infile", "format": "sequence.cdhit_cluster_dir"},  # 输入cd-hit比对后的文件夹
            {"name": "faa", "type": "outfile", "format": "sequence.fasta"},  # 非冗余基因集蛋白序列
            {"name": "fa", "type": "outfile", "format": "sequence.fasta"},  # 非冗余基因集核算序列
            {"name": "table", "type": "int", "default": 11},  # 给出transeq参数table，11为bacteria。
        ]
        self.add_option(options)
        self.step.add_steps('cdhitmerge')
        self.on('start', self.step_start)
        self.on('end', self.step_end)

    def step_start(self):
        self.step.cdhitmerge.start()
        self.step.update()

    def step_end(self):
        self.step.cdhitmerge.finish()
        self.step.update()

    def check_options(self):
        """
        重写参数检测函数
        """
        if not self.option("compare_dir").is_set:
            raise OptionError("必须设置参数compare_dir")

    def set_resource(self):
        """
        设置所需资源
        """
        self._cpu = 2
        self._memory = '3G'

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"],
        ])
        super(CdhitMergeAgent, self).end()


class CdhitMergeTool(Tool):
    def __init__(self, config):
        super(CdhitMergeTool, self).__init__(config)
        self._version = '1.0'
        self.perl_path = '/program/perl-5.24.0/bin/perl '
        self.merge_path = self.config.SOFTWARE_DIR + '/bioinfo/uniGene/cd-hit-v4.5.7-2011-12-16/clstr_merge.pl'
        self.sort_clstr_path = self.config.SOFTWARE_DIR + '/bioinfo/uniGene/scripts/sort_clstr.pl'
        self.trans_path = 'bioinfo/seq/EMBOSS-6.6.0/emboss/transeq'
        self.num = self.option("compare_dir").prop['files_number']

    def run(self):
        self.fasta_merge()
        self.linkfile()
        self.end()

    def fasta_merge(self):
        cmd1 = 'cat '
        if os.path.isfile(self.work_dir + '/gene.uniGeneset.bak.clstr'):
            os.remove(self.work_dir + '/gene.uniGeneset.bak.clstr')
        else:
            pass
        for i in range(0, self.num):
            cmd2 = self.config.SOFTWARE_DIR + self.perl_path + self.merge_path + ' '
            cmd1 += self.option("compare_dir").prop['path'] + '/gene.geneset.tmp.fa.div-' + str(i) + '-/o '
            clstr = self.option("compare_dir").prop['path'] + '/gene.geneset.tmp.fa.div-' + str(i) + '-/o.clstr '
            if i < self.num - 1:
                for j in range(i + 1, self.num):
                    clstr += self.option("compare_dir").prop['path'] + '/gene.geneset.tmp.fa.div-' + str(
                        j) + '-/vs.' + str(i) + '.clstr '
            cmd2 += clstr + '>> ' + self.work_dir + '/gene.uniGeneset.bak.clstr'
            try:
                subprocess.check_output(cmd2, shell=True)
                self.logger.info("clstr" + str(i) + "succeed")
            except subprocess.CalledProcessError:
                self.set_error("clstr" + str(i) + "failed")
                # if i == 0 :
                #   cmd2 += self.option("compare_dir").prop['path'] + '/gene.geneset.tmp.fa.div-' + str(i) + '-/o.clstr '
                # else:
                #   cmd2 += self.option("compare_dir").prop['path'] + '/gene.geneset.tmp.fa.div-' + str(i) + '-/vs0.clstr '
        cmd1 += '> ' + self.work_dir + '/gene.uniGeneset.fa'
        #        cmd2 += '>> ' + self.work_dir + '/gene.uniGeneset.clstr'
        self.logger.info(cmd1)
        try:
            subprocess.check_output(cmd1, shell=True)
            self.successful('fa')
        # self.end()
        except subprocess.CalledProcessError:
            self.set_error("fasta failed")
        fastafile = FastaFile()
        file = self.work_dir + '/geneCatalog_stat.xls'
        fastafile.set_path(self.work_dir + '/gene.uniGeneset.fa')
        self.logger.info('成功生成fasta文件夹,开始非冗余基因集信息')
        with open(file, "w") as f:
            f.write("Catalog_genes\tCatalog_total_length(bp)\tCatalog_average_length(bp)\n")
            if fastafile.check():
                info_ = list()
                fastafile.get_info()
                info_.append(fastafile.prop["seq_number"])
                info_.append(fastafile.prop["bases"])
                avg = round(float(fastafile.prop["bases"]) / float(fastafile.prop["seq_number"]), 2)
                avg = str(avg)
                info_.append(avg)
                f.write("\t".join(info_) + "\n")
        self.logger.info('非冗余基因集信息统计完毕！')
        #        self.logger.info(cmd2)
        #        try:
        #            subprocess.check_output(cmd2,shell=True)
        #            self.successful('clstr')
        #            self.end()
        #        except subprocess.CalledProcessError:
        #            self.set_error("clstr failed")
        #        self.new_clstr(self.work_dir + '/gene.uniGeneset.bak.clstr',self.work_dir + '/gene.uniGeneset.clstr')
        #        self.wait()
        real_fasta = os.path.join(self.work_dir, 'gene.uniGeneset.fa')
        real_fastaa = os.path.join(self.work_dir, 'gene.uniGeneset.faa')
        cmd3 = '%s -sequence %s -table %s -trim -outseq %s' % (
            self.trans_path, real_fasta, self.option("table"), real_fastaa)
        self.logger.info(cmd3)
        command3 = self.add_command('cmd_3', cmd3)
        command3.run()
        self.wait(command3)
        if command3.return_code == 0:
            self.successful('faa')
        else:
            self.logger.info("fastaa failed")
            raise Exception("fastaa failed")
        cmd4 = '%s %s %s %s' % (self.perl_path, self.sort_clstr_path, self.work_dir + '/gene.uniGeneset.bak.clstr',
                                self.work_dir + '/gene.uniGeneset.clstr')
        command4 = self.add_command('cmd_4', cmd4)
        command4.run()
        self.wait(command4)
        if command4.return_code == 0:
            self.successful('clstr')
            os.remove(self.work_dir + '/gene.uniGeneset.bak.clstr')
        else:
            self.logger.info("clstr failed")

    def successful(self, type):
        self.logger.info(type + " succeed")
        name = "gene.uniGeneset." + type
        filename = os.path.join(self.work_dir, name)
        linkfile = os.path.join(self.output_dir, name)
        if os.path.exists(linkfile):
            os.remove(linkfile)
        os.link(filename, linkfile)
        if type in ["faa", "fa"]:
            self.option(type, linkfile)
        else:
            pass

    def linkfile(self):
        filename = os.path.join(self.work_dir, 'geneCatalog_stat.xls')
        linkfile = os.path.join(self.output_dir, 'geneCatalog_stat.xls')
        if os.path.exists(linkfile):
            os.remove(linkfile)
        os.link(filename, linkfile)
