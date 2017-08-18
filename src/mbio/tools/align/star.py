# -*- coding:utf-8 -*-
# __author__ = 'chenyanyan'
# last_modifiy:2016.09.26

from __future__ import division
from biocluster.agent import Agent
from biocluster.tool import Tool
import shutil
import os
import glob
from biocluster.core.exceptions import OptionError
import json
import time


class StarAgent(Agent):
    """
    star 比对工具
    """

    def __init__(self, parent):
        super(StarAgent, self).__init__(parent)
        options = [

            {"name": "ref_genome_custom", "type": "infile", "format": "sequence.fasta"},  # 用户上传参考基因组文件
            {"name": "ref_genome", "type": "string"},  # 参考基因组模式选项 用户自定义、选择已有生物物种
            {"name": "ref_gtf", "type": "infile", "format": "gene_structure.gtf"},  # 参考基因组的gtf文件 ，gtf文件和fasta文件要配套使用
            {"name": "readFilesIN1", "type": "infile", "format": "sequence.fastq, sequence.fasta"},  # 双端序列文件1端
            {"name": "readFilesIN2", "type": "infile", "format": "sequence.fastq, sequence.fasta"},  # 双端序列文件2端
            {"name": "readFilesIN", "type": "infile", "format": "sequence.fastq"},  # 单端序列文件
            {"name": "seq_method", "type": "string"},
            {"name": "is_indexed", "type": "bool", "default": False},
            {"name": "sample", "type": "string", "default": ""},
            {"name": "star_index1", "type": "infile", "format": "align.star.star_index"}

        ]
        self.add_option(options)
        self.step.add_steps('star')
        self.on('start', self.step_start)
        self.on('end', self.step_end)

    def step_start(self):
        self.step.star.start()
        self.step.update()

    def step_end(self):
        self.step.star.finish()
        self.step.update()

    def check_options(self):

        """
        检查参数设置
        """
        if self.option("ref_genome") == "customer_mode" and not self.option("ref_genome_custom").is_set:
            raise OptionError("请上传自定义参考基因组")
        if self.option("seq_method") == "PE":
            if not self.option("readFilesIN1").is_set:
                raise OptionError("请提供用于比对的fastq或fasta文件1！")
            if not self.option("readFilesIN2").is_set:
                raise OptionError("请提供用于比对的fastq或fasta文件2！")
        if self.option("seq_method") == "SE":
            if not self.option("readFilesIN").is_set:
                raise OptionError("请提供用于比对的单端fastq或fasta文件！")

    def set_resource(self):
        self._cpu = 4
        if self.option("ref_genome") == "customer_mode":
            if self.option("ref_genome_custom").prop["size"] / 1024 / 1024 < 500:
                self._memory = '10G'
            elif self.option("ref_genome_custom").prop["size"] / 1024 / 1024 < 1024:
                self._memory = '20G'

            else:
                self._memory = '60G'
        else:
            self._memory = '60G'   # 设置资源大小

    def end(self):
        super(StarAgent, self).end()    # 继承超类的end方法


class StarTool(Tool):

    def __init__(self, config):
        super(StarTool, self).__init__(config)
        self.star_path = "bioinfo/rna/star-2.5/bin/Linux_x86_64/"  # 设置star的路径
        self.samtools_path = self.config.SOFTWARE_DIR + '/bioinfo/align/samtools-1.3.1/'
        self.shell_path = 'bioinfo/rna/scripts'
        self.genomeDir_path2 = os.path.join(self.work_dir, "ref_star_index2")
        self.genomeDir_path1 = os.path.join(self.work_dir, "ref_star_index1")


    def star_index1(self, genomeDir, ref_fa):
        """
        step1:第一步建索引；用star建立参考基因组的索引，当用户不上传参考基因组时，该步骤省略，直接调用已有的序列文件
        genomeDir为用于存放第一步建立的参考基因组索引的路径

        """
        cmd = "{}STAR --runMode genomeGenerate --limitGenomeGenerateRAM 50000000000 --genomeDir {} --genomeFastaFiles {} --runThreadN 10".format(self.star_path, genomeDir, ref_fa)
        self.logger.info("使用star建立参考基因组索引")
        command = self.add_command("star_index1", cmd)
        command.run()
        self.wait(command)
        if command.return_code == 0:
            self.logger.info("成功构建参考序列索引index1！")
        else:
            command.rerun()
            self.wait(command)
            if command.return_code == 0:
                self.logger.info("成功构建参考序列索引index1！")
            else:
                self.set_error("构建索引出错!")
                raise Exception("运行star出错")

    def star_aln1_se(self, genomeDir):
        """
        step2:第二步比对；用star进行单端序列的比对
        """

        cmd = "{}STAR --runThreadN 10 --genomeDir {} --readFilesIn {}".format(self.star_path, genomeDir, self.option("readFilesIN").prop["path"])
        print cmd
        self.logger.info("使用STAR对序列进行单端mapping")
        command = self.add_command("star_aln1", cmd)
        command.run()
        self.wait(command)
        if command.return_code == 0:
            self.logger.info("单端比对第一步比对成功！")
        else:
            self.set_error("单端比对第一步比对出错!")
            raise Exception("运行star出错")

    def star_aln1_pe(self, genomeDir):
        """
        step2:第二步比对；用star进行双端序列的比对
        """

        cmd = "{}STAR --runThreadN 10 --genomeDir {} --readFilesIn {} {}".format(self.star_path, genomeDir, self.option("readFilesIN1").prop["path"], self.option("readFilesIN2").prop["path"])
        print cmd
        self.logger.info("使用STAR对序列进行双端mapping")
        command = self.add_command("star_aln1_pe", cmd)
        command.run()
        self.wait(command)
        if command.return_code == 0:
            self.logger.info("双端比对第一步比对成功！")
        else:
            self.set_error("双端比对第一步比对出错!")
            raise Exception("运行star出错")

    def star_index2(self, ref_fa, sj):
        """
        step3：第三步，第二次建索引，用于最终比对
        """
        cmd = "{}STAR --runMode genomeGenerate --limitGenomeGenerateRAM 50000000000 --runThreadN 10 --genomeDir {} --genomeFastaFiles {} --sjdbFileChrStartEnd {} --sjdbOverhang 100".format(self.star_path, self.genomeDir_path2, ref_fa, sj)
        print cmd
        self.logger.info("根据生成的sjdb数据进行第二次建索引index2")
        command = self.add_command("star_index2", cmd)
        command.run()
        self.wait(command)
        if command.return_code == 0:
            self.logger.info("第二次索引建立成功！")
        else:
            self.logger.info("开始第二次建立索引！")
            command.rerun()
            self.wait(command)
            if command.return_code == 0:
                self.logger.info("第二次索引建立成功！")
            else:
                self.set_error("第二次索引建立出错!")
                raise Exception("运行star出错")

    def star_aln2_se(self):
        """
        step4：第四步，最终比对
        """
        cmd = "{}STAR --runThreadN 10 --genomeDir {} --readFilesIn {}".format(self.star_path, self.genomeDir_path2,
                                                                              self.option("readFilesIN").prop["path"])
        self.logger.info("进入最终比对阶段！")
        command = self.add_command("star_aln2", cmd)
        command.run()
        self.wait(command)
        if command.return_code == 0:
            self.logger.info("最终单端比对成功！")
        else:
            raise Exception("运行star出错")

    def star_aln2_pe(self):
        """
        step4：第四步，最终比对
        """
        cmd = "{}STAR --runThreadN 10 --genomeDir {} --readFilesIn {} {}".format(self.star_path, self.genomeDir_path2, self.option("readFilesIN1").prop["path"], self.option("readFilesIN2").prop["path"])
        print cmd
        self.logger.info("最终比对过程")
        command = self.add_command("star_aln2", cmd)
        command.run()
        self.wait(command)
        if command.return_code == 0:
            self.logger.info("最终双端比对成功！")
        else:
            command.rerun()
            self.wait(command)
            if command.return_code == 0:
                self.logger.info("最终双端比对成功！")
            else:
                self.set_error("最终双端比对出错!")
                raise Exception("运行star出错")

    def star_aln2_pe_bam(self):
        """
        step4：第四步，最终比对
        """
        cmd = "{}STAR --runThreadN 10 --outSAMtype BAM SortedByCoordinate --genomeDir {} --readFilesIn {} {}".format(self.star_path, self.genomeDir_path2, self.option("readFilesIN1").prop["path"], self.option("readFilesIN2").prop["path"])
        self.logger.info("最终比对过程")
        command = self.add_command("star_aln2_bam", cmd)
        command.run()
        self.wait(command)
        if command.return_code == 0:
            self.logger.info("最终双端比对成功！")
        else:
            command.rerun()
            self.wait(command)
            if command.return_code == 0:
                self.logger.info("最终双端比对成功！")
            else:
                self.set_error("最终双端比对出错!")
                raise Exception("运行star出错")

    def star_aln2_se_bam(self):
        """
        step4：第四步，最终比对
        """
        cmd = "{}STAR --runThreadN 10 --genomeDir {} --outSAMtype BAM SortedByCoordinate " \
              "--readFilesIn {}".format(self.star_path, self.genomeDir_path2,
                self.option("readFilesIN").prop["path"])
        self.logger.info("使用star进行最终比对！")
        command = self.add_command("star_aln2_bam", cmd)
        command.run()
        self.wait(command)
        if command.return_code == 0:
            self.logger.info("最终单端比对成功！")
        else:
            command.rerun()
            self.wait(command)
            if command.return_code == 0:
                self.logger.info("最终单端比对成功！")
            else:
                self.set_error("最终单端比对出错!")
                raise Exception("运行star出错")

    def run(self):
        """
        运行
        """
        super(StarTool, self).run()
        if not self.option("ref_genome_custom").is_set:
            raise Exception("请设置参考基因组文件")
        self.logger.info("star开始运行，建立索引的步骤由align.star_index进行，索引文件夹通过option-star_index1传入！")
        if not os.path.exists("ref_star_index2"):
            os.mkdir("ref_star_index2")
        if not os.path.exists("ref_star_index1"):
            os.mkdir("ref_star_index1")
        for file in os.listdir(self.option("star_index1").prop["path"]):
            old_path = os.path.join(self.option("star_index1").prop["path"], file)
            new_path = os.path.join(self.genomeDir_path1, file)
            if os.path.exists(new_path):
                os.remove(new_path)
            os.link(old_path, new_path)
        ref_fa = self.option("ref_genome_custom").prop["path"]
        if self.option("seq_method") == "PE":
            self.star_aln1_pe(self.genomeDir_path1)
            sj = os.path.join(self.work_dir, "SJ.out.tab")
            self.star_index2(ref_fa, sj)
            # self.star_aln2_pe()
            self.star_aln2_pe_bam()
        else:
            self.star_aln1_se(self.genomeDir_path1)
            sj = os.path.join(self.work_dir, "SJ.out.tab")
            self.star_index2(ref_fa, sj)
            # self.star_aln2_se()
            self.star_aln2_se_bam()
        self.mv_bam()
        self.end()

    def mv_bam(self):
        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)
            os.mkdir(self.output_dir)
        if not os.path.exists(self.output_dir + "/bam"):
            os.mkdir(self.output_dir + "/bam")
        if not os.path.exists(self.output_dir + "/sam"):
            os.mkdir(self.output_dir + "/sam")
        os.link(self.work_dir + "/Aligned.sortedByCoord.out.bam", self.output_dir + "/bam/" + self.option("sample") + ".bam")
        os.link(os.path.join(self.work_dir, "Aligned.out.sam"), self.output_dir + "/sam/" + self.option("sample") + ".sam")
