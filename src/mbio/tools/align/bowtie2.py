# -*- coding: utf-8 -*-
# __author__ = 'guhaidong'
import os
import re
from biocluster.core.exceptions import OptionError
from biocluster.agent import Agent
from biocluster.tool import Tool


class Bowtie2Agent(Agent):
    """
    运用bowtie2将reads map至contig
    version v2.2.9
    author: guhaidong
    last_modify: 2017.9.12
    """

    def __init__(self, parent):
        super(Bowtie2Agent, self).__init__(parent)
        options = [
            {"name": "ref_fasta", "type": "infile", "format": "sequence.fasta"},  # 输入文件,sample.contig.fa
            {"name": "fastq1", "type": "infile", "format": "sequence.fastq"},  # 输入文件,l
            {"name": "fastq2", "type": "infile", "format": "sequence.fastq"},  # 输入文件,r
            {"name": "fastqs", "type": "infile", "format": "sequence.fastq"},  # 输入文件,s 可不传
            {"name": "sam_file", "type": "outfile", "format": "align.bwa.sam_dir"},  # 输出文件,map结果的sam路径
        ]
        self.add_option(options)
        self.step.add_steps("Bowtie2_index")
        self.on('start', self.stepstart)
        self.on('end', self.stepfinish)

    def stepstart(self):
        self.step.Bowtie2_index.start()
        self.step.update()

    def stepfinish(self):
        self.step.Bowtie2_index.finish()
        self.step.update()

    def check_options(self):
        """
        重写参数检测函数
        :return:
        """
        if not self.option('fastq1'):
            raise OptionError('必须输入*l.fastq文件')
        if not self.option('fastq2'):
            raise OptionError('必须输入*r.fastq文件')
        if not self.option("ref_fasta"):
            raise OptionError('必须输入比对参考序列')
        return True

    def set_resource(self):
        """
        设置所需资源，需在子类中重写此方法 self._cpu ,self._memory
        :return:
        """
        self._cpu = 8
        self._cpu = 2
        self._memory = "20G"
        self._memory = "5G"

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        # result_dir.add_relpath_rules([
        #    [".", "", "结果输出目录"],
        # ])
        # result_dir.add_regexp_rules([
        #    ["", "", ""]
        # ])
        super(Bowtie2Agent, self).end()


class Bowtie2Tool(Tool):
    def __init__(self, config):
        super(Bowtie2Tool, self).__init__(config)
        # self.version = "v2.2.9"
        self.bowtie2_path = '/bioinfo/align/bowtie2-2.2.9/'
        self.index_prefix = ''
        self.samp_name = os.path.basename(self.option('fastq1').path).split('.')[0]

    def run_bowtie2_index(self):
        """
        运行bowtie2_index
        如果多次运行bowtie2使用同一组参考序列，则自动跳过command，直接进行比对
        :return:
        """
        if not os.path.exists(self.output_dir):
            os.mkdir(self.output_dir)
        ref_file_name = os.path.basename(self.option('ref_fasta').path)
        self.index_prefix = ref_file_name.split('.')[0]
        # self.logger.info("ref_file_name: " + ref_file_name)
        if not os.path.exists(self.output_dir + "/" + self.index_prefix + ".rev.2.bt2"):
            cmd = "{}bowtie2-build  {}  {}/{}".format(self.bowtie2_path, self.option("ref_fasta").path, self.work_dir,
                                                      self.index_prefix)
            self.logger.info('运行bowtie2_index')
            # self.logger.info('debugging information:' + cmd)
            command = self.add_command("bowtie2_index_cmd", cmd).run()
            self.wait(command)
            if command.return_code == 0:
                self.logger.info("bowtie2_index运行完成")
            else:
                self.set_error("bowtie2_index运行出错！")
        else:
            self.logger.info("%s已存在index，跳过bowtie2_index" % (self.index_prefix))

    def run_bowtie2_map(self):
        """
        运行bowtie2比对
        先进行pair-end reads比对，当有single reads出现时，进行single reads比对，没有则不进行
        :return:
        """
        self.logger.info('运行bowtie2 比对 pair reads')
        list_file = open(self.output_dir + '/list.txt', 'w')
        list_file.write("{}.pair.sam\t{}\tPE\n".format(self.samp_name, self.samp_name))
        cmd = "{}bowtie2  -p 2 -x {}/{}  -1  {}  -2  {}  -S  {}/{}.pair.sam ".format(self.bowtie2_path, \
                                                                                     self.work_dir, self.index_prefix,
                                                                                     self.option("fastq1").path,
                                                                                     self.option("fastq2").path, \
                                                                                     self.output_dir, self.samp_name)
        command = self.add_command("bowtie2_map_pair", cmd).run()
        self.wait(command)
        if command.return_code == 0:
            self.logger.info("bowtie2_map_pair运行完成")
        else:
            list_file.close()
            self.set_error("bowtie2_map_pair运行出错！")
        # self.logger.info("fastqs is : " + self.option("fastqs").path)
        if self.option("fastqs").path != None:
            self.logger.info("运行bowtie2 比对 single reads")
            list_file.write("{}.single.sam\t{}\tSE\n".format(self.samp_name, self.samp_name))
            cmd = "{}bowtie2  -p  6  -x  {}/{}  -U  {}  -S  {}/{}.single.sam".format(self.bowtie2_path, \
                                                                                     self.work_dir, self.index_prefix,
                                                                                     self.option("fastqs").path,
                                                                                     self.output_dir, self.samp_name)
            command = self.add_command("bowtie2_map_single", cmd).run()
            self.wait(command)
            if command.return_code == 0:
                self.logger.info("bowtie2_map_single运行完成")
            else:
                list_file.close()
                self.set_error("bowtie2_map_single运行出错！")
        list_file.close()

    def set_output(self):
        """
        将结果文件复制到output文件夹下面
        :return:
        """
        self.logger.info("设置结果目录")
        self.option('sam_file').set_path(self.output_dir)
        self.logger.info('设置组装拼接分析结果目录成功')

    def run(self):
        """
        运行bowtie2比对程序
        :return:
        """
        super(Bowtie2Tool, self).run()
        self.run_bowtie2_index()
        self.run_bowtie2_map()
        self.set_output()
        self.end()
