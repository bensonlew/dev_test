# -*- coding: utf-8 -*-
# __author__ = 'xuting'

"""fastxtoolkit  用于统计碱基质量信息"""
import os
import re
import errno
import gzip
import subprocess
from biocluster.config import Config
from biocluster.tool import Tool
from biocluster.agent import Agent
from biocluster.core.exceptions import OptionError


class FastxAgent(Agent):
    """
    fastxtoolkit
    """
    def __init__(self, parent=None):
        super(FastxAgent, self).__init__(parent)
        options = [
            {'name': 'sample_info', 'type': "infile", 'format': 'datasplit.miseq_split'},  # 样本拆分信息表
            {'name': 'data_path', 'type': "string"}  # bcl2fastq的下机输出目录
        ]
        self.add_option(options)

    def check_option(self):
        """
        参数检测
        """
        if not self.option('sample_info').is_set:
            raise OptionError("参数sample_info不能为空")
        if not self.option('data_path'):
            raise OptionError("参数data_path不能为空")
        return True

    def set_resource(self):
        """
        设置所需要的资源
        """
        self._cpu = 3
        self._memory = ''


class FastxTool(Tool):
    """
    """
    def __init__(self, config):
        super(FastxTool, self).__init__(config)
        self._version = 1.0
        self.fastx_dir = "fastxtoolkit/bin/"
        self.java_dir = os.path.join(Config().SOFTWARE_DIR, "sun_jdk1.8.0/bin/java")
        self.FastqTotalHighQualityBase = os.path.join(Config().SOFTWARE_DIR, "fastxtoolkit/bin/FastqTotalHighQualityBase.jar")
        self.gnuplot = os.path.join(Config().SOFTWARE_DIR, "fastxtoolkit/bin/")
        self.option('sample_info').get_info()
        self.fastqs = list()
        self.fastx = list()

    def find_parent_sn(self, filename):
        """
        用一个文件名查找一个样本的mj_sn
        :param filename: 文件名
        """
        for p in self.option('sample_info').prop["parent_sample"]:
            mj_sn = re.sub(r'-', r'_', p["mj_sn"])
            if re.search(mj_sn, filename):
                return p["mj_sn"]
            if re.search(p["mj_sn"], filename):
                return p["mj_sn"]
        raise Exception("没有找到对应的样本")

    def make_ess_dir(self):
        """
        为软件fastxtoolkit的运行创建必要的运行目录
        """
        unzip_dir = os.path.join(self.work_dir, "unzip")
        fastx_dir = os.path.join(self.work_dir, "fastx")
        dir_list = [unzip_dir, fastx_dir]
        for name in dir_list:
            try:
                os.makedirs(name)
            except OSError as exc:
                if exc.errno == errno.EEXIST and os.path.isdir(name):
                    pass
                else:
                    raise OSError("创建目录失败")

    def unzip(self):
        """
        解压和重命名bcl2fastq的结果，供后续的fastxtoolkit使用
        """
        for project in self.option('sample_info').prop["projects"]:
            fq_dir = os.path.join(self.option('data_path'), project)
            fq_list = os.listdir(fq_dir)
            for fq in fq_list:
                mj_sn = self.find_parent_sn(fq)
                if re.search(r'.+_R1_001.+', fq):
                    unzip_name = mj_sn + "_r1.fastq"
                elif re.search(r'.+_R2_001.+', fq):
                    unzip_name = mj_sn + "_r2.fastq"
                else:
                    raise Exception("错误的文件名")
                fq = os.path.join(fq_dir, fq)
                self.logger.debug("开始解压文件" + fq)
                unzip_name = os.path.join(self.work_dir, "unzip", unzip_name)
                self.fastqs.append(unzip_name)
                r = gzip.open(fq, 'r')
                with open(unzip_name, 'w') as w:
                    for line in r:
                        w.write(line)

    def fastxtoolkit(self):
        """
        统计碱基质量
        """
        cmd_list = list()
        i = 0
        for fastq in self.fastqs:
            i += 1
            file_name = os.path.join(self.work_dir, "fastx", os.path.basename(fastq) + ".fastxstat")
            self.fastx.append(file_name)
            cmd = self.fastx_dir + "fastx_quality_stats" + " -i " + fastq + " -o " + file_name
            command = self.add_command("fastx_quality_stats" + str(i), cmd)
            cmd_list.append(command)
        for mycmd in cmd_list:
            self.logger.info("开始运行fastx_quality_stats")
            mycmd.run()
        self.wait()
        for mycmd in cmd_list:
            if mycmd.return_code == 0:
                self.logger.info("运行fastx_quality_stats完成")
            else:
                self.set_error("运行fastx_quality_stats出错")

    def fastx_nucl_dist(self):
        """
        根据碱基质量统计文件绘制质量分布图和box图
        """
        cmd_list = list()
        i = 0
        self.set_environ(PATH=self.gnuplot)
        for fastxstat in self.fastx:
            i += 1
            nucl_name = os.path.join(self.work_dir, "fastx", os.path.basename(fastxstat) + ".nucl.png")
            box_name = os.path.join(self.work_dir, "fastx", os.path.basename(fastxstat) + ".box.png")
            cmd = (self.fastx_dir + "fastx_nucleotide_distribution_graph.sh -i " + fastxstat
                   + " -o " + nucl_name)
            command = self.add_command("fastx_nucleotide_distribution_graph.sh" + str(i), cmd)
            cmd_list.append(command)
            cmd = (self.fastx_dir + "fastq_quality_boxplot_graph.sh -i " + fastxstat
                   + " -o " + box_name)
            command = self.add_command("fastq_quality_boxplot_graph.sh" + str(i), cmd)
            cmd_list.append(command)
        for mycmd in cmd_list:
            self.logger.info("开始运行" + mycmd.name)
            mycmd.run()
        self.wait()
        for mycmd in cmd_list:
            if mycmd.return_code == 0:
                self.logger.info(mycmd.name + "完成")
            else:
                self.set_error(mycmd.name + "发生错误")

    def q20_q30(self):
        cmd_list = list()
        for fastq in self.fastqs:
            file_name1 = os.path.join(self.work_dir, "fastx", os.path.basename(fastq) + ".q20")
            file_name2 = os.path.join(self.work_dir, "fastx", os.path.basename(fastq) + ".q30")
            cmd = (self.java_dir + " -jar " + self.FastqTotalHighQualityBase +
                   " -i " + fastq + " -q 20 >> " + file_name1)
            command = subprocess.Popen(cmd, shell=True)
            cmd_list.append(command)
            cmd = (self.java_dir + " -jar " + self.FastqTotalHighQualityBase +
                   " -i " + fastq + " -q 30 >> " + file_name2)
            command = subprocess.Popen(cmd, shell=True)
            cmd_list.append(command)
        for mycmd in cmd_list:
            self.logger.info("开始运行java,统计q20，q30")
            mycmd.communicate()
        for mycmd in cmd_list:
            if mycmd.returncode == 0:
                self.logger.info("q20,q30统计完成")
            else:
                self.set_error("q20,q30统计出错")
        for fastq in self.fastqs:
            file_name1 = os.path.join(self.work_dir, "fastx", os.path.basename(fastq) + ".q20")
            file_name2 = os.path.join(self.work_dir, "fastx", os.path.basename(fastq) + ".q30")
            file_name3 = os.path.join(self.work_dir, "fastx", os.path.basename(fastq) + ".q20q30")
            with open(file_name1, 'r') as r1:
                line1 = r1.next().rstrip('\n')
            with open(file_name2, 'r') as r2:
                line2 = r2.next().rstrip('\n')
            with open(file_name3, 'w') as w:
                w.write(line1 + "\n")
                w.write(line2 + "\n")

    def run(self):
        super(FastxTool, self).run()
        self.make_ess_dir()
        self.unzip()
        self.fastxtoolkit()
        self.fastx_nucl_dist()
        self.q20_q30()
        self.end()
