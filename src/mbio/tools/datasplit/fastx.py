# -*- coding: utf-8 -*-
# __author__ = 'xuting'

"""fastxtoolkit  用于统计碱基质量信息"""
import os
import re
import errno
import gzip
import multiprocessing
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
        self._run_mode = "ssh1"
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
        self.python_dir = "Python/bin/python"
        self.q20q30_stat = os.path.join(Config().SOFTWARE_DIR, "datasplit/bin/q20q30_stat.py")
        self.gnuplot = os.path.join(Config().SOFTWARE_DIR, "gnuplot/bin")
        self.option('sample_info').get_info()
        self.fastqs = list()
        self.fastx = list()

    def find_parent_sample_id(self, filename):
        """
        用一个文件名查找一个样本的id
        :param filename: 文件名
        """
        for p in self.option('sample_info').prop["parent_sample"]:
            sample_id = re.sub(r'-', r'_', p["sample_id"])
            if re.search(sample_id, filename):
                return p["sample_id"]
            if re.search(p["sample_id"], filename):
                return p["sample_id"]
        raise Exception("没有找到对应的样本: {}".format(filename))

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
        threads = list()
        self.logger.debug(self.option('sample_info').prop["library_type"])
        print "\n"
        for library_type in self.option('sample_info').prop["library_type"]:
            fq_dir = os.path.join(self.option('data_path'), library_type)
            fq_list = os.listdir(fq_dir)
            for fq in fq_list:
                sample_id = self.find_parent_sample_id(fq)
                if re.search(r'.+_R1_001.+', fq):
                    unzip_name = sample_id + "_r1.fastq"
                elif re.search(r'.+_R2_001.+', fq):
                    unzip_name = sample_id + "_r2.fastq"
                else:
                    raise Exception("错误的文件名")
                fq = os.path.join(fq_dir, fq)
                self.logger.debug("开始解压文件" + fq)
                unzip_name = os.path.join(self.work_dir, "unzip", unzip_name)
                self.fastqs.append(unzip_name)
                t = multiprocessing.Process(target=self.unzip_file, args=(fq, unzip_name))
                threads.append(t)
        for t in threads:
            t.start()
        for t in threads:
            t.join()

    def unzip_file(self, infile, outfile):
        """
        输入一个压缩文件名和一个输出文件名，进行文件的解压
        """
        with gzip.open(infile, 'rb') as r, open(outfile, 'wb') as w:
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
        i = 0
        for fastq in self.fastqs:
            i += 1
            file_name = os.path.join(self.work_dir, "fastx", os.path.basename(fastq) + ".q20q30")
            cmd = (self.python_dir + " " + self.q20q30_stat + " -i " + fastq + " -o " + file_name)
            command = self.add_command("q20q30_stat" + str(i), cmd)
            cmd_list.append(command)
        for mycmd in cmd_list:
            self.logger.info("开始运行" + mycmd.name)
            mycmd.run()
        self.wait()
        for mycmd in cmd_list:
            if mycmd.return_code == 0:
                self.logger.info(mycmd.name + " 统计完成")
            else:
                self.set_error(mycmd.name + " 统计出错")

    def run(self):
        super(FastxTool, self).run()
        self.make_ess_dir()
        self.unzip()
        self.fastxtoolkit()
        self.fastx_nucl_dist()
        self.q20_q30()
        self.end()
