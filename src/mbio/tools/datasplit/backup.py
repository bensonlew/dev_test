# -*- coding: utf-8 -*-
# __author__ = 'xuting'

"""在二次拆分完成之后，对父样本和子样本进行备份存储"""
import os
import errno
import time
import subprocess
import shutil
from biocluster.config import Config
from biocluster.tool import Tool
from biocluster.agent import Agent
from biocluster.core.exceptions import OptionError


class BackupAgent(Agent):
    """
    数据备份
    """
    def __init__(self, parent=None):
        super(BackupAgent, self).__init__(parent)
        self._run_mode = "ssh1"
        options = [
            {'name': 'sample_info', 'type': "infile", 'format': 'datasplit.miseq_split'},  # 样本拆分信息表
            {'name': "parent_path", 'type': "string"},  # 解压后父样本的路径
            {'name': "fastx_path", 'type': "string"},  # fastx模块的fastx路径
            {'name': "report_path", 'type': "string"},  # bcl2fastq的Report的路径
            {'name': "child_path", 'type': "string", "default": ""},   # 解压后子样本的路径
            {'name': "time", 'type': "outfile", 'format': 'datasplit.backup_time'}  # 输出文件, 记录了备份时用到的year和month
        ]
        self.add_option(options)

    def check_option(self):
        """
        参数检测
        """
        if not self.option("parent_path"):
            raise OptionError("参数parent_path不能为空")
        if not self.option("fastx_path"):
            raise OptionError("参数fastx_path不能为空")
        if not self.option("report_path"):
            raise OptionError("参数report_path不能为空")
        if not self.option('sample_info').is_set:
            raise OptionError("参数sample_info不能为空")
        return True

    def set_resource(self):
        """
        设置所需要的资源
        """
        self._cpu = 3
        self._memory = ''


class BackupTool(Tool):
    def __init__(self, config):
        super(BackupTool, self).__init__(config)
        self._version = 1.0
        self.option('sample_info').get_info()
        self.backup_dir = "/mnt/ilustre/users/sanger/data_split_tmp/"
        self.gzip_path = os.path.join(Config().SOFTWARE_DIR, "datasplit/bin/gzip")
        year = time.localtime()[0]
        month = time.localtime()[1]
        self.create_time_file(year, month)
        program = self.option('sample_info').prop["program"]
        name = "id_" + str(self.option('sample_info').prop["split_id"]) +\
               "_" + str(self.option('sample_info').prop["sequcing_sn"])
        self.seq_id = os.path.join(self.backup_dir, program, str(year), str(month), name)

    def create_time_file(self, year, month):
        """
        生成time文件，供下一模块使用
        """
        name = os.path.join(self.work_dir, "output", "time.txt")
        with open(name, 'w') as w:
            w.write("year\t" + str(year) + "\n")
            w.write("month\t" + str(month) + "\n")
        self.option("time").set_path(name)

    def make_ess_dir(self):
        """
        为这块板子创建备份目录
        """
        dir_list = list()
        for pro in self.option('sample_info').prop["library_type"]:
            name = os.path.join(self.seq_id, pro)
            name1 = os.path.join(name, "parent", "fastx")
            name2 = os.path.join(name, "child")
            dir_list.append(name1)
            dir_list.append(name2)
        for name in dir_list:
            try:
                os.makedirs(name)
            except OSError as exc:
                if exc.errno == errno.EEXIST and os.path.isdir(name):
                    pass
                else:
                    raise OSError("创建目录失败")

    def cp_fastx(self):
        """
        将fastx统计文件复制到相关路径下
        """
        self.logger.info("复制fastx统计文件")
        for p_id in self.option('sample_info').prop["parent_ids"]:
            sample_id = self.option('sample_info').parent_sample(p_id, "sample_id")
            file_list = list()
            file_list.append(os.path.join(self.option('fastx_path'), sample_id + "_r1.fastq.fastxstat"))
            file_list.append(os.path.join(self.option('fastx_path'), sample_id + "_r1.fastq.fastxstat.box.png"))
            file_list.append(os.path.join(self.option('fastx_path'), sample_id + "_r1.fastq.fastxstat.nucl.png"))
            file_list.append(os.path.join(self.option('fastx_path'), sample_id + "_r1.fastq.q20q30"))
            file_list.append(os.path.join(self.option('fastx_path'), sample_id + "_r2.fastq.fastxstat"))
            file_list.append(os.path.join(self.option('fastx_path'), sample_id + "_r2.fastq.fastxstat.box.png"))
            file_list.append(os.path.join(self.option('fastx_path'), sample_id + "_r2.fastq.fastxstat.nucl.png"))
            file_list.append(os.path.join(self.option('fastx_path'), sample_id + "_r2.fastq.q20q30"))
            library_type = self.option('sample_info').parent_sample(p_id, "library_type")
            if library_type is None:
                library_type = "undefine"
            dst = os.path.join(self.seq_id, library_type, "parent", "fastx")
            for file_name in file_list:
                shutil.copy2(file_name, dst)

    def cp_report(self):
        """
        将bcl2fastq的Report文件复制到相关的目录下
        """
        self.logger.info("复制bcl2fastq报告文件")
        dst = os.path.join(self.seq_id, "Reports")
        if os.path.exists(dst):
            shutil.rmtree(dst)
        shutil.copytree(self.option('report_path'), dst)

    def gz_parent(self):
        """
        将父样本压缩至备份路径
        """
        cmd_list = list()
        i = 0
        self.logger.info("开始压缩父样本")
        for p_id in self.option('sample_info').prop["parent_ids"]:
            sample_id = self.option('sample_info').parent_sample(p_id, "sample_id")
            file_name_r1 = sample_id + "_r1.fastq"
            file_name_r2 = sample_id + "_r2.fastq"
            for name in os.listdir(self.option('parent_path')):
                if file_name_r1 == name or file_name_r2 == name:
                    i += 1
                    sourcefile = os.path.join(self.option('parent_path'), name)
                    library_type = self.option('sample_info').parent_sample(p_id, "library_type")
                    if library_type is None:
                        library_type = "undefine"
                    target_file = os.path.join(self.seq_id, library_type, "parent", name + '.gz')
                    cmd = (self.gzip_path + " -c -f " + sourcefile + " > " + target_file)
                    command = subprocess.Popen(cmd, shell=True)
                    cmd_list.append(command)
        for mycmd in cmd_list:
            self.logger.info("开始压缩父样本")
            mycmd.communicate()
        for mycmd in cmd_list:
            if mycmd.returncode == 0:
                self.logger.info("gzip完成")
            else:
                self.set_error("gzip发生错误")
                self.logger.error("gzip发生错误")
                raise Exception("gzip发生错误")

    def gz_child(self):
        """
        将子样本压缩至备份文件
        """
        cmd_list = list()
        i = 0
        self.logger.info("开始压缩子样本")
        for p_id in self.option('sample_info').prop["parent_ids"]:
            if self.option('sample_info').parent_sample(p_id, "has_child"):
                child_ids = self.option('sample_info').find_child_ids(p_id)
                for c_id in child_ids:
                    sample_id = self.option('sample_info').child_sample(c_id, "sample_id")
                    primer = self.option('sample_info').child_sample(c_id, "primer")
                    file_name = sample_id + "_" + primer + ".fastq"
                    for name in os.listdir(self.option('child_path')):
                        if name == file_name:
                            i += 1
                            sourcefile = os.path.join(self.option('child_path'), name)
                            library_type = self.option('sample_info').parent_sample(p_id, "library_type")
                            if library_type is None:
                                library_type = "undefine"
                            target_file = os.path.join(self.seq_id, library_type, "child", name + '.gz')
                            cmd = (self.gzip_path + " -c -f " + sourcefile + " > " + target_file)
                            command = subprocess.Popen(cmd, shell=True)
                            cmd_list.append(command)
        for mycmd in cmd_list:
            self.logger.info("开始压缩子样本")
            mycmd.communicate()
        for mycmd in cmd_list:
            if mycmd.returncode == 0:
                self.logger.info("gzip完成")
            else:
                self.set_error("gzip发生错误")

    def run(self):
        super(BackupTool, self).run()
        self.make_ess_dir()
        self.cp_report()
        self.cp_fastx()
        self.gz_parent()
        if self.option('child_path'):
            self.gz_child()
        self.end()
