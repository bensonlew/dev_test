# -*- coding: utf-8 -*-
# __author__ = 'xuting'

"""二次拆分程序 用于对父样本(混样)进行二次拆分"""
import os
import re
import errno
from biocluster.config import Config
from biocluster.tool import Tool
from biocluster.agent import Agent
from biocluster.core.exceptions import OptionError
from mbio.packages.datasplit.miseq_split import reverse_complement


class SecondSplitAgent(Agent):
    """
    二次拆分
    """
    def __init__(self, parent=None):
        super(SecondSplitAgent, self).__init__(parent)
        options = [
            {'name': 'sample_info', 'type': "infile", 'format': 'datasplit.miseq_split'},  # 样本拆分信息表
            {'name': 'unzip_path', 'type': "string"}  # bcl2fastq软件拆分出来的fastq解压后的输出目录
        ]
        self.add_option(options)

    def check_option(self):
        """
        参数检测
        """
        if not self.option('sample_info').is_set:
            raise OptionError("参数sample_info不能为空")
        if not self.option('unzip_path'):
            raise OptionError("参数unzip_path不能为空")
        return True

    def set_resource(self):
        """
        设置所需要的资源
        """
        self._cpu = 3
        self._memory = ''

class SecondSplitTool(Tool):
    """
    """
    def __init__(self, config):
        super(SecondSplitTool, self).__init__(config)
        self._version = 1.0
        self.option('sample_info').get_info()
        self.pear_path = "seqs/pear"
    
    def make_ess_dir(self):
        """
        为二次拆分创建必要的目录
        """
        merge_dir = os.path.join(self.work_dir, "merge")
        dir_list = [merge_dir]
        for name in dir_list:
            try:
                os.makedirs(name)
            except OSError as exc:
                if exc.errno == errno.EEXIST and os.path.isdir(name):
                    pass
                else:
                    raise OSError("创建目录失败")

    def pear(self):
        """
        用软件pear对bcl2fastq的结果进行merage
        """
        merge_dir = os.path.join(self.work_dir, "merge")
        i = 0
        cmd_list = list()
        for p in self.option('sample_info').prop["parent_sample"]:
            if p["has_child"]:
                i += 1
                file_r1 = os.path.join(self.option('unzip_path'), p['mj_sn'] + "_r1.fastq")
                file_r2 = os.path.join(self.option('unzip_path'), p['mj_sn'] + "_r2.fastq")
                pearstr = (self.pear_path + "  -p 1.0 -j 16 -f " + file_r1 + " -r " + file_r2 + " -o " +
                           merge_dir + "pear_" + p['mj_sn'] + ">> " + merge_dir + "/pear.log")
                command = subprocess.Popen(pearstr, shell=True)
                cmd_list.append(command)
        for mycmd in cmd_list:
            self.logger.info("开始运行pear")
            mycmd.communicate()
        for mycmd in cmd_list:
            if mycmd.returncode == 0:
                self.logger.info("pear运行完成")
            else:
                self.set_error("pear运行出错")

    def split(self):
        """
        对所有的父样本进行遍历检测，如果该父样本有子样本，则进行2次拆分
        """
        for p_id in self.option('sample_info').prop["parent_ids"]:
            if self.option('sample_info').parent_sample(p_id, "has_child"):
                self.s_split(p_id)

    def s_split(self, p_id):
        """
        接收一个父样本的id号，进行二次拆分
        :param p_id: 父样本的id号
        """
        index = self.option('sample_info').find_index(p_id)
        mj_sn = self.option('sample_info').parent_sample(p_id, "mj_sn")
        sourcefile = os.path.join(self.work_dir, "merge", "pear_" + mj_sn + ".fastq")
        with open(sourcefile, 'r') as r:
            for line in r:
                head = line.rstrip('\n')
                ori_seq = r.next().rstrip('\n')
                direction = r.next()
                ori_quality = r.next().rstrip('\n')
                rev_ori_seq = ori_seq[::-1]




