# -*- coding: utf-8 -*-
# __author__ = 'zouxuan'
from biocluster.agent import Agent
from biocluster.tool import Tool
import os
import subprocess
from biocluster.core.exceptions import OptionError
import shutil


class CdhitCompareSingleAgent(Agent):
    """
    cd-hit-est
    version v1.0
    author: zouxuan
    last modified:2017.8.10
    """

    def __init__(self, parent):
        super(CdhitCompareSingleAgent, self).__init__(parent)
        options = [
            {"name": "query", "type": "infile", "format": "sequence.fasta"},  # 输入fasta文件
            {"name": "qunum", "type": "int", "default": 0},  # fasta编号
            {"name": "identity", "type": "float", "default": 0.95},  ##给出cdhit的参数identity
            {"name": "coverage", "type": "float", "default": 0.9},  # 给出cdhit的参数coverage
            {"name": "memory_limit", "type": "int", "default": 10000},  # 内存大小，0为无限制
            {"name": "method", "type": "int", "default": 0},  # 1为全局比对，0为局部比对
            {"name": "direction", "type": "int", "default": 1},  # 1为双向比对，0为单向比对
            {"name": "num_threads", "type": "int", "default": 8},  # cpu数
            {"name": "select", "type": "int", "default": 1},  # 1为聚类到最相似的类中，0为聚类到第一个符合阈值的类
            {"name": "compare", "type": "string", "default": ""},  # 比对结果输出路径
        ]
        self.add_option(options)
        self.step.add_steps('cdhitcomparesingle')
        self.on('start', self.step_start)
        self.on('end', self.step_end)

    def step_start(self):
        self.step.cdhitcomparesingle.start()
        self.step.update()

    def step_end(self):
        self.step.cdhitcomparesingle.finish()
        self.step.update()

    def check_options(self):
        """
        重写参数检测函数
        """
        if not self.option("query").is_set:
            raise OptionError("必须设置参数query")
        if not self.option("compare").strip():
            raise OptionError("必须设置输出路径compare")
        if not 0.75 <= self.option("identity") <= 1:
            raise OptionError("identity必须在0.75，1之间")
        if not 0 <= self.option("coverage") <= 1:
            raise OptionError("coverage必须在0,1之间")

    def set_resource(self):
        """
        设置所需资源
        """
        self._cpu = self.option("num_threads")
        self._memory = str(self.option("memory_limit") / 1000) + 'G'

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"],
        ])
        super(CdhitCompareSingleAgent, self).end()


class CdhitCompareSingleTool(Tool):
    def __init__(self, config):
        super(CdhitCompareSingleTool, self).__init__(config)
        self._version = '1.0'
        self.cdhit_est_path = 'bioinfo/uniGene/cd-hit-v4.5.7-2011-12-16/cd-hit-est'

    def run(self):
        self.single_compare()
        self.set_output()

    def word_len(self):
        word_length = 8
        if self.option("identity") >= 0.9:
            word_length = 8
        elif 0.88 <= self.option("identity") < 0.9:
            word_length = 7
        elif 0.85 <= self.option("identity") < 0.88:
            word_length = 6
        elif 0.8 <= self.option("identity") < 0.85:
            word_length = 5
        elif 0.75 <= self.option("identity") < 0.8:
            word_length = 4
        return word_length

    def single_compare(self):
        length = self.word_len()
        out_dir = self.option("compare") + '/gene.geneset.tmp.fa.div-' + str(self.option("qunum")) + "-"
        if os.path.exists(out_dir):
            pass
        else:
            os.mkdir(out_dir)
        cmd = '%s -i %s -o %s -c %s -aS %s -n %s -G %s -M %s -d %s -r %s -g %s -T %s' % (
            self.cdhit_est_path, self.option("query").prop['path'], out_dir + "/o", self.option("identity"),
            self.option("coverage"), length, self.option("method"), self.option("memory_limit"), 0,
            self.option("direction"), self.option("select"), self.option("num_threads"))
        self.logger.info(cmd)
        command1 = self.add_command('cmd_1', cmd)
        command1.run()
        self.wait(command1)
        if command1.return_code == 0:
            self.logger.info("compare single succeed")
        else:
            self.set_error("compare single failed")
            raise Exception("compare single failed")

    def set_output(self):
        self.linkdir(self.option("compare") + '/gene.geneset.tmp.fa.div-' + str(self.option("qunum")) + "-",
                     "gene.geneset.tmp.fa.div-" + str(self.option("qunum")) + "-")
        self.end()

    def linkdir(self, dirpath, dirname):
        """
        link一个文件夹下的所有文件到本module的output目录
        """
        allfiles = os.listdir(dirpath)
        newdir = os.path.join(self.output_dir, dirname)
        if not os.path.exists(newdir):
            os.mkdir(newdir)
        oldfiles = [os.path.join(dirpath, i) for i in allfiles]
        newfiles = [os.path.join(newdir, i) for i in allfiles]
        for newfile in newfiles:
            if os.path.exists(newfile):
                os.remove(newfile)
        for i in range(len(allfiles)):
            os.link(oldfiles[i], newfiles[i])
