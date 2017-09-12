# -*- coding: utf-8 -*-
# __author__ = 'zouxuan'
from biocluster.agent import Agent
from biocluster.tool import Tool
import os
from biocluster.core.exceptions import OptionError
import shutil


class CdhitSplitFastaAgent(Agent):
    """
    cd-hit-div
    version v1.0
    author: zouxuan
    last modified:2017.9.12
    """

    def __init__(self, parent):
        super(CdhitSplitFastaAgent, self).__init__(parent)
        options = [
            {"name": "gene_tmp_fa", "type": "infile", "format": "sequence.fasta"},  # 输入序列
            {"name": "number", "type": "int", "default": 1},  # 切分为几份
            {"name": "ou_dir", "type": "string", "default": ""}  # 输出路径
        ]
        self.add_option(options)
        self.step.add_steps('cdhitsplitfasta')
        self.on('start', self.step_start)
        self.on('end', self.step_end)

    def step_start(self):
        self.step.cdhitsplitfasta.start()
        self.step.update()

    def step_end(self):
        self.step.cdhitsplitfasta.finish()
        self.step.update()

    def check_options(self):
        """
        重写参数检测函数
        """
        if not self.option("gene_tmp_fa").is_set:
            raise OptionError("必须设置参数gene_tmp_fa")
        if self.option("number") <= 0:
            raise OptionError("切割份数必须大于等于1")
        if not self.option("ou_dir").strip():
            raise OptionError("必须设置参数ou_dir")

    def set_resource(self):
        """
        设置所需资源
        """
        self._cpu = 2
        self._memory = '4G'

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"],
        ])
        super(CdhitSplitFastaAgent, self).end()


class CdhitSplitFastaTool(Tool):
    def __init__(self, config):
        super(CdhitSplitFastaTool, self).__init__(config)
        self._version = '1.0'
        self.div_path = 'bioinfo/uniGene/cd-hit-v4.5.7-2011-12-16/cd-hit-div'

    def run(self):
        self.div()
        self.set_output()

    #    def div_num(self):
    #        filesize = os.path.getsize(self.option("gene_tmp_fa").prop['path'])
    #        n = filesize/500000000 + 1
    #        return n

    def div(self):
        n = self.option("number")
        if os.path.exists(self.option("ou_dir")):
            pass
        else:
            os.mkdir(self.option("ou_dir"))
        cmd = '%s -i %s -o %s -div %s' % (
            self.div_path, self.option("gene_tmp_fa").prop['path'], self.option("ou_dir") + "/gene.geneset.tmp.fa.div",
            n)
        self.logger.info(cmd)
        command1 = self.add_command('cmd_1', cmd)
        command1.run()
        self.wait(command1)
        if command1.return_code == 0:
            self.logger.info("div succeed")
        else:
            self.set_error("div failed")
            raise Exception("div failed")

    def set_output(self):
        self.linkdir(self.option("ou_dir"), "")
        self.end()

    def linkdir(self, dirpath, dirname):
        """
        link文件夹下的所有文件到本module的output目录
        """
        allfiles = os.listdir(dirpath)
        newdir = os.path.join(self.output_dir, dirname)
        if not os.path.exists(newdir):
            os.mkdir(newdir)
        oldfiles = [os.path.join(dirpath, i) for i in allfiles if os.path.isfile(os.path.join(dirpath,i))]
        newfiles = [os.path.join(newdir, i) for i in allfiles if os.path.isfile(os.path.join(dirpath,i))]
        for newfile in newfiles:
            if os.path.exists(newfile):
                os.remove(newfile)
        for i in range(len(allfiles)):
            os.link(oldfiles[i], newfiles[i])
