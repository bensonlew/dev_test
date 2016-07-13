#!/usr/bin/env python
# -*- coding: utf-8 -*-

from biocluster.agent import Agent
from biocluster.tool import Tool
import os
import subprocess
from biocluster.core.exceptions import OptionError


class PhyloTreeAgent(Agent):
    """
    phylo_tree:生成OTU代表序列的树文件
    version 1.0
    author: qindanhua
    last_modify: 2016.05.31
    """

    def __init__(self, parent):
        super(PhyloTreeAgent, self).__init__(parent)
        options = [
            {"name": "fasta_file", "type": "infile", "format": "sequence.fasta"},  # 输入文件
            {"name": "phylo_tre", "type": "outfile", "format": "meta.beta_diversity.newick_tree"},  # 输出结果
            {"name": "method", "type": "string", "default": "mafft"}  # 比对方法
        ]
        self.add_option(options)
        self.step.add_steps('phylo_tree')

    def phylo_tree_start_callback(self):
        self.step.phylo_tree.start()
        self.step.update()

    def phylo_tree_end_callback(self):
        self.step.phylo_tree.finish()
        self.step.update()

    def check_options(self):
        """
        检查参数是否正确
        """
        if not self.option("fasta_file").is_set:
            raise OptionError("请传入fasta序列文件")
        if self.option("method") not in ["mafft", "clustalw2"]:
            raise OptionError("请选择正确的比对方法")

    def set_resource(self):
        """
        所需资源
        """
        self._cpu = 10
        self._memory = ''

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"],
            ["./phylo.tre", "tre", "进化树树文件"]
        ])
        print self.get_upload_files()
        super(PhyloTreeAgent, self).end()


class PhyloTreeTool(Tool):
    """
    version 1.0
    """

    def __init__(self, config):
        super(PhyloTreeTool, self).__init__(config)
        self.clustalw2_path = 'bioinfo/align/'
        self.python_path = '/program/Python/bin/'
        self.mafft_path = self.config.SOFTWARE_DIR+'/bioinfo/align/mafft-7.299-with-extensions/bin/'
        self.FastTree_path = os.path.join(self.config.SOFTWARE_DIR, "bioinfo/phylogenetic/fasttree2.1.9/FastTreeMP")

    def align(self):
        """
        比对，根据method参数，选择不同的比对软件进行比对，结果文件为phylo.align
        """
        if self.option("method") in ["mafft"]:
            cmd = "{}mafft {} > phylo.align".\
                format(self.mafft_path, self.option('fasta_file').prop['path'])
        else:
            cmd = self.clustalw2_path + "clustalw2 -ALIGN -INFILE=%s -OUTFILE=phylo.align  -OUTPUT=FASTA" % \
                                    self.option('fasta_file').prop['path']
        print cmd
        self.add_state('phylo_tree_start', data='开始运行程序生成树文件')
        # os.system(cmd)
        self.logger.info(cmd)
        self.logger.info("开始运行{}软件，进行比对".format(self.option("method")))
        command = subprocess.Popen(cmd, shell=True)
        command.communicate()
        if command.returncode == 0:
            self.logger.info("完成比对！")
        else:
            self.set_error("运行出错！")
            raise Exception("比对处错")
        # self.add_state('clustalw_end', data='done')

    def fasttree(self):
        """
        执行fasttree脚本，生成结果文件
        """
        # self.add_state('fasttree_start', data='开始运行fasttree命令，生成树文件')
        cmd = "{} -nt {} > {}".format(self.FastTree_path, os.path.join(self.work_dir, "phylo.align"), os.path.join(self.work_dir, "phylo.tre"))
        self.logger.info(cmd)
        self.logger.info("开始运行fasttree")
        command = subprocess.Popen(cmd, shell=True)
        command.communicate()
        if command.returncode == 0:
            self.logger.info("运行fasttree完成！")
        else:
            self.set_error("运行fasttree出错！")
        self.set_output()
        self.add_state('phylo_tree_end', data='done')

    def set_output(self):
        """
        设置输出文件
        """
        self.logger.info("set out put")
        for f in os.listdir(self.output_dir):
            os.remove(os.path.join(self.output_dir, f))
        os.link(self.work_dir+'/phylo.tre', self.output_dir+'/phylo.tre')
        self.option('phylo_tre').set_path(self.output_dir+'/phylo.tre')
        self.logger.info("done")

    def run(self):
        """
        运行
        """
        super(PhyloTreeTool, self).run()
        self.align()
        self.fasttree()
        self.end()
