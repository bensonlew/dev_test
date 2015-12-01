#!/usr/bin/env python
# -*- coding: utf-8 -*-

from biocluster.agent import Agent
from biocluster.tool import Tool
import os
from biocluster.core.exceptions import OptionError


class PhyloTreeAgent(Agent):
    """
    phylo_tree:生成OTU代表序列的树文件
    version 1.0  
    author: qindanhua  
    last_modify: 2015.11.10  
    """

    def __init__(self, parent):
        super(PhyloTreeAgent, self).__init__(parent)
        options = [
            {"name": "otu_reps.fasta", "type": "infile", "format": "sequence.fasta"},  # 输入文件
            {"name": "phylo.tre", "type": "outfile", "format": "meta.beta_diversity.newick_tree"}  # 输出结果
        ]
        self.add_option(options)

    def check_options(self):
        """
        检查参数是否正确
        """
        if not self.option("otu_reps.fasta").is_set:
            raise OptionError("请传入OTU代表序列文件")

    def set_resource(self):
        """
        所需资源
        """
        self._cpu = 10
        self._memory = ''


class PhyloTreeTool(Tool):
    """
    version 1.0
    """

    def __init__(self, config):
        super(PhyloTreeTool, self).__init__(config)
        self.cmd_path = 'meta/otu/'
        self.clustalw2_path = '/align/'
        self.FastTree_path = '/mnt/ilustre/users/sanger/app/meta/scripts/'

    def clustalw(self):
        """
        执行clustalw2命令,生成中间文件phylo.clustalw.align
        """
        cmd = self.clustalw2_path + "clustalw2 -ALIGN -INFILE=%s -OUTFILE=phylo.clustalw.align  -OUTPUT=FASTA" % \
                                    self.option('otu_reps.fasta').prop['path']
        print cmd
        # os.system(cmd)
        self.logger.info("开始运行clustalw2")
        clustalw_command = self.add_command("clustalw2", cmd)
        clustalw_command.run()
        self.wait()
        if clustalw_command.return_code == 0:
            self.logger.info("运行clustal2完成！")
        else:
            self.set_error("运行clastal2出错！")

    def fasttree(self):
        """
        执行fasttree脚本，生成结果文件
        """
        cmd = 'Python/bin/python %sfasttree.py -i %s' % (self.FastTree_path, 'phylo.clustalw.align')
        print cmd
        self.logger.info("开始运行fasttree")
        fasttree_command = self.add_command("fasttree", cmd)
        fasttree_command.run()
        self.wait()
        if fasttree_command.return_code == 0:
            self.logger.info("运行fasttree完成！")
        else:
            self.set_error("运行fasttree出错！")
        self.set_output()

    def set_output(self):
        """
        设置输出文件
        """
        self.logger.info("set out put")
        for f in os.listdir(self.output_dir):
            os.remove(os.path.join(self.output_dir, f))
        os.link(self.work_dir+'/phylo.tre', self.output_dir+'/phylo.tre')
        self.option('phylo.tre').set_path(self.output_dir+'/phylo.tre')
        self.logger.info("done")

    def run(self):
        """
        运行
        """
        super(PhyloTreeTool, self).run()
        self.clustalw()
        self.fasttree()
        self.end()
        # self.fasttree()
        # self.set_output()
