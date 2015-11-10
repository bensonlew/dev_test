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

    def __init__(self,parent):
        super(PhyloTreeAgent, self).__init__(parent)
        options = [
            {"name": "otu_reps.fasta", "type": "infile", "format": "fasta"},  # 输入文件
            {"name": "phylo.tre", "type": "outfile", "format": "newick"}  # 输出结果
        ]
        self.add_option(options)

    def check_options(self):
        """
        检查参数是否正确
        """
        if not self.option("otu_reps.fasta").is_set:
            raise OptionError(u"请传入OTU代表序列文件")

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
    def __init__(self,config):
        super(RarefactionTool, self).__init__(config)
        self.cmd_path = 'meta/otu/'

    def phylo_tree(self):
        cmd = '%s\n' % 'clustalw2 -ALIGN -INFILE=otu_reps.fasta -OUTFILE=phylo.clustalw.align  -OUTPUT=FASTA'
        cmd += 'FastTree -nt phylo.clustalw.align > phylo.tre'
        self.logger.info(u"开始运行phylo_tree")
        command = self.add_command("phylo_tree", cmd)
        command.run()
        self.wait()
        if command.return_code == 0:
            self.logger.info(u"运行phylo_tree完成！")
            self.end()
        else:
            self.set_error(u"运行phylo_tree出错！")
            break
        self.set_output()

    def set_output(self):
        os.link(self.work_dir+'phylo.tre', self.output_dir+'phylo.tre')
        self.option('phylo.tre', value=self.output_dir+'phylo.tre')

    def run(self):
        """
        运行
        """
        super(PhyloTreeTool,self).run()
        self.phylo_tree()
        
