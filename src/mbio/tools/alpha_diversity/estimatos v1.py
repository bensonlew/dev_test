#!/usr/bin/env python
# -*- coding: utf-8 -*-
from biocluster.agent import Agent
from biocluster.tool import Tool
import os
from biocluster.core.exceptions import OptionError


class EstimatorsAgent(Agent):
    """
    estimators:用于生成所有样本的指数表 #undone
    version 1.0  
    author: qindanhua  
    last_modify: 2015.11.04  
    """
    def __init__(self, parent):
        super(EstimatorsAgent, self).__init__(parent)
        options = [
            {"name": "otutable", "type": "infile", "format": "xls"},  # 输入文件
            {"name": "indices", "type": "string", "default": "ace-chao-shannon-simpson"},  # 指数类型
            {"name": "estimators", "type": "outfile", "format": "txt"} # 输出结果
        ]
        self.add_option(options)

    def check_options(self):
        """
        检测参数是否正确
        """
        if not self.option("otutable").is_set:
            raise OptionError(u"请选择otu表")

    def set_resource(self):
        """
        所需资源
        """
        self._cpu = 10
        self._memory = ''



class EstimatorsTool(object):
    """
    version 1.0
    """
    def __init__(self, config):
        super(EstimatorsTool, self).__init__(config)
        self.cmd_path = 'meta/alpha/estimators'


    def cmd1(self):
        """
        返回pl脚本命令
        """
        cmd1 = os.path.join(self.cmd_path, 'otu2shared.pl')
        cmd1 += '-i otu_table.xls -l 0.97 -o otu.shared' 
        return cmd1

    def cmd2(self):
        """
        返回mothur运行生成各样本指数值文件命令
        """
        cmd2 = 'mothur "#summary.single(shared=otu.shared,groupmode=f,calc=ace-chao-shannon-simpson)"'
        return cmd2

    def cmd3(self):
        """
        返回py脚本命令，生成结果文件
        """
        cmd3 = os.path.join(self.cmd_path, 'estimators.py')
        cmd3 += '-path %s' % self.work_dir
        return cmd3

    def set_output(self):
        os.link(self.work_dir+'estimators', self.output_dir+'estimators')
        self.option('estimators', value=self.output_dir+'estimators')

    def run(self):
        super(EstimatorsTool,self).run()
        i = 0
        while i < 4:
            i += 1
            self.logger.info(u"开始运行cmd"+i)
            cmd = getattr(self, 'cmd'+i)()
            command = self.add_command('cmd'+i, cmd)
            command.run()
            self.wait(command)
            if command.return_code == 0:
                self.logger.info(u"运行cmd"+i+u"完成")
                self.end()
            else:
                self.set_error(u"cmd"+i+u"运行出错!")
                break
        self.set_output()




            