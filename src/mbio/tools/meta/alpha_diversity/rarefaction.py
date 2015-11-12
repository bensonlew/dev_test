#!/usr/bin/env python
# -*- coding: utf-8 -*-
from biocluster.agent import Agent
from biocluster.tool import Tool
import os
from biocluster.core.exceptions import OptionError


class RarefactionAgent(Agent):
    """
    rarefaction:稀释曲线 #undone
    version 1.0  
    author: qindanhua  
    last_modify: 2015.11.10  
    """
    ESTIMATORS = ['sobs','chao','ace','jack','bootstrap','simpsoneven',
    'shannoneven','heip','smithwilson','bergerparker','shannon',
    'npshannon','simpson','invsimpson','coverage','qstat']

    def __init__(self, parent):
        super(EstimatosAgent, self).__init__(parent)
        options = [
            {"name": "otutable", "type": "infile", "format": "xls"},  # 输入文件            
            {"name": "indices", "type": "string", "default": "chao-shannon"},  # 指数类型
            {"name": "random_number", "type": "int", "default": 100}, #随机取样数
            {"name": "rarefaction", "type": "outfile", "format": "rarefaction"} # 输出结果
        ]
        self.add_option(options)

    def check_options(self):
        """
        检测参数是否正确
        """
        if not self.option("otutable").is_set:
            raise OptionError(u"请选择otu表")
        for estimators in self.options('indices').split('-'):
            if not estimators in ESTIMATORS:
                raise OptionError(u"请选择正确的指数类型")

    def set_resource(self):
            """
            所需资源
            """
            self._cpu = 10
            self._memory = ''


class RarefactionTool(Tool):
    """
    version 1.0

    """
    def __init__(self, config):
        super(RarefactionTool, self).__init__(config)
        self.cmd_path = 'meta/alpha/'

    def rarefaction(self):
        cmd = os.path.join(self.cmd_path,'otu2shared.pl')
        cmd += ' -i %s -l %s -o %s \n' % ('otu_table.xls','0.97','otu.shared')
        cmd +='mothur "#rarefaction.single(shared=otu.shared,calc=sobs-%s,groupmode=f,freq=%s,processors=10)"'
                                            %('-'.join(self.option('indices')),self.option('random_number'))
        for estimators in self.options('indices').split('-'):
            cmd +='\n mkdir %s|find -name "otu*%s"|xargs mv -t %s \n' %(estimators,estimators,estimators)
        self.logger.info(u"开始运行rarefacton")
        rarefaction_command = self.add_command("rarefaction", cmd)
        rarefaction_command.run()
        self.wait()
        if rarefaction_command.return_code == 0:
            self.logger.info(u"运行rarefaction完成！")
            self.end()
        else:
            self.set_error(u"运行rarefaction出错！")
            break
        self.set_output()
    
    def set_output(self):
        os.link(self.work_dir+'rarefaction', self.output_dir+'rarefaction')
        self.option('rarefaction', value=self.output_dir+'rarefaction')
        
    def run(self):
        """
        运行
        """
        super(RarefactionTool,self).run()
        self.rarefaction()





            