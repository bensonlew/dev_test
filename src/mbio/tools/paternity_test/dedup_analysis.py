## !/mnt/ilustre/users/sanger-dev/app/program/Python/bin/python
# -*- coding: utf-8 -*-
# __author__ = "moli.zhou"
#last_modify:20161121
import re
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from biocluster.config import Config
import os

class DedupAnalysisAgent(Agent):
    """
    合并家族之后的一系列分析
    包括父权值、有效率、无效率、错配率等等
    version v1.0
    author: moli.zhou
    last_modify: 2016.11.21
    """
    def __init__(self, parent):
        super(DedupAnalysisAgent, self).__init__(parent)
        options = [#输入的参数
            # {"name": "dad_tab", "type": "infile", "format": "paternity_test.tab"},
            {"name": "dad_list", "type": "string"},
            {"name": "mom_tab", "type": "infile", "format": "paternity_test.tab"},
            {"name": "preg_tab", "type": "infile", "format": "paternity_test.tab"},
            {"name": "ref_point", "type": "infile", "format": "paternity_test.rda"},
            {"name": "err_min", "type": "int", "default": 2},
        ]
        self.add_option(options)
        self.step.add_steps("family_analysis")
        self.on('start', self.stepstart)
        self.on('end', self.stepfinish)

    def stepstart(self):
        self.step.family_analysis.start()
        self.step.update()

    def stepfinish(self):
        self.step.family_analysis.finish()
        self.step.update()


    def check_options(self):
        """
        重写参数检测函数
        :return:
        """
        # if not self.option('query_amino'):
        #     raise OptionError("必须输入氨基酸序列")
        if not self.option('dad_list') :
            raise OptionError("必须提供父本tab")
        if not self.option('mom_tab') :
            raise OptionError("必须提供母本tab")
        if not self.option('preg_tab') :
            raise OptionError("必须提供胎儿tab")
        return True

    def set_resource(self):
        """
        设置所需资源，需在之类中重写此方法 self._cpu ,self._memory
        :return:
        """
        self._cpu = 6
        self._memory = '50G'

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"]
        ])
        result_dir.add_regexp_rules([
            ["family_analysis.Rdata", "Rdata", "父权值等计算"],
        ])
        super(DedupAnalysisAgent, self).end()


class DedupAnalysisTool(Tool):
    """
    蛋白质互作组预测tool
    """
    def __init__(self, config):
        super(DedupAnalysisTool, self).__init__(config)
        self._version = '1.0.1'

        self.R_path = 'program/R-3.3.1/bin/'
        self.script_path = self.config.SOFTWARE_DIR + '/bioinfo/medical/scripts/'
        self.set_environ(PATH=self.config.SOFTWARE_DIR + '/gcc/5.1.0/bin')
        self.set_environ(LD_LIBRARY_PATH=self.config.SOFTWARE_DIR + '/gcc/5.1.0/lib64')

    def run_tf(self):
        dad_list = self.option('dad_list').split(',')
        n = 0
        for dad_tab in dad_list:

            tab2family_cmd = "{}Rscript {}family_joined.R {} {} {} {} {}". \
                format(self.R_path, self.script_path, dad_tab,
                    self.option("mom_tab").prop['path'], self.option("preg_tab").prop['path'],
                    self.option("err_min"), self.option("ref_point").prop['path'])
            self.logger.info(tab2family_cmd)
            self.logger.info("开始运行家系合并")
            cmd = self.add_command("tab2family_cmd_{}".format(n), tab2family_cmd).run()
            self.wait(cmd)

            if cmd.return_code == 0:
                self.logger.info("运行家系合并成功")
            else:
                self.set_error('运行家系{}合并出错'.format(dad_tab))
                raise Exception("运行家系合并出错")

            dad = re.match(".*(WQ[0-9]*-F.*)\.tab",dad_tab)
            dad_name = dad.group(1)
            mom = re.match(".*(WQ[0-9]*-M.*)\.tab",self.option("mom_tab").prop['path'])
            mom_name = mom.group(1)
            preg = re.match(".*(WQ[0-9]*-S.*)\.tab", self.option("preg_tab").prop['path'])
            preg_name = preg.group(1)

            tab_name = dad_name + '_' +mom_name+'_'+preg_name+'_family_joined_tab.Rdata'
            if not self.script_path+'/'+ tab_name:
                continue
            else:
                analysis_cmd = "{}Rscript {}data_analysis.R {}".\
                    format(self.R_path,self.script_path,tab_name)
                self.logger.info(analysis_cmd)
                self.logger.info("开始运行家系的分析")
                cmd = self.add_command("analysis_cmd_{}".format(n), analysis_cmd).run()
                self.wait(cmd)

                if cmd.return_code == 0:
                    self.logger.info("运行家系分析成功")
                else:
                    self.set_error('运行家系{}分析出错'.format(dad_tab))
                    raise Exception("运行家系分析出错")

            n = n+1

    def set_output(self):
        """
        将结果文件link到output文件夹下面
        :return:
        """
        for root, dirs, files in os.walk(self.output_dir):
            for names in files:
                os.remove(os.path.join(root, names))
        self.logger.info("设置结果目录")
        results = os.listdir(self.work_dir)
        for f in results:
            if re.search(r'.*family_analysis\.Rdata$', f):
                os.link(self.work_dir + '/' + f, self.output_dir + '/' + f)
            elif re.search(r'.*family_analysis\.txt$', f):
                os.link(self.work_dir + '/' + f, self.output_dir + '/' + f)
            elif re.search(r'.*family_joined_tab\.Rdata$', f):
                os.link(self.work_dir + '/' + f, self.output_dir + '/' + f)
            elif re.search(r'.*family_joined_tab\.txt$', f):
                os.link(self.work_dir + '/' + f, self.output_dir + '/' + f)
        self.logger.info('设置文件夹路径成功')

    def run(self):
        super(DedupAnalysisTool, self).run()
        self.run_tf()
        self.set_output()
        self.end()

# /mnt/ilustre/users/sanger-dev/app/program/R-3.3.1/bin/Rscript /mnt/ilustre/users/sanger-dev/app/bioinfo/medical/scripts/family_joined.R /mnt/ilustre/users/sanger-dev/workspace/20170502/PatchDcBackup_pt_batch_8991_378/output/WQ170826-F.tab /mnt/ilustre/users/sanger-dev/workspace/20170502/PatchDcBackup_pt_batch_8991_378/output/WQ170826-M.tab /mnt/ilustre/users/sanger-dev/workspace/20170502/PatchDcBackup_pt_batch_8991_378/output/WQ170826-S.tab 1 /mnt/ilustre/users/sanger-dev/sg-users/zhoumoli/pt/targets.bed.rda
