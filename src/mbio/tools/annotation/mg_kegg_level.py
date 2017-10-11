# -*- coding: utf-8 -*-
# __author__ = 'zhouxuan'

from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from mbio.packages.align.blast.xml2table import xml2table
import subprocess
import os


class MgKeggLevelAgent(Agent):
    """
    宏基因kegg注释level水平丰度计算
    author: zhouxuan
    last_modify: 2017.09.25
    last_modify by: shaohua.yuan
    """

    def __init__(self, parent):
        super(MgKeggLevelAgent, self).__init__(parent)
        options = [
            {"name": "kegg_result_dir", "type": "infile", "format": "annotation.mg_anno_dir"},
            {"name": "reads_profile", "type": "infile", "format": "sequence.profile_table"},
        ]
        self.add_option(options)

    def check_options(self):
        if not self.option("kegg_result_dir").is_set:
            raise OptionError("必须设置输入文件")
        if not self.option("reads_profile").is_set:
            raise OptionError("必须设置基因丰度表")
        return True

    def set_resource(self):
        self._cpu = 2
        self._memory = '5G'

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"],
        ])
        super(MgKeggLevelAgent, self).end()


class MgKeggLevelTool(Tool):
    def __init__(self, config):
        super(MgKeggLevelTool, self).__init__(config)
        self._version = "1.0"
        self.perl_path = '/program/perl-5.24.0/bin/perl'
        self.perl_script = self.config.SOFTWARE_DIR + '/bioinfo/annotation/scripts/mg_kegg_level_abu.pl'
        self.python_path = "program/Python/bin/python"
        self.python_script = self.config.SOFTWARE_DIR + '/bioinfo/annotation/scripts/mg_kegg_level_mongo.py'
        self.sh_path = 'bioinfo/align/scripts/cat.sh'
        self.anno_result = ''
        self.level_anno = ''

    def run(self):
        """
        运行
        :return:
        """
        super(MgKeggLevelTool, self).run()
        self.merge_table()
        self.run_kegg_level_anno()
        self.run_kegg_stat()
        self.set_output()
        self.end()

    def merge_table(self):
        kegg_number = 0
        profile_file = os.listdir(self.option('kegg_result_dir').prop['path'])
        self.anno_result = os.path.join(self.work_dir, "tmp_kegg_anno.xls")
        if os.path.exists(self.anno_result):
            os.remove(self.anno_result)
        for i in profile_file:
            if "kegg_anno_result" in i:
                kegg_number += 1
                file_path = os.path.join(self.option('kegg_result_dir').prop['path'], i)
                cmd = '{} {} {}'.format(self.sh_path, file_path, self.anno_result)
                self.logger.info("start cat {}".format(i))
                command_name = "cat" + str(kegg_number)
                command = self.add_command(command_name, cmd).run()
                self.wait(command)
                if command.return_code == 0:
                    self.logger.info("cat {} done".format(i))
                else:
                    self.set_error("cat {} error".format(i))
                    raise Exception("cat {} error".format(i))

    def run_kegg_level_anno(self):
        kegg_anno = self.anno_result
        self.level_anno =  self.work_dir + "/kegg_level_anno.xls"
        cmd = self.python_path + ' {} -i {} -o {} '.format(self.python_script, kegg_anno, self.work_dir)
        self.logger.info(cmd)
        command = self.add_command('kegg_level_anno', cmd).run()
        self.wait(command)
        if command.return_code == 0:
            self.logger.info("kegg_level_anno succeed")
        else:
            self.set_error("kegg_level_anno failed")
            raise Exception("kegg_level_anno failed")

    def run_kegg_stat(self):
        cmd = self.perl_path + ' {} -q {} -p {} -o {} '. \
            format(self.perl_script, self.level_anno, self.option('reads_profile').prop['path'], self.output_dir)
        self.logger.info(cmd)
        command = self.add_command('kegg_stat', cmd).run()
        self.wait(command)
        if command.return_code == 0:
            self.logger.info("kegg_stat succeed")
        else:
            self.set_error("kegg_stat failed")
            raise Exception("kegg_stat failed")

    def set_output(self):
        newfile = self.output_dir + "/gene_kegg_anno.xls"
        if os.path.exists(newfile):
            os.remove(newfile)
        os.link(self.work_dir + "/gene_kegg_anno_all.xls", newfile)
        self.logger.info("set_output")
        if len(os.listdir(self.output_dir)) == 4:
            self.logger.info("OUTPUT RIGHT")
        else:
            raise Exception("OUTPUT WRONG")
