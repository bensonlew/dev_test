# -*- coding: utf-8 -*-
# __author__ = 'zhouxuan'
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
import os
import re


class MgCogStatAgent(Agent):
    """
    宏基因cog注释结果统计.py v1.0
    author: zhouxuan
    last_modify: 2017.09.14
    last_modify by :shaohua.yuan
    """

    def __init__(self, parent):
        super(MgCogStatAgent, self).__init__(parent)
        options = [
            {"name": "cog_table_dir", "type": "infile", "format": "annotation.mg_anno_dir"},
            # 比对到eggNOG库的注释结果文件
            {"name": "reads_profile_table", "type": "infile", "format": "sequence.profile_table"}
        ]
        self.add_option(options)

    def check_options(self):
        if not self.option("cog_table_dir").is_set:
            raise OptionError("必须设置输入文件")
        if not self.option('reads_profile_table').is_set:
            raise OptionError
        return True

    def set_resource(self):
        self._cpu = 5
        self._memory = '10G'

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"],
        ])
        super(MgCogStatAgent, self).end()


class MgCogStatTool(Tool):
    def __init__(self, config):
        super(MgCogStatTool, self).__init__(config)
        self._version = "1.0"
        self.perl_path = '/program/perl-5.24.0/bin/perl'
        self.script = self.config.SOFTWARE_DIR  + '/bioinfo/annotation/scripts/eggNOG_anno_abundance.pl'
        self.result_name = ''
        self.sh_path = 'bioinfo/align/scripts/cat.sh'

    def run(self):
        """
        运行
        :return:
        """
        super(MgCogStatTool, self).run()
        self.merge_table()
        self.run_cog_stat()
        self.set_output()
        self.end()

    def merge_table(self):
        cog_number = 0
        profile_file = os.listdir(self.option('cog_table_dir').prop['path'])
        self.result_name = os.path.join(self.output_dir, "cog_anno_result.xls")
        if os.path.exists(self.result_name):
            os.remove(self.result_name)
        for i in profile_file:
            cog_number += 1
            file_path = os.path.join(self.option('cog_table_dir').prop['path'], i)
            cmd = '{} {} {}'.format(self.sh_path, file_path, self.result_name)
            self.logger.info("start cat {}".format(i))
            command_name = "cat" + str(cog_number)
            command = self.add_command(command_name, cmd).run()
            self.wait(command)
            if command.return_code == 0:
                self.logger.info("cat {} done".format(i))
            else:
                self.set_error("cat {} error".format(i))
                raise Exception("cat {} error".format(i))

    def run_cog_stat(self):
        self.logger.info("start cog_stat")
        cmd = "{} {} -q {} -p {} -o {}".format(self.perl_path, self.script, self.result_name,
                                               self.option('reads_profile_table').prop['path'], self.output_dir)
        command = self.add_command('tax_profile', cmd).run()
        self.wait(command)
        if command.return_code == 0:
            self.logger.info("cog_stat succeed")
        else:
            self.set_error("cog_stat failed")
            raise Exception("cog_stat failed")

    def set_output(self):
        self.logger.info("start set_output")
