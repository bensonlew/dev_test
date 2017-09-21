# -*- coding: utf-8 -*-
# __author__ = 'shaohua.yuan'

from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
import os
import re

class CardAnnoStatAgent(Agent):
    """
    宏基因card注释结果丰度统计表
    author: shaohua.yuan
    last_modify:
    """

    def __init__(self, parent):
        super(CardAnnoStatAgent, self).__init__(parent)
        options = [
            {"name": "card_anno_table", "type": "infile", "format": "sequence.profile_table"},
            # 基因注释具体信息结果表
            {"name": "reads_profile_table", "type": "infile", "format": "sequence.profile_table"}
            ]
        self.add_option(options)

    def check_options(self):
        if not self.option("card_anno_table").is_set:
            raise OptionError("必须设置注释文件")
        if not self.option('reads_profile_table').is_set:
            raise OptionError("必须设置基因丰度文件")
        return True

    def set_resource(self):
        self._cpu = 2
        self._memory = '2G'
    
    def end(self):
        """
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"],
            ['query_taxons_detail.xls', 'xls', '序列详细分类文件']
            ])
        """
        super(CardAnnoStatAgent, self).end()

class CardAnnoStatTool(Tool):
    def __init__(self, config):
        super(CardAnnoStatTool, self).__init__(config)
        self._version = "1.0"
        #self.script = self.config.SOFTWARE_DIR + '/bioinfo/annotation/scripts/card_anno_abudance.pl'
        self.script = '/bioinfo/annotation/scripts/card_anno_abudance.pl'

    def run(self):
        """
        运行
        :return:
        """
        super(CardAnnoStatTool, self).run()
        self.run_card_stat()
        self.set_output()
        self.end()

    def run_card_stat(self):
        self.logger.info("start card_stat")
        #cmd = "perl {} -q {} -p {} -o {}".format(self.script, self.option('card_anno_table').prop['path'],\
        #                                       self.option('reads_profile_table').prop['path'], self.output_dir)
        cmd = "{} -q {} -p {} -o {}".format(self.script, self.option('card_anno_table').prop['path'],self.option('reads_profile_table').prop['path'], self.output_dir)
        self.logger.info(cmd)
        command = self.add_command('card_profile', cmd).run()
        self.wait()
        if command.return_code == 0:
            self.logger.info("card_stat succeed")
        else:
            self.set_error("card_stat failed")
            raise Exception("card_stat failed")

    def set_output(self):
        self.logger.info("start set_output")
