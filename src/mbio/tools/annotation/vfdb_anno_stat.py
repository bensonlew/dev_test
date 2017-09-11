# -*- coding: utf-8 -*-
# __author__ = 'shaohua.yuan'

from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
import os
import re

class VfdbAnnoStatAgent(Agent):
    """
    宏基因vfdb注释结果丰度统计表
    author: shaohua.yuan
    last_modify:
    """

    def __init__(self, parent):
        super(VfdbAnnoStatAgent, self).__init__(parent)
        options = [
            {"name": "vfdb_core_anno", "type": "infile", "format": "sequence.profile_table"},
            # 核心库注释表
            {"name": "vfdb_predict_anno", "type": "infile", "format": "sequence.profile_table"},
            #预测数据库注释表
            {"name": "reads_profile_table", "type": "infile", "format": "sequence.profile_table"}
            ]
        self.add_option(options)

    def check_options(self):
        if not self.option("vfdb_core_anno").is_set:
            raise OptionError("找不到核心注释文件")
        if not self.option("vfdb_predict_anno").is_set:
            raise OptionError("找不到预测注释文件")
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
            ['query_taxons_detail.xls', 'xls', '序列详细物种分类文件']
            ])
        """
        super(VfdbAnnoStatAgent, self).end()

class VfdbAnnoStatTool(Tool):
    def __init__(self, config):
        super(VfdbAnnoStatTool, self).__init__(config)
        self._version = "1.0"
        self.script = '/bioinfo/annotation/scripts/vfdb_anno_abudance.pl'

    def run(self):
        """
        运行
        :return:
        """
        super(VfdbAnnoStatTool, self).run()
        self.run_vfdb_stat()
        self.set_output()
        self.end()

    def run_vfdb_stat(self):
        self.logger.info("start vfdb_stat")
        cmd = "{} -c {} -pre {} -p {} -o {}".format(self.script, self.option('vfdb_core_anno').prop['path'],self.option('vfdb_predict_anno').prop['path'],self.option('reads_profile_table').prop['path'], self.output_dir)
        command = self.add_command('vfdb_profile', cmd).run()
        self.wait(command)
        if command.return_code == 0:
            self.logger.info("vfdb_stat succeed")
        else:
            self.set_error("vfdb_stat failed")
            raise Exception("vfdb_stat failed")

    def set_output(self):
        if len(os.listdir(self.output_dir)) == 6:
            self.logger.info("结果文件正确生成")
        else:
            self.logger.info("文件个数不正确，请检查")
            raise Exception("文件个数不正确，请检查")
