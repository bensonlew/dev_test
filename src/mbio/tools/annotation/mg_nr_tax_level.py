# -*- coding: utf-8 -*-
# __author__ = 'zhouxuan'

from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError


class NrTaxLevelAgent(Agent):
    """
    nr注释的level统计
    author: zhouxuan
    last_modify: 2017.0911
    last_modify by shaohua.yuan
    """

    def __init__(self, parent):
        super(NrTaxLevelAgent, self).__init__(parent)
        options = [
            {"name": "nr_taxon_profile_dir", "type": "infile", "format": "annotation.mg_anno_dir"},
        ]
        self.add_option(options)

    def check_options(self):
        if not self.option("nr_taxon_profile_dir").is_set:
            raise OptionError("必须设置输入文件")
        return True

    def set_resource(self):
        self._cpu = 2
        self._memory = '5G'

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"],
            ['query_taxons_detail.xls', 'xls', '序列详细物种分类文件']
        ])
        super(NrTaxLevelAgent, self).end()


class NrTaxLevelTool(Tool):
    def __init__(self, config):
        super(NrTaxLevelTool, self).__init__(config)
        self._version = "1.0"
        self.python_path = "program/Python/bin/python"
        self.python_script_2 = self.config.SOFTWARE_DIR + '/bioinfo/taxon/scripts/metagen_nr_taxlevel.py'

    def run(self):
        """
        运行
        :return:
        """
        super(NrTaxLevelTool, self).run()
        self.tax_level()
        self.set_output()
        self.end()

    def merge_table(self):
        self.nr_number = 0
        profile_file = os.listdir(self.option('nr_taxon_profile_dir').prop['path'])
        self.result_name = os.path.join(self.output_dir, "query_taxons_detail.xls")
        if os.path.exists(self.result_name):
            os.remove(self.result_name)
        n = 0
        for i in profile_file:
            n += 1
            self.nr_number += 1
            file_path = os.path.join(self.option('nr_taxon_profile_dir').prop['path'], i)

            cmd = '{} {} {}'.format(self.sh_path, table, self.result_name)
            self.logger.info("start cat {}".format(i))
            command_name = "cat" + str(n)
            command = self.add_command(command_name, cmd).run()
            self.wait(command)
            if command.return_code == 0:
                self.logger.info("cat {} done".format(i))
            else:
                self.set_error("cat {} error".format(i))
                raise Exception("cat {} error".format(i))

    def tax_level(self):
        self.logger.info("start nr_tax_level")
        cmd2 = self.python_path + ' {} -i {} -l 1,2,3,4,5,6,7,8 -o {}'. \
            format(self.python_script_2, self.option('nr_taxon_profile').prop['path'], self.output_dir)
        command2 = self.add_command('nr_tax_level', cmd2).run()
        self.wait(command2)
        if command2.return_code == 0:
            self.logger.info("nr_tax_level succeed")
        else:
            self.set_error("nr_tax_level failed")
            raise Exception("nr_tax_level failed")

    def set_output(self):
        self.logger.info("start set_output")
