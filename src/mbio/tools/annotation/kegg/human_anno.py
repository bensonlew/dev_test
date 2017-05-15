# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
import os


class HumanAnnoAgent(Agent):
    """
    author: shenghe
    last_modify: 2017.05.12
    """

    def __init__(self, parent):
        super(HumanAnnoAgent, self).__init__(parent)
        options = [
            {"name": "blastout", "type": "infile", "format": "align.blast.blast_xml, align.blast.blast_table"}  # 输入文件
            ]
        self.add_option(options)

    def check_options(self):
        if not self.option("blastout").is_set:
            raise OptionError("必须设置输入文件")
        return True

    def set_resource(self):
        self._cpu = 2
        self._memory = '5G'

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"],
            ['pathway_profile.xls', 'xls', 'kegg pathway丰度文件'],
            ['module_profile.xls', 'xls', 'kegg module丰度文件'],
            ])
        super(HumanAnnoAgent, self).end()


class HumanAnnoTool(Tool):
    def __init__(self, config):
        super(HumanAnnoTool, self).__init__(config)
        self._version = "1.0"
        self.humann_scon = self.config.SOFTWARE_DIR + "/bioinfo/annotation/humann-0.99/SConstruct"


    def run(self):
        """
        运行
        :return:
        """
        super(HumanAnnoTool, self).run()
        table_file = self.work_dir + '/temp_blastable.xls'
        if self.option("blastout").format == 'align.blast.blast_xml':
            self.option("blastout").convert2table(table_file)
        else:
            table_file = self.option('blastout').path
        try:
            self.humann_anno(table_file)
            self.end()
        except Exception:
            import traceback
            self.logger.debug(traceback.format_exc())
            self.set_error('humann注释出错！')

    def humann_anno(self, blast_table):
        if not os.path.exists("input"):
            os.makedirs("input")
        os.link(blast_table, "input")
        with open(self.humann_scon) as f, open("SConstruct", 'w') as w:
            w.write(f.read())
        cmd = self.config.SOFTWARE_DIR + "/program/Python/bin/scons " + "--site-dir=" + os.path.dirname(self.humann_scon) + \
        "site_scons " + "-C " + self.work_dir + " -j 2"
        command = self.add_command("humann", cmd)
        command.run()
        self.wait()

        pass
