# -*- coding: utf-8 -*-
# __author__ = 'shaohua.yuan'

from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from mbio.packages.align.blast.xml2table import xml2table
import subprocess
import os


class ArdbAnnoAgent(Agent):
    """
    宏基因组比对结果文件注释,先将xml文件转换成table并合并成一张table，进行anno详细注释
    author: shaohua.yuan
    last_modify:
    """

    def __init__(self, parent):
        super(ArdbAnnoAgent, self).__init__(parent)
        options = [
            {"name": "ardb_xml_dir", "type": "infile", "format": "align.blast.blast_xml_dir"},  # 比对到ardb库的xml文件夹
            {"name": "ardb_anno_result", "type": "outfile", 'format': "sequence.profile_table"}  # 注释详细结果表
            ]
        self.add_option(options)
        #self.ardb_number = 0
        self.step.add_steps("merge","ardb_anno")
        self.result_name = ''
        #self.on('start', self.stepstart)
        #self.on('end', self.stepfinish)

    def check_options(self):
        if not self.option("ardb_xml_dir").is_set:
            raise OptionError("必须设置输入文件")
        return True

    def set_resource(self):
        self._cpu = 2
        self._memory = '5G'

    def end(self):
        self.option('ardb_anno_result',os.path.join(self.output_dir,"gene_ardb_anno.xls"))
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"],
            ['gene_ardb_anno.xls', 'xls', '序列详细物种分类文件']
            ])
        super(ArdbAnnoAgent, self).end()


class ArdbAnnoTool(Tool):
    def __init__(self, config):
        super(ArdbAnnoTool, self).__init__(config)
        self._version = "1.0"
        self.python_path = self.config.SOFTWARE_DIR + "/program/Python/bin/python"
        #self.python_path = "program/Python/bin/python"
        self.python_script = self.config.SOFTWARE_DIR + '/bioinfo/annotation/scripts/meta_ardb_mongo.py'
        self.sh_path = 'bioinfo/align/scripts/cat.sh'
        self.result_name = ''

    def run(self):
        """
        运行
        :return:
        """
        super(ArdbAnnoTool, self).run()
        self.set_output()
        self.merge_table()
        self.run_ardb_anno()
        self.end()

    def merge_table(self):
        self.ardb_number = 0
        xml_file = os.listdir(self.option('ardb_xml_dir').prop['path'])
        self.result_name = os.path.join(self.output_dir, "ardb_align_table.xls")
        if os.path.exists(self.result_name):
            os.remove(self.result_name)
        n = 0
        for i in xml_file:
            n += 1
            self.ardb_number += 1
            file_path = os.path.join(self.option('ardb_xml_dir').prop['path'], i)
            table = xml2table(file_path, self.work_dir + "/tmp_ardb_anno/" + "ardb_" + str(self.ardb_number) + "_table.xls")
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

    def run_ardb_anno(self):
        cmd = '{} {} -i {} -o {}'.format(self.python_path, self.python_script,self.result_name , self.output_dir + "/gene_ardb_anno.xls")
        #command = self.add_command("anno", cmd).run()
        self.logger.info(cmd)
        #self.wait(command)
        try:
            subprocess.check_output(cmd, shell=True)
            self.logger.info('运行ardb_anno完成')
        except subprocess.CalledProcessError:
            self.set_error('运行ardb_anno出错')

    def set_output(self):
        if os.path.exists(self.work_dir + '/tmp_ardb_anno'):
                pass
        else:
                os.mkdir(self.work_dir + '/tmp_ardb_anno')
        #self.option('ardb_anno_result',os.path.join(self.output_dir,"gene_ardb_anno.xls"))
        #if len(os.listdir(self.output_dir)) == 1:
        #    self.logger.info("output right")


