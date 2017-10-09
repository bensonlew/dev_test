# -*- coding: utf-8 -*-
# __author__ = 'zhouxuan'
# last modify by shaohua.yuan
# last modify date: 20170913

from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from mbio.packages.annotation.nr_stat import nr_stat
import os


class MgNrTaxLevelAgent(Agent):
    """
    nr注释的level统计
    """

    def __init__(self, parent):
        super(MgNrTaxLevelAgent, self).__init__(parent)
        options = [
            {"name": "nr_taxon_profile_dir", "type": "infile", "format": "annotation.mg_anno_dir"},
            {"name": "nr_taxon_anno_dir", "type": "infile", "format": "annotation.mg_anno_dir"}
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
        super(MgNrTaxLevelAgent, self).end()


class MgNrTaxLevelTool(Tool):
    def __init__(self, config):
        super(MgNrTaxLevelTool, self).__init__(config)
        self._version = "1.0"
        self.python_path = "program/Python/bin/python"
        self.python_script = self.config.SOFTWARE_DIR + '/bioinfo/taxon/scripts/mg_nr_taxlevel.py'
        self.sh_path = 'bioinfo/align/scripts/cat.sh'
        self.result_name = ''

    def run(self):
        """
        运行
        :return:
        """
        super(MgNrTaxLevelTool, self).run()
        self.merge_anno_table()
        self.merge_profile_table()
        self.tax_level()
        self.set_output()
        self.end()

    def merge_anno_table(self):
        nr_anno = 0
        anno_file = os.listdir(self.option('nr_taxon_anno_dir').prop['path'])
        self.anno_name = os.path.join(self.work_dir, "tmp_taxons_anno.xls")
        if os.path.exists(self.anno_name):
            os.remove(self.anno_name)
        for i in anno_file:
            nr_anno += 1
            file_path = os.path.join(self.option('nr_taxon_anno_dir').prop['path'], i)
            cmd = '{} {} {}'.format(self.sh_path, file_path, self.anno_name)
            self.logger.info("start cat {}".format(i))
            command_name = "cat anno" + str(nr_anno)
            command = self.add_command(command_name, cmd).run()
            self.wait(command)
            if command.return_code == 0:
                self.logger.info("cat {} done".format(i))
            else:
                self.set_error("cat {} error".format(i))
                raise Exception("cat {} error".format(i))
        nr = nr_stat()
        self.logger.info("start nr_stat(detail_to_level)")
        nr.detail_to_level(detail_file = self.anno_name, out_dir = self.work_dir)
        with open(self.work_dir + "/query_taxons.xls","r") as f, open(self.output_dir + "/gene_nr_anno.xls","w") as outfile:
            outfile.write("#Query\tDomain\tKingdom\tPhylum\tClass\tOrder\tFamily\tGenus\tSpecies\n")
            for line in f:
                line = line.strip().split("\t")
                gene = line[0]
                tax = "\t".join(line[1].split(";"))
                outfile.write(gene + "\t" + tax + "\n")
        
    def merge_profile_table(self):
        nr_number = 0
        profile_file = os.listdir(self.option('nr_taxon_profile_dir').prop['path'])
        self.result_name = os.path.join(self.work_dir, "tmp_taxons_profile.xls")
        if os.path.exists(self.result_name):
            os.remove(self.result_name)
        for i in profile_file:
            nr_number += 1
            file_path = os.path.join(self.option('nr_taxon_profile_dir').prop['path'], i)
            cmd = '{} {} {}'.format(self.sh_path, file_path, self.result_name)
            self.logger.info("start cat {}".format(i))
            command_name = "cat profile" + str(nr_number)
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
            format(self.python_script, self.result_name, self.output_dir)
        command2 = self.add_command('nr_tax_level', cmd2).run()
        self.wait(command2)
        if command2.return_code == 0:
            self.logger.info("nr_tax_level succeed")
        else:
            self.set_error("nr_tax_level failed")
            raise Exception("nr_tax_level failed")

    def set_output(self):
        self.logger.info("start set_output")
