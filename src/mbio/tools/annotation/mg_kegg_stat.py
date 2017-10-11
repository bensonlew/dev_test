# -*- coding: utf-8 -*-
# __author__ = 'zhouxuan'
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
import os


class MgKeggStatAgent(Agent):
    """
    宏基因kegg注释结果统计tool 调用脚本 meta_kegg_stat.py
    author: zhouxuan
    last_modify: 2017.0925
    last_modify_by:shaohua.yuan
    """

    def __init__(self, parent):
        super(MgKeggStatAgent, self).__init__(parent)
        options = [
            {"name": "kegg_result_dir", "type": "infile", "format": "annotation.mg_anno_dir"},
            {"name": "reads_profile", "type": "infile", "format": "sequence.profile_table"},
            {"name": "kegg_profile_dir", "type": "outfile", "format": "annotation.mg_anno_dir"},
        ]
        self.add_option(options)

    def check_options(self):
        if not self.option("kegg_result_dir").is_set:
            raise OptionError("必须设置输入文件夹")
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
        super(MgKeggStatAgent, self).end()


class MgKeggStatTool(Tool):
    def __init__(self, config):
        super(MgKeggStatTool, self).__init__(config)
        self._version = "1.0"
        self.python_path = "program/Python/bin/python"
        # self.python_path = self.config.SOFTWARE_DIR + "/program/Python/bin/python"
        self.python_script = self.config.SOFTWARE_DIR + '/bioinfo/annotation/scripts/meta_kegg_stat.py'
        self.sh_path = 'bioinfo/align/scripts/cat.sh'
        self.anno_result = ''
        self.enzyme_list = ''
        self.module_list = ''
        self.pathway_list = ''

    def run(self):
        """
        运行
        :return:
        """
        super(MgKeggStatTool, self).run()
        self.merge_table()
        self.run_kegg_stat()
        self.set_output()
        self.end()

    def merge_table(self):
        kegg_number = 0
        profile_file = os.listdir(self.option('kegg_result_dir').prop['path'])
        self.anno_result = os.path.join(self.work_dir, "tmp_kegg_anno.xls")
        self.enzyme_list = os.path.join(self.work_dir, "kegg_enzyme_list.xls")
        self.module_list =  os.path.join(self.work_dir, "kegg_module_list.xls")
        self.pathway_list = os.path.join(self.work_dir, "kegg_pathway_list.xls")
        if os.path.exists(self.anno_result):
            os.remove(self.anno_result)
        if os.path.exists(self.enzyme_list):
            os.remove(self.enzyme_list)
        if os.path.exists(self.module_list):
            os.remove(self.module_list)
        if os.path.exists(self.pathway_list):
            os.remove(self.pathway_list)
        for i in profile_file:
            kegg_number += 1
            suffix = ["anno_result","enzyme_list","module_list","pathway_list"]
            merge_result = [self.anno_result ,self.enzyme_list,self.module_list,self.pathway_list]
            for j in range(0,4):
                if suffix[j] in i:
                    file_path = os.path.join(self.option('kegg_result_dir').prop['path'], i)
                    cmd = '{} {} {}'.format(self.sh_path, file_path, merge_result[j])
                    self.logger.info("start cat {}".format(i))
                    command_name = "cat" + str(kegg_number)
                    command = self.add_command(command_name, cmd).run()
                    self.wait(command)
                    if command.return_code == 0:
                        self.logger.info("cat {} done".format(i))
                    else:
                        self.set_error("cat {} error".format(i))
                        raise Exception("cat {} error".format(i))

    def run_kegg_stat(self):
        kegg_anno = self.anno_result
        enzyme_list = self.enzyme_list
        module_list = self.module_list
        pathway_list = self.pathway_list
        cmd = self.python_path + ' {} -k {} -e {} -p {}  -m {} -r {} -o {} '. \
            format(self.python_script, kegg_anno, enzyme_list, pathway_list, module_list,
                   self.option('reads_profile').prop['path'], self.output_dir)
        self.logger.info(cmd)
        command = self.add_command('kegg_stat', cmd).run()
        self.wait(command)
        if command.return_code == 0:
            self.logger.info("kegg_stat succeed")
        else:
            self.set_error("kegg_stat failed")
            raise Exception("kegg_stat failed")

    def set_output(self):
        self.logger.info("set_output")
        if len(os.listdir(self.output_dir)) == 6:
            try:
                self.option("kegg_profile_dir", self.output_dir)
            except Exception as e:
                raise Exception("SET_OUTFILE FAILED {}".format(e))
            self.logger.info("OUTPUT RIGHT")
        else:
            raise Exception("OUTPUT WRONG")


