## !/mnt/ilustre/users/sanger-dev/app/program/Python/bin/python
# -*- coding: utf-8 -*-
# __author__ = "moli.zhou"
#last_modify:20161108
# 包含三种数据库

from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
# from biocluster.config import Config
import os
import subprocess
import shutil

class TfPredictAgent(Agent):
    """
    利用hmmer软件，进行转录因子预测
    version v1.0
    author: moli.zhou
    last_modify: 2016.11.4
    """
    def __init__(self, parent):
        super(TfPredictAgent, self).__init__(parent)
        options = [#输入的参数
            {"name": "query_amino", "type": "infile", "format": "sequence.fasta"},  # 上游输入的氨基酸文件（含与差异基因的对应）
            {"name": "database", "type": "string", "default": "iTAK"}, #还有PlantTFDB和AnimalTFDB
            {"name": "TFPredict", "type": "string"},
        ]
        self.add_option(options)
        self.step.add_steps("TfPredict")
        self.on('start', self.stepstart)
        self.on('end', self.stepfinish)

    def stepstart(self):
        self.step.TfPredict.start()
        self.step.update()

    def stepfinish(self):
        self.step.TfPredict.finish()
        self.step.update()


    def check_options(self):
        """
        重写参数检测函数
        :return:
        """
        database_list = ["PlantTFDB", "AnimalTFDB", "iTAK"]
        # if not self.option('query_amino').is_set:
        #     raise OptionError("必须输入氨基酸序列")
        if self.option('database') not in database_list:  # species的判定有问题
            raise OptionError("database选择不正确")
        return True

    def set_resource(self):
        """
        设置所需资源，需在之类中重写此方法 self._cpu ,self._memory
        :return:
        """
        self._cpu = 10
        self._memory = '100G'

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"]
        ])
        result_dir.add_regexp_rules([
            ["TfPredict.txt", "txt", "转录因子预测信息"],
        ])
        super(TfPredictAgent, self).end()


class TfPredictTool(Tool):
    """
    蛋白质互作组预测tool
    """
    def __init__(self, config):
        super(TfPredictTool, self).__init__(config)
        self._version = '1.0.1'
        # self.python_path = Config().SOFTWARE_DIR + '/program/Python/'
        # self.script_path = Config().SOFTWARE_DIR + '/bioinfo/rna/scripts/'
        # self.hmmer_path = Config().SOFTWARE_DIR + '/bioinfo/align/hmmer-3.1b2-linux-intel-x86_64/binaries/'
        # self.ref_path = Config().SOFTWARE_DIR + '/database/refGenome/TF/plant/'

        self.python_path = 'program/Python/bin/'
        self.perl_path = 'program/perl/perls/perl-5.24.0/bin/'
        self.script_path = '/mnt/ilustre/users/sanger-dev/app/bioinfo/rna/scripts/'
        self.ref_path = '/mnt/ilustre/users/sanger-dev/app/database/refGenome/TF/'
        self.itak_path = '/mnt/ilustre/users/sanger-dev/app/bioinfo/rna/iTAK-1.6b/'

    # python phmmer_process.py 1e-180  PlantTFDB-all_TF_pep.fas test.fas planttfdb_family_vs_tfid.txt
    def run_tf(self):
        if self.option("database") == 'PlantTFDB':
            ref = self.ref_path + "plant/planttfdb.hmm"
            family = self.ref_path + "plant/family_DBD.txt"
            tf_cmd = "{}python {}TF_process_plant.py {} {} {}".format(self.python_path,self.script_path,ref,self.option("query_amino").prop['path'],family)
        elif self.option("database") == 'iTAK':
            tf_cmd = '{}perl {}iTAK.pl {}'.format(self.perl_path, self.itak_path, self.option("query_amino").prop['path'])
        elif self.option('database') == 'AnimalTFDB':
            ref = self.ref_path + "animal/animaltfdb.hmm"
            family = self.ref_path + "animal/family_vs_DBD_animal_2.0.txt"
            tf_cmd = '{}python {}TF_process_animal.py {} {} {}'.format(self.python_path,self.script_path,ref,self.option("query_amino").prop['path'],family)

        self.logger.info(tf_cmd)
        self.logger.info("开始运行TFPredict")
        cmd = self.add_command("tf_cmd", tf_cmd).run()
        self.wait(cmd)
        if cmd.return_code == 0:
            self.logger.info("运行TFPredict成功")
        else:
            self.logger.info("运行TFPredict出错")

    def set_output(self):
        """
        将结果文件link到output文件夹下面
        :return:
        """
        for root, dirs, files in os.walk(self.output_dir):
            for names in files:
                os.remove(os.path.join(root, names))
        self.logger.info("设置结果目录")

        if self.option("database") == 'PlantTFDB':
            f = 'TF_result_plant.txt'
            os.link(self.work_dir + '/' + f, self.output_dir + '/' + f)
            self.logger.info('设置文件夹路径成功')
        elif self.option("database") == 'AnimalTFDB':
            f = 'TF_result_animal.txt'
            os.link(self.work_dir + '/' + f, self.output_dir + '/' + f)
            self.logger.info('设置文件夹路径成功')
        elif self.option("database") == 'iTAK':
            f = self.option("query_amino").prop['path'] + '_output'
            # os.link(f, self.output_dir)
            shutil.copytree(f, self.output_dir+'/iTAK')
            self.logger.info('设置文件夹路径成功')

    def run(self):
        super(TfPredictTool, self).run()
        self.run_tf()
        self.set_output()
        self.end()
