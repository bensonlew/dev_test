## !/mnt/ilustre/users/sanger-dev/app/program/Python/bin/python
# -*- coding: utf-8 -*-
# __author__ = "moli.zhou"
#last_modify:20170424

from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from biocluster.config import Config
import os
import re


class FastqProcessAgent(Agent):
    """
    产筛的生信shell部分功能
    处理fastq，得到bed2文件
    version v1.0
    author: moli.zhou
    """
    def __init__(self, parent):
        super(FastqProcessAgent, self).__init__(parent)
        options = [#输入的参数
            {"name": "sample_id", "type": "string"},
            {"name":"fastq_path","type":"string"}
        ]
        self.add_option(options)
        self.step.add_steps("fastq2bed")
        self.on('start', self.stepstart)
        self.on('end', self.stepfinish)

    def stepstart(self):
        self.step.fastq2bed.start()
        self.step.update()

    def stepfinish(self):
        self.step.fastq2bed.finish()
        self.step.update()


    def check_options(self):
        """
        重写参数检测函数
        :return:
        """
        # if not self.option('query_amino'):
        #     raise OptionError("必须输入氨基酸序列")
        if not self.option('sample_id'):
            raise OptionError("必须输入样本名")
        return True

    def set_resource(self):
        """
        设置所需资源，需在之类中重写此方法 self._cpu ,self._memory
        :return:
        """
        self._cpu = 10
        self._memory = '20G'

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"]
        ])
        result_dir.add_regexp_rules([
            [".bed.2", "bed.2", "信息表"],
        ])
        super(FastqProcessAgent, self).end()


class FastqProcessTool(Tool):
    """
    蛋白质互作组预测tool
    """
    def __init__(self, config):
        super(FastqProcessTool, self).__init__(config)
        self._version = '1.0.1'
        # self.R_path = 'program/R-3.3.1/bin/'
        self.script_path = 'bioinfo/medical/scripts/'
        self.java_path = self.config.SOFTWARE_DIR +'/program/sun_jdk1.8.0/bin/java'
        self.picard_path = self.config.SOFTWARE_DIR +'/bioinfo/medical/picard-tools-2.2.4/picard.jar'

        self.set_environ(PATH=self.config.SOFTWARE_DIR + '/gcc/5.1.0/bin')
        self.set_environ(LD_LIBRARY_PATH=self.config.SOFTWARE_DIR + '/gcc/5.1.0/lib64')
        self.set_environ(PATH=self.config.SOFTWARE_DIR + '/bioinfo/medical/FastQc')
        self.set_environ(PATH=self.config.SOFTWARE_DIR + '/bioinfo/medical/bwa-0.7.15/bin')
        self.set_environ(PATH=self.config.SOFTWARE_DIR + '/bioinfo/seq/bioawk')
        self.set_environ(PATH=self.config.SOFTWARE_DIR + '/bioinfo/seq/seqtk-master')
        self.set_environ(PATH=self.config.SOFTWARE_DIR + '/bioinfo/align/samtools-1.3.1')
        self.set_environ(PATH=self.config.SOFTWARE_DIR + '/bioinfo/medical/samblaster-0.1.22/bin')

    def run_tf(self):
        # ./nipt-0208-zml.sh WS-170281 /mnt/ilustre/users/sanger-dev/sg-users/xuanhongdong/db/genome/human/hg38_nipt/nchr.fa /mnt/ilustre/users/sanger-dev/sg-users/xuanhongdong/db/genome/human/hg38.chromosomal_assembly/ref.fa
        # /mnt/ilustre/users/sanger-dev/sg-users/zhoumoli/nipt/temp /mnt/ilustre/users/sanger-dev/app/program/sun_jdk1.8.0/bin/java /mnt/ilustre/users/sanger-dev/app/bioinfo/medical/picard-tools-2.2.4/picard.jar
        fq1 = self.option('sample_id') + '_R1.fastq.gz'
        fq2 = self.option('sample_id') + '_R2.fastq.gz'

        pre_cmd = '{}nipt_fastq_pre.sh {} {}'.format(self.script_path, self.option("fastq_path"), self.option('sample_id'))
        self.logger.info(pre_cmd)
        cmd = self.add_command("pre_cmd", pre_cmd).run()
        self.wait(cmd)
        if cmd.return_code == 0:
            self.logger.info("处理接头成功")
        else:
            self.logger.info("处理接头出错")


    def set_output(self):
        """
        将结果文件link到output文件夹下面
        :return:
        """
        for root, dirs, files in os.walk(self.output_dir):
            for names in files:
                os.remove(os.path.join(root, names))
        self.logger.info("设置结果目录")
        results = os.listdir(self.work_dir)

    def run(self):
        super(FastqProcessTool, self).run()
        self.run_tf()
        self.set_output()
        self.end()
