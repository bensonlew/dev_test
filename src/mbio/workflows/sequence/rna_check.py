# -*- coding: utf-8 -*-
# __author__ = 'shijin'

"""运行denovo_rna.qc.qc_stat"""

from biocluster.workflow import Workflow
from mbio.files.sequence.fastq_dir import FastqDirFile

class RnaCheckWorkflow(Workflow):
    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        super(RnaCheckWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "fastq_dir", "type": "infile", "format": "sequence.fastq_dir"},
            {"name": "fq_type", "type": "string", "default": "PE"},
            {"name": "update_info", "type": "string"}
        ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.qc = self.add_module("denovo_rna.qc.qc_stat")
        self.add_text = self.add_tool("sequence.change_fastqdir")

    def check_options(self):

        pass

    def run_qc(self):
        opts = {
            "fastq_dir": self.add_text.output_dir,
            "fq_type": self.option("fq_type")
        }
        self.qc.set_options(opts)
        self.qc.run()

    def get_fastq(self):
        opts = {
            "fastq_dir": self.option("fastq_dir"),
            "fq_type": self.option("fq_type")
        }
        self.add_text.set_options(opts)
        self.add_text.run()

    def end(self):
        self.import2mongo()
        super(RnaCheckWorkflow, self).end()

    def run(self):
        self.add_text.on("end", self.run_qc)
        self.qc.on("end", self.end)
        self.get_fastq()
        super(RnaCheckWorkflow, self).run()

    def import2mongo(self):
        self.logger.info("开始导入数据库")
        api_sample = self.api.sample_base
        try:
            table_id = self.option("update_info").split(":")[0]
        except:  # 测试时无update_info参数
            table_id = "test_01"
        sample_list = self.get_sample()
        for sample in sample_list:
            sample_id = api_sample.add_sg_test_specimen(sample, self.qc.output_dir + "/fastq_stat.xls",
                                                        self.add_text.output_dir + "/list.txt", table_id)
            api_sample.add_sg_test_batch_specimen(table_id, sample_id, sample)
            api_sample.add_sg_test_batch_task_specimen(table_id, sample_id, sample)

    def get_sample(self):
        dir_path = self.add_text.option("samplebase_dir").prop["path"]
        dir = FastqDirFile()
        dir.set_path(dir_path)
        dir.check()
        sample_list = dir.prop["samples"]
        self.logger.info(str(sample_list))

        return sample_list