# -*- coding: utf-8 -*-
# __author__ = 'shijin'

"""运行denovo_rna.qc.qc_stat"""

from biocluster.workflow import Workflow


class RnaCheckWorkflow(Workflow):
    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        super(RnaCheckWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "fastq_dir", "type": "infile", "format": "sequence.fastq, sequence.fastq_dir"},
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
        pass
