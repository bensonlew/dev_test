# -*- coding: utf-8 -*-
# __author__ = 'sj'
from biocluster.agent import Agent
from biocluster.tool import Tool
import os
from biocluster.core.exceptions import OptionError
import shutil

class TophatAgent(Agent):
    """
    tophat  
    version 1.0
    author: sj
    last_modify: 2016.9.5
    """
    def __init__(self, parent):
        super(TophatAgent, self).__init__(parent)
        self._ref_genome_lst = ["customer_mode"]
        options = [
            {"name": "ref_genome", "type": "string"},
            {"name":"ref_genome_custom", "type": "infile", "format": "sequence.fasta"},
            {"name": "mapping_method", "type": "string"},
            {"name":"seq_method", "type": "string"},
            {"name": "single_end_reads", "type": "infile", "format": "sequence.fastq"},
            {"name": "left_reads", "type": "infile", "format":"sequence.fastq"},
            {"name": "right_reads", "type": "infile", "format":"sequence.fastq"},
            {"name": "bam_output", "type": "outfile", "format": "align.bwa.bam"},
            ]
        self.add_option(options)
        self.step.add_steps('Tophat')
        self.on('start', self.step_start)
        self.on('end', self.step_end)

    def step_start(self):
        self.step.Tophat.start()
        self.step.update()

    def step_end(self):
        self.step.Tophat.finish()
        self.step.update()

    def check_options(self):
        if not self.option("ref_genome") in self._ref_genome_lst:
            raise OptionError("请设置参考基因组")
        if self.option("ref_genome") == "customer_mode" and not self.option("ref_genome_custom").is_set:
            raise OptionError("请上传自定义参考基因组")
        if self.option("seq_method") == "Paired-end":
            if self.option("single_end_reads").is_set:
                raise OptionError("您上传的是单端测序的序列，请上传双端序列")
            elif not (self.option("left_reads").is_set and  self.option("right_reads").is_set):
                raise OptionError("您漏了一端序列")
            else:
                pass
        else:
            if not self.option("single_end_reads").is_set:
                raise OptionError("请上传单端序列")
            elif self.option("left_reads").is_set or  self.option("right_reads").is_set:
                raise OptionError("有单端的序列就够啦")
            else:
                pass
        if not self.option("mapping_method") == "tophat":
            raise OptionError("这是tophat的tool")
        return True

    def set_resource(self):
        self._cpu = 10
        self._memory = '100G'

    def end(self):
        """
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"],
            ])
        result_dir.add_regexp_rules([
            [r"accepted_hits.bam", "bam", "tophat生成的文件"],
            ])
        """
        super(TophatAgent, self).end()


class TophatTool(Tool):
    def __init__(self, config):
        super(TophatTool, self).__init__(config)
        self.cmd_path = "bioinfo/align/tophat-2.1.1/tophat-2.1.1.Linux_x86_64/" 
    
    def run_build_index_and_blast(self):
        cmd = "{}bowtie2-build {} ref".format(self.cmd_path,self.option("ref_genome_custom").prop['path'])
        index_command_obj = self.add_command("build_index",cmd).run()
        self.wait(index_command_obj)
        if index_command_obj.return_code == 0:
            self.logger.info("索引建立完成")
            self.run_tophat()
        else:
            self.set_error("索引建立出错")
        
        
    def run_tophat(self):
        pre = os.path.splitext(os.path.basename(self.option("left_reads").prop['path']))[0].split("_")[0] + "_"
        ref_path = os.path.join(self.work_dir,"ref")
        if self.option("seq_method") == "Paired-end":
            cmd = "{}tophat2 {} {} {}".format(self.cmd_path,ref_path,self.option("left_reads").prop['path'],self.option("right_reads").prop['path'])
        else:
            cmd = "{}tophat2 {} {}".format(self.cmd_path,ref_path,self.option("single_end_reads").prop['path'])
        tophat_command = self.add_command("tophat",cmd)
        self.logger.info("开始运行tophat")
        tophat_command.run()
        self.wait()
        if tophat_command.return_code == 0:
            output = os.path.join(self.work_dir,"tophat_out/accepted_hits.bam")
            outfile_path = os.path.split(output)[0]
            outfile = os.path.join(outfile_path,pre + "accepted_hits.bam")
            os.rename(output,outfile)
            self.option('bam_output').set_path(outfile)
            shutil.move(outfile,"../output/")
        return True
    
    def run(self):
        """
        运行
        :return:
        """
        super(TophatTool, self).run()
        if self.option("ref_genome") == "customer_mode":
            self.run_build_index_and_blast()
        else:
            self.run_tophat()
        self.end()
        
        