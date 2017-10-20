# -*- coding: utf-8 -*-
# __author__ = 'wangzhaoyue'

from __future__ import division
import os
import re
import shutil
from Bio import SeqIO
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from collections import defaultdict

class FastqRecombinedAgent(Agent):
    """
    从fastq或者fastq文件夹里提取样本的信息
    """
    def __init__(self, parent):
        super(FastqRecombinedAgent, self).__init__(parent)
        options = [
            {"name": "file_list", "type": "infile", "format": "nipt.xlsx"},  # 从数据库dump下来的样本信息，仅重组样本集时用
            {"name": "output_list", "type": "outfile", "format": "nipt.xlsx"},  # 样本统计信息
            {"name": "info_file", "type": "outfile", "format": "nipt.xlsx"},  # 样本基本信息
        ]
        self.add_option(options)
        self.step.add_steps("fastq_recombined")
        self.on('start', self.start_fastq_recombined)
        self.on("end", self.end_fastq_recombined)

    def start_fastq_recombined(self):
        self.step.fastq_recombined.start()
        self.step.update()

    def end_fastq_recombined(self):
        self.step.fastq_recombined.finish()
        self.step.update()

    def check_options(self):
        if self.option("file_list").is_set:
            pass
        else:
            raise OptionError("重组时必须有需要重组的样本信息！")

    def set_resource(self):
        self._cpu = 1
        self._memory = "1G"

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", ""],
        ])
        super(FastqRecombinedAgent, self).end()


class FastqRecombinedTool(Tool):
    def __init__(self, config):
        super(FastqRecombinedTool, self).__init__(config)

    def run(self):
        super(FastqRecombinedTool, self).run()
        self.combined_fastq()
        self.end()

    def combined_fastq(self):
        """
        根据样本名将名称相同的样本进行合并，并将统计信息整合输出
        """
        sample_list, new_samples = self.get_sample_info()
        info_txt = self.output_dir + '/info.txt'
        sample_txt = self.output_dir + '/sample_info.xls'
        with open(sample_txt, "wb")as fw1, open(info_txt, "wb")as fw2:
            fw1.write("#alias_sample\tplatform\tstrategy\tprimer\tcontract_number\tcontract_sequence_number\tmj_number\tclient_name\tID\tsample_name\n")
            fw2.write("#seqs_num\tbase_num\tmean_length\tmin_length\tmax_length\n")
            for alias_name in new_samples:
                self.logger.info(new_samples)
                id_list = []
                sample_name = set()
                contract_sequence_number = set()
                mj_number = set()
                strategy = set()
                platform = set()
                client_name = set()
                primer = set()
                contract_number = set()
                sequence_num = []
                base_num = []
                min_length = []
                mean_length = []
                max_length = []
                sample_path = []
                for info in sample_list:
                    self.logger.info(info[2] + "," + alias_name)
                    if info[2] == alias_name:
                        sample_name.add(info[1])
                        id_list.append(info[0])
                        platform.add(info[3])
                        strategy.add(info[4])
                        primer.add(info[5])
                        contract_number.add(info[6])
                        contract_sequence_number.add(info[7])
                        mj_number.add(info[8])
                        client_name.add(info[9])
                        sequence_num.append(int(info[10]))
                        base_num.append(int(info[11]))
                        min_length.append(int(info[13]))
                        mean_length.append(info[12])
                        max_length.append(int(info[14]))
                        sample_path.append(info[15])
                    else:
                        pass
                if len(platform) != 1:
                    self.set_error("合并的样本{}测序平台不一致{}，不能进行合并!".format(alias_name,platform))
                    raise Exception("合并的样本{}测序平台不一致{}，不能进行合并!".format(alias_name,platform))
                if len(strategy) != 1:
                    self.set_error("合并的样本{}测序策略不一致，不能进行合并!".format(alias_name))
                    raise Exception("合并的样本{}测序策略不一致，不能进行合并!".format(alias_name))
                if len(primer) != 1:
                    self.set_error("合并的样本{}引物不一致，不能进行合并!".format(alias_name))
                    raise Exception("合并的样本{}引物不一致，不能进行合并!".format(alias_name))
                if len(sample_path) != 1:  # 当样本同名时，合并样本
                    os.system("cat {} > {}".format(" ".join(sample_path), self.output_dir + "/" + alias_name + ".fq"))
                final_id = ",".join(id_list)
                final_sample_name = ",".join(list(sample_name))
                final_platform = platform.pop()
                final_strategy = strategy.pop()
                final_primer = primer.pop()
                final_contract_number = ",".join(list(contract_number))
                final_contract_sequence_number = ",".join(list(contract_sequence_number))
                final_mj_number = ",".join(list(mj_number))
                final_client_name = ",".join(list(client_name))
                final_sequence_num = sum(sequence_num)
                final_base_num = sum(base_num)
                final_min_length = min(min_length)
                final_mean_length = final_base_num/final_sequence_num
                final_max_length = max(max_length)
                new_line1 = alias_name + '\t' + final_platform + "\t" + final_strategy + '\t' + final_primer + "\t" + \
                            final_contract_number + '\t' + final_contract_sequence_number + '\t' + final_mj_number + \
                            '\t' + final_client_name + '\t' + final_id + '\t' + final_sample_name + '\n'
                fw1.write(new_line1)
                new_line2 = final_id + '\t' + alias_name + '\t' + self.work_dir + '\t' + str(final_sequence_num) + '\t' + \
                           str(final_base_num) + '\t' + str(final_mean_length) + '\t' + str(final_min_length) + '\t' + \
                           str(final_max_length) + '\n'
                fw2.write(new_line2)
        self.option("output_list").set_path(self.output_dir + '/info.txt')
        self.option("info_file").set_path(self.output_dir + '/sample_info.xls')

    def get_sample_info(self):
        """
        获取样本信息
        :return:
        """
        with open(self.option("file_list").prop["path"], "r")as fr:
            lines = fr.readlines()
            sample_list = []
            new_samples = set()
            for line in lines[1:]:
                info = []
                line_split = line.strip().split("\t")
                new_samples.add(line_split[2])
                for i in line_split:
                    info.append(i)
                sample_list.append(info)
        return sample_list, new_samples

