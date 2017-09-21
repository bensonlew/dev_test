# -*- coding: utf-8 -*-

from biocluster.agent import Agent
from biocluster.tool import Tool
import os
from biocluster.core.exceptions import OptionError


class BwaAgent(Agent):
    """
    bwa:比对工具
    version 1.0
    author: zhujuan
    last_modify: 2017.08
    """
    def __init__(self, parent):
        super(BwaAgent, self).__init__(parent)
        options = [
            {"name": "ref_database", "type": "string", "default": ""},  # 宿主参考序列库中对应的物种名，eg：E.coli ,B.taurus
            {"name": "ref_undefined", "type": "infile", "format": "sequence.fasta_dir"},
            # 未定义的宿主序列所在文件加，多个宿主cat到一个文件，并作为tool:align.bwa的输入文件
            {"name": "fq_type", "type": "string", "default": "PSE"},  # fq类型，PE、SE、PSE（即PE+SE，单端加双端）
            {"name": "fastq_dir", "type": "infile", "format": "sequence.fastq_dir"},
            # 输入质控后的fastq文件夹其中包含list文件
            {"name": "fastq_r", "type": "infile", "format": "sequence.fastq"},  # 右端序列文件
            {"name": "fastq_l", "type": "infile", "format": "sequence.fastq"},  # 左端序列文件
            {"name": "fastq_s", "type": "infile", "format": "sequence.fastq"},  # SE序列文件
            {"name": "head", "type": "string",
             "default": "'@RG\\tID:sample\\tLB:rna-seq\\tSM:sample\\tPL:ILLUMINA'"},  # 设置结果头文件
            {"name": "sam", "type": "outfile", "format": "align.bwa.sam_dir"},     # sam格式文件夹,内含对应list文件
            {"name": "method", "type": "string", "default": "align"},     # sam格式文件
        ]
        self.add_option(options)

    def check_options(self):
        """
        检查参数
        """
        if self.option("ref_database") == "" and not self.option("ref_undefined").is_set:
            raise OptionError("请传入参考序列")
        if self.option("fastq_dir").is_set and not os.path.exists(self.option("fastq_dir").prop["path"] + "/list.txt"):
            raise OptionError("fastq序列文件夹需还有list文件")
        if self.option("method") == "align":
            if self.option('fq_type') not in ['PE', 'SE', 'PSE']:
                raise OptionError("请说明序列类型，PE or SE or 'PSE'?")
        if not self.option("fastq_dir").is_set and self.option('fq_type') in ["PE", "PSE"]:
            if not self.option("fastq_r").is_set:
                raise OptionError("请传入PE右端序列文件")
            if not self.option("fastq_l").is_set:
                raise OptionError("请传入PE左端序列文件")
        if not self.option("fastq_dir").is_set and self.option('fq_type') in ["SE", "PSE"]:
            if not self.option("fastq_s").is_set:
                raise OptionError("请传入SE序列文件")
        return True

    def set_resource(self):
        """
        所需资源
        """
        self._cpu = 10
        self._memory = '10G'


class BwaTool(Tool):
    """
    version 1.0
    """

    def __init__(self, config):
        super(BwaTool, self).__init__(config)
        self.bwa_path = "bioinfo/align/bwa-0.7.9a/"
        global ref_fasta
        if self.option("ref_database") != "":
            ref_fasta = os.path.join("/mnt/ilustre/users/sanger-dev/app/database/",
                                     self.option("ref_database"), ".fasta")  # 数据库宿主所在path, 需要再确定
        if self.option("ref_undefined").is_set:
            ref_undefined = os.path.join(self.option("ref_undefined").prop["path"], "*.fasta")
            all_ref_undefined = os.path.join(self.option("ref_undefined").prop["path"],
                                             "ref_undefined/ref_undefined.fasta")
            os.system('mkdir -p '+self.option("ref_undefined").prop["path"] + '/ref_undefined')
            os.system('cat ' + ref_undefined + ' >' + all_ref_undefined)
            ref_fasta = os.path.join(self.option("ref_undefined").prop["path"], "ref_undefined/ref_undefined.fasta")
        if self.option("fastq_dir").is_set:
            self.samples = self.get_list()
            self.fq_dir = True
            self.fq_dir_path = self.option("fastq_dir").prop["path"]
        else:
            self.fq_dir = False

    def bwa_index(self):
        cmd = "{}bwa index {}".format(self.bwa_path, ref_fasta)
        print cmd
        self.logger.info("开始构建参考序列索引")
        command = self.add_command("bwa_index", cmd)
        command.run()
        self.wait()
        if command.return_code == 0:
            self.logger.info("成功构建参考序列索引！")
        else:
            self.set_error("构建索引出错")

    def bwa_aln(self, fastq, outfile):
        fq_name = fastq.split("/")[-1]
        cmd = "{}bwa aln -t 10 {} {} -f {}".format(self.bwa_path, ref_fasta, fastq, outfile)
        print(cmd)
        self.logger.info("开始运行{}_bwa_aln".format(fq_name.lower()))
        command = self.add_command("{}_bwa_aln".format(fq_name.lower()), cmd)
        command.run()
        return command

    def bwa_sampe(self, outfile, aln_l, aln_r, fastq_l, fastq_r):
        outfile_name = outfile.split("/")[-1]
        if self.option("head") is None:
            cmd = "{}bwa sampe -f {} {} {} {} {} {}".format(self.bwa_path, outfile,
                                                            ref_fasta, aln_l, aln_r, fastq_l, fastq_r)
        else:
            cmd = "{}bwa sampe -r {} -f {} {} {} {} {} {}".format(self.bwa_path, self.option("head"), outfile,
                                                                  ref_fasta, aln_l, aln_r, fastq_l, fastq_r)
        print(cmd)
        self.logger.info("开始运行{}_bwa_sampe".format(outfile_name.lower()))
        command = self.add_command("{}_bwa_sampe".format(outfile_name.lower()), cmd)
        command.run()
        if self.fq_dir is True:
            return command
        else:
            self.wait()
            if command.return_code == 0:
                self.logger.info("生成sam比对结果文件完成！")
            else:
                self.set_error("生成sam比对结果文件出错")

    def bwa_samse(self, outfile, aln_s, fastq_s):
        outfile_name = outfile.split("/")[-1]
        if self.option("head") is None:
            cmd = "{}bwa samse -f {} {} {} {}".format(self.bwa_path, outfile, ref_fasta, aln_s, fastq_s)
        else:
            cmd = "{}bwa samse -r {} -f {} {} {} {}".format(self.bwa_path, self.option("head"),
                                                            outfile, ref_fasta, aln_s, fastq_s)
        print(cmd)
        self.logger.info("开始运行{}_bwa_sampe命令".format(outfile_name.lower()))
        command = self.add_command("{}_bwa_samse".format(outfile_name.lower()), cmd)
        command.run()
        if self.fq_dir is True:
            return command
        else:
            self.wait()
            if command.return_code == 0:
                self.logger.info("生成sam比对结果文件完成！")
            else:
                self.set_error("生成sam比对结果文件出错")

    def multi_aln(self):
        samples = self.samples
        aln_commands = []
        for sample in samples:
            if self.option("fq_type") in ["PE", "PSE"]:
                aln_l_cmd = self.bwa_aln(os.path.join(self.fq_dir_path, samples[sample]["l"]),
                                         "{}_l.sai".format(sample))
                aln_r_cmd = self.bwa_aln(os.path.join(self.fq_dir_path, samples[sample]["r"]),
                                         "{}_r.sai".format(sample))
                aln_commands.append(aln_l_cmd)
                aln_commands.append(aln_r_cmd)
            if self.option("fq_type") in ["SE", "PSE"]:
                aln_s_cmd = self.bwa_aln(os.path.join(self.fq_dir_path, samples[sample]["s"]),
                                         "{}_s.sai".format(sample))
                aln_commands.append(aln_s_cmd)
                self.logger.info(aln_s_cmd)
        return aln_commands

    def multi_sam(self):
        samples = self.samples
        sam_commands = []
        sam_list_path = os.path.join(self.output_dir, "list.txt")
        with open(sam_list_path, "wb") as w:
            for sample in samples:
                if self.option("fq_type") in ["PE", "PSE"]:
                    fq_r = os.path.join(self.option("fastq_dir").prop["path"], samples[sample]["r"])
                    fq_l = os.path.join(self.option("fastq_dir").prop["path"], samples[sample]["l"])
                    sam_pe_cmd = self.bwa_sampe(os.path.join(self.output_dir, "{}.sam".format(sample)),
                                                "{}_l.sai".format(sample), "{}_r.sai".format(sample), fq_l, fq_r)
                    sam_commands.append(sam_pe_cmd)
                    w.write(sample+".sam\t"+sample+"\tpe\n")
                if self.option("fq_type") in ["SE", "PSE"]:
                    fq_s = os.path.join(self.option("fastq_dir").prop["path"], samples[sample]["s"])
                    sam_se_cmd = self.bwa_samse(os.path.join(self.output_dir, "{}_s.sam".format(sample)),
                                                "{}_s.sai".format(sample), fq_s)
                    sam_commands.append(sam_se_cmd)
                    w.write(sample+"_s.sam\t"+sample+"\tse\n")
            return sam_commands

    def get_list(self):
        list_path = self.option("fastq_dir").prop["path"] + "/list.txt"
        self.logger.info(list_path)
        list_path = os.path.join(self.option("fastq_dir").prop["path"], "list.txt")
        if os.path.exists(list_path):
            self.logger.info(list_path)
        sample = {}
        with open(list_path, "rb") as l:
            for line in l:
                line = line.strip().split()
                if len(line) == 3:
                    if line[1] not in sample:
                        sample[line[1]] = {line[2]: line[0]}
                    else:
                        sample[line[1]][line[2]] = line[0]
                if len(line) == 2:
                    if line[1] not in sample:
                        sample[line[1]] = {"s": line[0]}
                    else:
                        sample[line[1]]["s"] = line[0]
        return sample

    def set_ouput(self):
        self.logger.info('开始设置输出结果文件')
        try:
            self.option('sam', self.output_dir)
            self.logger.info("设置输出结果文件正常")
        except Exception as e:
            raise Exception("设置输出结果文件异常——{}".format(e))

    def run(self):
        """
        运行
        """
        super(BwaTool, self).run()
        if self.option("method") == "index":
            self.bwa_index()
        else:
            if os.path.exists(ref_fasta + ".pac"):
                pass
            else:
                self.bwa_index()
            if self.option("fastq_dir").is_set:
                aln_commands = self.multi_aln()
                self.logger.info(aln_commands)
                self.wait()
                for aln_cmd in aln_commands:
                    if aln_cmd.return_code == 0:
                        self.logger.info("运行{}完成".format(aln_cmd.name))
                    else:
                        self.set_error("运行{}运行出错!".format(aln_cmd.name))
                        return False
                sam_commands = self.multi_sam()
                self.logger.info(sam_commands)
                self.wait()
                for sam_cmd in sam_commands:
                    if sam_cmd.return_code == 0:
                        self.logger.info("运行{}完成".format(sam_cmd.name))
                    else:
                        self.set_error("运行{}运行出错!".format(sam_cmd.name))
                        return False
            else:
                if self.option("fq_type") in ["PE ", "PSE"]:
                    aln_l = self.bwa_aln(self.option("fastq_l").prop["path"], "aln_l.sai")
                    aln_r = self.bwa_aln(self.option("fastq_r").prop["path"], "aln_r.sai")
                    self.wait(aln_l, aln_r)
                    if aln_l.return_code == 0:
                        self.logger.info("左端比对完成！")
                    else:
                        self.set_error("左端比对出错")
                    if aln_r.return_code == 0:
                        self.logger.info("右端比对完成！")
                    else:
                        self.set_error("右端比对出错")
                    self.bwa_sampe("pe.sam", "aln_l.sai", "aln_r.sai", self.option("fastq_l").prop["path"],
                                   self.option("fastq_r").prop["path"])
                elif self.option("fq_type") in ["SE", "PSE"]:
                    aln_s = self.bwa_aln(self.option("fastq_s").prop["path"], "aln_s.sai")
                    self.wait(aln_s)
                    if aln_s.return_code == 0:
                        self.logger.info("比对完成！")
                    else:
                        self.set_error("比对出错")
                    self.bwa_samse("se.sam", "aln_s.sai", self.option("fastq_s").prop["path"])
        self.set_ouput()
        self.end()
