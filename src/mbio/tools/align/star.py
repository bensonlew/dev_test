# -*- coding:utf-8 -*-
# __author__ = 'chenyanyan'
# last_modifiy:2016.09.26

from biocluster.agent import Agent
from biocluster.tool import Tool
import shutil
import os
import glob
from biocluster.core.exceptions import OptionError 
import json
import time


class StarAgent(Agent):
    """
    star 比对工具
    """

    def __init__(self, parent):
        super(StarAgent, self).__init__(parent)
        """
        软件建议提供参考基因组的gtf文件，下表为已有参考基因组物种
        """
        self._ref_genome_lst = ["customer_mode", "Chicken", "Tilapia", "Zebrafish", "Cow", "pig", "Fruitfly", "human",
                                "Mouse", "Rat", "Arabidopsis", "Broomcorn", "Rice", "Zeamays", "Test", "test"]
        options = [
        
            {"name": "ref_genome_custom", "type": "infile", "format": "sequence.fasta"},  # 用户上传参考基因组文件
            {"name": "ref_genome", "type": "string"},  # 参考基因组模式选项 用户自定义、选择已有生物物种
            {"name": "ref_gtf", "type": "infile", "format": "gene_structure.gtf"},  # 参考基因组的gtf文件 ，gtf文件和fasta文件要配套使用
            {"name": "readFilesIN1", "type": "infile", "format": "sequence.fastq, sequence.fasta"},  # 双端序列文件1端
            {"name": "readFilesIN2", "type": "infile", "format": "sequence.fastq, sequence.fasta"},  # 双端序列文件2端
            {"name": "readFilesIN", "type": "infile", "format": "sequence.fastq"},  # 单端序列文件
            {"name": "seq_method", "type": "string"},
            {"name": "is_indexed", "type": "bool", "default": False},
           
        ]
        self.add_option(options)
        self.step.add_steps('star')
        self.on('start', self.step_start)
        self.on('end', self.step_end)
        
    def step_start(self):
        self.step.star.start()
        self.step.update()

    def step_end(self):
        self.step.star.finish()
        self.step.update()

    def check_options(self):

        """
        检查参数设置
        """
        if self.option("ref_genome") == "customer_mode" and not self.option("ref_genome_custom").is_set:
            raise OptionError("请上传自定义参考基因组")
            
        if not self.option("ref_genome") in self._ref_genome_lst:
            raise OptionError("请设置参考基因组")  # 自定义模式还是选择物种
        
        # if self.option("ref_genome") == "customer_mode" and not self.option("ref_gtf").is_set:
        #    raise OptionError("请提供参考基因组gtf文件！")
        
        if self.option("seq_method") == "PE":
            if not self.option("readFilesIN1").is_set:
                raise OptionError("请提供用于比对的fastq或fasta文件1！")
            if not self.option("readFilesIN2").is_set:
                raise OptionError("请提供用于比对的fastq或fasta文件2！") 
        
        if self.option("seq_method") == "SE":
            if not self.option("readFilesIN").is_set:
                raise OptionError("请提供用于比对的单端fastq或fasta文件！")

    def set_resource(self):
        self._cpu = 20
        self._memory = '30G'   # 设置资源大小

    def end(self):
        super(StarAgent, self).end()    # 继承超类的end方法


class StarTool(Tool):

    def __init__(self, config):
        super(StarTool, self).__init__(config)
        self.star_path = "bioinfo/rna/star-2.5/bin/Linux_x86_64/"  # 设置star的路径
        
        # self.ref_fasta = self.option('ref_genome_custom').prop["path"]  # 用户上传的基因组路径
        # # if self.option("is_indexed") is False:
        # shutil.copy(self.ref_fasta, self.work_dir)  # 将参考基因组复制到当前工作路径下
        # self.ref_fa_cp = os.path.join(self.work_dir, os.path.basename(self.ref_fasta))
        # self.ref_fasta = self.ref_fa_cp
        
        if not os.path.exists("ref_star_index2"):   # 创建第二次建索引的目录文件夹
            os.mkdir("ref_star_index2")
        self.genomeDir_path2 = os.path.join(self.work_dir, "ref_star_index2")
           
    def star_index1(self, genomeDir, ref_fa):
        """
        step1:第一步建索引；用star建立参考基因组的索引，当用户不上传参考基因组时，该步骤省略，直接调用已有的序列文件
        """
        cmd = "{}STAR --runMode genomeGenerate --limitGenomeGenerateRAM 128000000000 --genomeDir {} --genomeFastaFiles {} --runThreadN 20".format(self.star_path, genomeDir, ref_fa)  # self.work_dir/ref_star_index1 用于存放第一步建立的参考基因组索引的路径,参数为用户上传的参考基因组文件
        print cmd 
        self.logger.info("使用star建立参考基因组索引")
        command = self.add_command("star_index1", cmd)
        command.run()
        self.wait()
        if command.return_code == 0:
            self.logger.info("成功构建参考序列索引index1！")
        else:
            self.set_error("构建索引出错!")
            raise Exception("运行star出错")

    def star_aln1_se(self, genomeDir):
        """
        step2:第二步比对；用star进行单端序列的比对
        """
       
        cmd = "{}STAR --runThreadN 20 --genomeDir {} --readFilesIn {}".format(self.star_path, genomeDir, self.option("readFilesIN").prop["path"])
        print cmd
        self.logger.info("使用STAR对序列进行单端mapping")
        command = self.add_command("star_aln1", cmd)
        command.run()
        self.wait() 
        if command.return_code == 0:
            self.logger.info("单端比对第一步比对成功！")
        else:
            self.set_error("单端比对第一步比对出错!")
            raise Exception("运行star出错")
            
    def star_aln1_pe(self, genomeDir):
        """
        step2:第二步比对；用star进行双端序列的比对
        """
       
        cmd = "{}STAR --runThreadN 20 --genomeDir {} --readFilesIn {} {}".format(self.star_path, genomeDir, self.option("readFilesIN1").prop["path"], self.option("readFilesIN2").prop["path"])
        print cmd
        self.logger.info("使用STAR对序列进行双端mapping")
        command = self.add_command("star_aln1_pe", cmd)
        command.run()
        self.wait()
        if command.return_code == 0:
            self.logger.info("双端比对第一步比对成功！")
        else:
            self.set_error("双端比对第一步比对出错!")
            raise Exception("运行star出错")
            
    def star_index2(self, ref_fa, sj):
        """
        step3：第三步，第二次建索引，用于最终比对
        """
        cmd = "{}STAR --runMode genomeGenerate --limitGenomeGenerateRAM 128000000000 --runThreadN 30 --genomeDir {} --genomeFastaFiles {} --sjdbFileChrStartEnd {} --sjdbOverhang 75".format(self.star_path, self.genomeDir_path2, ref_fa, sj)
        print cmd
        self.logger.info("根据生成的sjdb数据进行第二次建索引index2")
        command = self.add_command("star_index2", cmd)
        command.run()
        self.wait()
        if command.return_code == 0:
            self.logger.info("第二次索引建立成功！")
        else:
            self.set_error("第二次索引建立出错!")
            raise Exception("运行star出错")
            
    def star_aln2_se(self):
        """
        step4：第四步，最终比对
        """
        cmd = "{}STAR --runThreadN 20 --genomeDir {} --readFilesIn {}".format(self.star_path, self.genomeDir_path2,
                                                                              self.option("readFilesIN").prop["path"])
        print cmd
        self.logger.info("使用star进行最终比对！")
        command = self.add_command("star_aln2", cmd)
        command.run()
        self.wait()
        """
        import subprocess
        import time
        command._subprocess.returncode = 0
        time.sleep(10)
        """
        if command.return_code == 0:
            self.logger.info("最终单端比对成功！")
        else:
            self.set_error("最终单端比对出错!")
            raise Exception("运行star出错")
         
    def star_aln2_pe(self):
        """
        step4：第四步，最终比对
        """
        cmd = "{}STAR --runThreadN 20 --genomeDir {} --readFilesIn {} {}".format(self.star_path, self.genomeDir_path2, self.option("readFilesIN1").prop["path"], self.option("readFilesIN2").prop["path"])
        print cmd
        self.logger.info("最终比对过程")
        command = self.add_command("star_aln2", cmd)
        command.run()
        self.wait()
        if command.return_code == 0:
            self.logger.info("最终双端比对成功！")
        else:
            command.rerun()
            if command.return_code == 0:
                self.logger.info("最终双端比对成功！")
            else:
                self.set_error("最终双端比对出错!")
                raise Exception("运行star出错")
            
    def run(self):
        """
        运行
        """
        super(StarTool, self).run()
        
        if self.option("ref_genome") == "customer_mode" and self.option("ref_genome_custom").is_set:  # 自定义模式的时候需要建索引
            self.logger.info("在参考基因组为自定义模式下运行star！")
                    
            ref_fa = self.option("ref_genome_custom").prop["path"]
            if not os.path.exists("ref_star_index1"):
                os.mkdir("ref_star_index1")
            genomeDir_path1 = os.path.join(self.work_dir, "ref_star_index1")   # 准备第一次建索引的路径
            if self.option("is_indexed") is False:
                self.star_index1(genomeDir_path1, ref_fa)  # 第一步：建索引，传入第一步索引的文件夹（此时是空文件夹）
            if self.option("seq_method") == "PE":
                self.star_aln1_pe(genomeDir_path1)
                sj = os.path.join(self.work_dir, "SJ.out.tab")
                self.star_index2(ref_fa, sj)
                self.star_aln2_pe()
                # time.sleep(30)
                
            else:
                self.star_aln1_se(genomeDir_path1)
                sj = os.path.join(self.work_dir, "SJ.out.tab")
                self.star_index2(ref_fa, sj)
                self.star_aln2_se()
        else:  # 参考基因组来自数据库
            self.logger.info("在参考基因组从数据库中选择时，运行star")
            """
            参考基因组来自数据库，加载json文件获取相应路径
            """
            ref_genome_json = self.config.SOFTWARE_DIR + "/database/refGenome/scripts/ref_genome.json"
            with open(ref_genome_json, "r") as f:
                ref_dict = json.loads(f.read())
                print(ref_dict)
                for dd in ref_dict:
                    print(dd)
                if "human" in ref_dict:
                    self.logger.info("lllllllltrrrrreeee")
                # ref = ref_dict[self.option("ref_genome")]["ref_index"]   # 是ref_index的路径，作为参数传给比对函数
                # ref = os.path.join(self.work_dir, "ref_star_index1")
                ref_fa = ref_dict[self.option("ref_genome")]["ref_genome"]
                if self.option("ref_genome") == "human":
                    ref = "/".join(ref_fa.split("/")[:-1]) + "/ref_star_index1"
                else:
                    ref = os.path.join(self.work_dir, "ref_star_index1")
                    if not os.path.exists(ref):
                        os.mkdir(ref)
                    self.star_index1(ref, ref_fa)
                self.logger.info(ref_fa)
                self.logger.info(ref)
            # if not os.path.exists(ref):
            #     os.mkdir(ref)
            # self.star_index1(ref, ref_fa)s
            if self.option("seq_method") == "PE":
                self.star_aln1_pe(ref)
                sj = os.path.join(self.work_dir, "SJ.out.tab")
                self.star_index2(ref_fa, sj)
                self.star_aln2_pe()
                
            else:
                self.star_aln1_se(ref)
                sj = os.path.join(self.work_dir, "SJ.out.tab")
                self.star_index2(ref_fa, sj)
                self.star_aln2_se()
        """
        只需要生成的Sam文件
        """
        if os.path.exists(os.path.join(self.work_dir, "Aligned.out.sam")):
            shutil.copy(os.path.join(self.work_dir, "Aligned.out.sam"), self.output_dir)
        # self.set_output()
        self.end()
