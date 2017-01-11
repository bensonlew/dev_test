# -*- coding:utf-8 -*-
# __author__ = 'chenyy'
# last_modifiy:2016.09.20


from biocluster.agent import Agent
from biocluster.tool import Tool
import os
import glob
from biocluster.core.exceptions import OptionError 

class StarAgent(Agent):
    """
    star 比对工具
    """

    def __init__(self, parent):
        super(StarAgent, self).__init__(parent)
        options = [
            {"name":"readFilesIN1", "type":"infile", "format":"sequence.fastq, sequence.fasta"},
            {"name":"readFilesIN2", "type":"infile", "format":"sequence.fastq, sequence.fasta"}   #输入用于比对的fasta或者fastq文件，可以为压缩文件，对参数进行选择
        ]
        self.add_option(options)
    
    def check_options(self):
        #if not self.option("ref_fa").is_set:
        #    raise OptionError("请提供参考基因组序列！")
        #if not self.option("ref_gtf").is_set:
        #    raise OptionError("请提供参考基因组gtf文件！")
        if not self.option("readFilesIN1").is_set:
            raise OptionError("请提供用于比对的fastq或fasta文件1！")
        if not self.option("readFilesIN2").is_set:
            raise OptionError("请提供用于比对的fastq或fasta文件2！")    
            
    def set_resource(self):
        self._cpu = 10
        self._memory = '500G'   #设置资源大小

        
class StarTool(Tool):

    def __init__(self, config):
        super(StarTool, self).__init__(config)
        self.star_path = "bioinfo/rna/star-2.5/bin/Linux_x86_64/" #设置star的路径
        #if not os.path.exists("ref_star_index1"):
        #    os.mkdir("ref_star_index1")
        #self.genomeDir_path1 = os.path.join(self.work_dir, "ref_star_index1")
        if not os.path.exists("ref_star_index2"):
            os.mkdir("ref_star_index2")
        self.genomeDir_path2 = os.path.join(self.work_dir, "ref_star_index2")
        self.genome_path = "/mnt/ilustre/users/sanger-dev/workspace/20160918/Single_chen.yanyan_test_star/Star/ref_star_index"
    """       
    def star_index1(self):
        
        step1:第一步建索引；用star建立参考基因组的索引，当用户不上传参考基因组时，该步骤省略，直接调用已有的文件（fa index_of_fa gtf 等等）
        
        
       
        cmd = "{}STAR --runMode genomeGenerate --genomeDir {} --genomeFastaFiles {} --sjdbGTFfile {}".format(self.star_path, self.genomeDir_path1, \
        self.option("ref_fa").prop["path"], self.option("ref_gtf").prop["path"])  # self.work_dir/ref_star_index1 用于存放第一步建立的参考基因组索引的路径
        print cmd 
        self.logger.info("使用star建立参考基因组索引")
        command = self.add_command("star_index", cmd)
        command.run()
        self.wait()
        if command.return_code == 0:
            self.logger.info("成功构建参考序列索引！")
        else:
            self.set_error("构建索引出错!")
    """    
            
    def star_aln1(self):
        """
        step2:第二步比对；用star进行比对
        """
        cmd = "{}STAR --genomeDir {} --readFilesIn {} {}".format(self.star_path, self.genome_path, self.option("readFilesIN1").prop["path"], self.option("readFilesIN2").prop["path"])
        print cmd
        self.logger.info("使用STAR对序列进行mapping")
        command = self.add_command("star_aln1", cmd)
        command.run()
        self.wait()
        if command.return_code == 0:
            self.logger.info("第一步比对成功！")
        else:
            self.set_error("第一步比对出错!")
            
    def star_index2(self,sj):
        """
        step3：第三步，第二次建索引，用于最终比对
        """
        cmd = "{}STAR --runMode genomeGenerate --genomeDir {} --genomeFastaFiles {} --sjdbChrStartEnd {} --sjdbOverhang 75".format(self.star_path, self.genomeDir_path2, self.option("ref_fa").prop["path"], \
        sj)
        print cmd
        self.logger.info("根据生成的sjdb数据进行第二次建索引")
        command = self.add_command("star_index2", cmd)
        command.run()
        self.wait()
        if command.return_code == 0:
            self.logger.info("第二次索引建立成功！")
        else:
            self.set_error("第二次索引建立出错!")
            
    def star_aln2(self):
        """
        step4：第四步，最终比对
        """
        cmd = "{}STAR --genomeDir{} --readFilesIN {} {}".format(self.star_path, self.genomeDir_path2, self.option("readFilesIN2").prop["path"], self.option("readFilesIN2").prop["path"])
        print cmd
        self.logger.info("最终比对过程")
        command = self.add_command("star_aln2", cmd)
        command.run()
        self.wait()
        if command.return_code == 0:
            self.logger.info("最终比对成功！")
        else:
            self.set_error("最终比对出错!")       
            
    def run(self):
        """
        运行
        """
        super(StarTool, self).run()
        #self.logger.info("运行star_index1，对参考基因组第一次建索引")
        
        #if self.option("ref_fa").is_set:
        #    self.star_index1()
        
        self.logger.info("运行star_aln1，进行第一次比对")    
        #if os.listdir(self.genomeDir_path1):
        self.star_aln1()
        
        self.logger.info("运行star_index2，对参考基因组第二次建索引")
        #if os.path.exists(os.path.join(self.work_dir, "SJ.out.tab")):
        SJ.out.tab = os.path.join(self.work_dir, "SJ.out.tab")
        star_index2(SJ.out.tab)
        

        self.logger.info("运行star_aln2，进行第二次比对")
        if os.listdir(self.genomeDir_path2):
            if os.path.exists(os.path.join(self.work_dir, "Aligned.out.sam")):
                os.remove(os.path.join(self.work_dir, "Aligned.out.sam"))
                self.star_aln2()   
        
            
        self.end()
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            


