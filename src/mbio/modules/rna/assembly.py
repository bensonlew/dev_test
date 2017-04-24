# -*- coding: utf-8 -*-
# __author__ = 'wangzhaoyue'
import os
import shutil
from biocluster.core.exceptions import OptionError
from biocluster.module import Module
from mbio.packages.ref_rna.trans_step import *
import re
from mbio.files.sequence.file_sample import FileSampleFile


class AssemblyModule(Module):
    """
    拼接以及新转录本预测
    version v1.0.1
    author: wangzhaoyue
    last_modify: 2016.09.09
    """
    def __init__(self,work_id):
        super(AssemblyModule,self).__init__(work_id)
        options = [
            {"name": "sample_bam_dir", "type": "infile","format":"align.bwa.bam_dir"},  # 所有样本的bam文件夹
            {"name": "sample_bam", "type": "infile", "format": "align.bwa.bam"},  # 所有样本比对之后的bam文件
            {"name": "ref_fa", "type": "infile", "format": "sequence.fasta"},  # 参考基因文件
            {"name": "ref_gtf", "type": "infile", "format": "gene_structure.gtf"},  # 参考基因的注释文件
            {"name": "assembly_GTF_list.txt", "type": "infile", "format": "assembly.merge_txt"},  # 所有样本比对之后的bam文件路径列表
            {"name": "cpu", "type": "int", "default": 10},  # 软件所分配的cpu数量
            {"name": "fr_stranded", "type": "string", "default": "fr-unstranded"},  # 是否链特异性
            {"name": "strand_direct", "type": "string", "default": "none"},  # 链特异性时选择正负链
            {"name": "assemble_method", "type": "string", "default": "cufflinks"},  # 选择拼接软件
            # {"name": "sample_gtf", "type": "outfile", "format": "gene_structure.gtf"},# 输出的gtf文件
            # {"name": "merged.gtf", "type": "outfile", "format": "gene_structure.gtf"},  # 输出的合并文件
            # {"name": "tmap", "type": "outfile", "format": "assembly.tmap"},  # compare后的tmap文件
            # {"name": "refmap", "type": "outfile", "format": "assembly.tmap"},  # compare后的refmap文件
            # {"name": "combined.gtf", "type": "outfile", "format": "gene_structure.gtf"},  # compare后的combined.gtf文件
            # {"name": "new_gtf", "type": "outfile", "format": "gene_structure.gtf"},  # 新转录本注释文件
            # {"name": "new_fa", "type": "outfile", "format": "sequence.fasta"},  # 新转录本注释文件
        ]
        self.add_option(options)
        self.tools=[]
        self.step.add_steps('stringtie', 'cufflinks','stringtie_merge','cuffmerge','cuffcompare','new_transcripts')
        self.sum_tools=[]



    def check_options(self):
        """
        检查参数
        """
        # if not self.option('assemble_method'):
        #     raise OptionError('必须选择拼接的软件')
        if not self.option('sample_bam_dir'):
            raise OptionError('必须输入样本文件夹，文件夹里的文件为bam格式')
        if not self.option('ref_fa'):
            raise OptionError('必须输入参考序列ref.fa')
        if not self.option('ref_gtf'):
            raise OptionError('必须输入参考序列ref.gtf')
        if self.option("fr_stranded") != "fr-unstranded" and not self.option("strand_direct").is_set:
            raise OptionError("当链特异性时必须选择正负链")
        return True


    def finish_update(self,event):
        step = getattr(self.step,event['data'])
        step.finish()
        self.step.update()


    def stringtie_run(self):
        n =0
        samples = os.listdir(self.option('sample_bam_dir').prop['path'])
        for f in samples:
            f = os.path.join(self.option('sample_bam_dir').prop['path'], f)
            stringtie = self.add_tool('ref_rna.assembly.stringtie')
            self.step.add_steps('stringtie_{}'.format(n))
            stringtie.set_options({
                "sample_bam":f,
                "ref_fa":self.option('ref_fa').prop['path'],
                "ref_gtf":self.option('ref_gtf').prop['path'],
            })
            step = getattr(self.step, 'stringtie_{}'.format(n))
            step.start()
            stringtie.on('end',self.finish_update,'stringtie_{}'.format(n))
            self.tools.append(stringtie)
            self.sum_tools.append(stringtie)
            n += 1
        if len(self.tools) == 1:
            self.tools[0].on('end', self.set_output)
        else:
            self.on_rely(self.tools, self.stringtie_merge_run)
            self.step.stringtie_merge.start()
            self.step.update()
        for tool in self.tools:
            tool.run()


    def stringtie_merge_run(self):
        self.get_list()
        # gtffile_path = os.path.join(self.work_dir, "assembly_gtf.txt")
        stringtie_merge = self.add_tool("ref_rna.assembly.stringtie_merge")
        stringtie_merge.set_options({
            "assembly_GTF_list.txt": gtffile_path,
            "ref_fa": self.option('ref_fa').prop['path'],
            "ref_gtf": self.option('ref_gtf').prop['path'],
        })
        stringtie_merge.on('end', self.cuffcompare_run)
        stringtie_merge.run()
        self.sum_tools.append(stringtie_merge)
        self.step.stringtie_merge.finish()
        self.step.cuffcompare.start()
        self.step.update()


    def cufflinks_run(self):
        n = 0
        samples = os.listdir(self.option('sample_bam_dir').prop['path'])
        for f in samples:
            f = os.path.join(self.option('sample_bam_dir').prop['path'], f)
            cufflinks = self.add_tool('ref_rna.assembly.cufflinks')
            self.step.add_steps('cufflinks_{}'.format(n))
            cufflinks.set_options({
                "sample_bam": f,
                "ref_fa": self.option('ref_fa').prop['path'],
                "ref_gtf": self.option('ref_gtf').prop['path'],
                "fr_stranded": self.option("fr_stranded"),
            })
            step = getattr(self.step, 'cufflinks_{}'.format(n))
            step.start()
            cufflinks.on('end', self.finish_update, 'cufflinks_{}'.format(n))
            self.tools.append(cufflinks)
            self.sum_tools.append(cufflinks)
            n += 1
        if len(self.tools) == 1:
            self.tools[0].on('end', self.set_output)
        else:
            self.on_rely(self.tools, self.cuffmerge_run)
            self.step.cuffmerge.start()
            self.step.update()
        for tool in self.tools:
            tool.run()


    def cuffmerge_run(self):
        self.get_list()
        # gtffile_path = os.path.join(self.work_dir, "assembly_gtf.txt")
        cuffmerge = self.add_tool("ref_rna.assembly.cuffmerge")
        cuffmerge.set_options({
            "assembly_GTF_list.txt": gtffile_path,
            "ref_fa": self.option('ref_fa').prop['path'],
            "ref_gtf": self.option('ref_gtf').prop['path'],
        })
        cuffmerge.on('end', self.cuffcompare_run)
        cuffmerge.run()
        self.sum_tools.append(cuffmerge)
        self.step.cuffmerge.finish()
        self.step.cuffcompare.start()
        self.step.update()


    def cuffcompare_run(self):
        if self.option("assemble_method") == "cufflinks":
            merged_gtf = os.path.join(self.work_dir, "Cuffmerge/output/merged.gtf")
        elif self.option("assemble_method") =="stringtie":
             merged_gtf = os.path.join(self.work_dir,"StringtieMerge/output/merged.gtf")
        cuffcompare = self.add_tool("ref_rna.assembly.cuffcompare")
        cuffcompare.set_options({
             "merged.gtf": merged_gtf,
             "ref_fa": self.option('ref_fa').prop['path'],
             "ref_gtf": self.option('ref_gtf').prop['path'],
         })
        cuffcompare.on('end', self.new_transcripts_run)
        cuffcompare.run()
        self.sum_tools.append(cuffcompare)
        self.step.cuffcompare.finish()
        self.step.new_transcripts.start()
        self.step.update()


    def new_transcripts_run(self):
        if self.option("assemble_method") == "cufflinks":
            tmap = os.path.join(self.work_dir, "Cuffmerge/output/cuffcmp.merged.gtf.tmap")
            merged_gtf = os.path.join(self.work_dir, "Cuffmerge/output/merged.gtf")
        elif self.option("assemble_method") == "stringtie":
            tmap = os.path.join(self.work_dir, "StringtieMerge/output/cuffcmp.merged.gtf.tmap")
            merged_gtf = os.path.join(self.work_dir, "StringtieMerge/output/merged.gtf")
        new_transcripts = self.add_tool("ref_rna.assembly.new_transcripts")
        new_transcripts.set_options({
            "tmap":tmap,
            "merged.gtf":merged_gtf,
            "ref_fa": self.option('ref_fa').prop['path'],
        })
        new_transcripts.on('end', self.set_output)
        new_transcripts.run()
        self.sum_tools.append(new_transcripts)
        self.step.new_transcripts.finish()
        self.step.update()


    def get_list(self):
        gtffile_path = os.path.join(self.work_dir, "assembly_gtf.txt")
        global gtffile_path
        with open(gtffile_path, "w+") as w:
            for gtf in self.tools:
                for f in os.listdir(gtf.output_dir):
                    m = re.match(".+\.gtf",f)
                    if m:
                        file_path = os.path.join(gtf.output_dir,f)
                        w.write(file_path + "\n")

    def get_numberlist(self):
        file_list = []
        numberlist_path = os.path.join(self.output_dir,"number_list.txt")
        with open (numberlist_path,"w+") as w:
            # for gtf in self.tools:
            a= os.listdir(self.output_dir+'/assembly_newtranscripts')
            for f in a:
                file_list.append(f)
                if f.endswith("_out.gtf") or f.endswith("merged.gtf"):
                    file = os.path.join(self.output_dir+'/assembly_newtranscripts',f)
                    r = open(file)
                    list1 = set("")
                    list2 = set("")
                    for line in r:
                        m = re.match("#.*", line)
                        if not m:
                            arr = line.strip().split("\t")
                            nine_array = arr[-1].strip().split(";")
                            gene_id = nine_array[0].strip().split("\"")
                            trans_id = nine_array[1].strip().split("\"")
                            if len(trans_id) == 3 and trans_id[1] not in list1:
                                list1.add(trans_id[1])
                            if len(gene_id) == 3 and gene_id[1] not in list2:
                                list2.add(gene_id[1])
                    num_count = f + "\t" + str(len(list1)) +"\t"+str(len(list2))+ "\n"
                    # print(num_count)
                    w.write(num_count)
                    r.close()
    def trans_stat(self):
        allfile = os.listdir(self.output_dir+'/assembly_newtranscripts')
        for f in allfile:
            if f.endswith(".fa"):
                file = os.path.join(self.output_dir + '/assembly_newtranscripts', f)

                step_count(file, self.output_dir+"/"+f+".txt", 5,5000 , self.output_dir+"/assembly_newtranscripts/trans_count_stat.txt")


    def linkdir(self, dirpath, dirname):
        """
        link一个文件夹下的所有文件到本module的output目录
        :param dirpath: 传入文件夹路径
        :param dirname: 新的文件夹名称
        :return:
        """
        allfiles = os.listdir(dirpath)
        newdir = os.path.join(self.output_dir, dirname)
        if not os.path.exists(newdir):
            os.mkdir(newdir)
        oldfiles = [os.path.join(dirpath, i) for i in allfiles]
        newfiles = [os.path.join(newdir, i) for i in allfiles]
        for newfile in newfiles:
            if os.path.exists(newfile):
                if os.path.isfile(newfile):
                    os.remove(newfile)
                else:
                    os.system('rm -r %s' % newfile)
        for i in range(len(allfiles)):
            if os.path.isfile(oldfiles[i]):
                os.link(oldfiles[i], newfiles[i])
            elif os.path.isdir(oldfiles[i]):
                os.system('cp -r %s %s' % (oldfiles[i], newdir))


    def set_output(self):
        self.logger.info("set output")
        for tool in self.sum_tools:
            self.linkdir(tool.output_dir, 'assembly_newtranscripts')
        self.get_numberlist()
        self.trans_stat()
        self.end()


    def run(self):
        if self.option("assemble_method") =="cufflinks":
            self.cufflinks_run()
        elif self.option("assemble_method") =="stringtie":
            self.stringtie_run()
        # self.set_output()
        super(AssemblyModule,self).run()



    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [r".", "", "结果输出目录"],
            ["merged.gtf", "gtf", "样本合并之后的gtf文件"],
            ["new_transcripts.gtf", "gtf", "新转录本gtf注释文件"],
            ["new_transcripts.fa", "fa", "新转录本序列文件"],

        ])
        result_dir.add_regexp_rules([
            ["_out.gtf", "gtf", "样本拼接之后的gtf文件"],
            ["cuffcomp.*","", "提取新转录本过程文件"],
        ])
        super(AssemblyModule, self).end()