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
    def __init__(self, work_id):
        super(AssemblyModule, self).__init__(work_id)
        options = [
            {"name": "sample_bam_dir", "type": "infile", "format": "align.bwa.bam_dir"},  # 所有样本的bam文件夹
            {"name": "ref_fa", "type": "infile", "format": "sequence.fasta"},  # 参考基因文件
            {"name": "ref_gtf", "type": "infile", "format": "gene_structure.gtf"},  # 参考基因的注释文件
            {"name": "assembly_GTF_list.txt", "type": "infile", "format": "assembly.merge_txt"},
            # 所有样本比对之后的bam文件路径列表
            {"name": "cpu", "type": "int", "default": 10},  # 软件所分配的cpu数量
            {"name": "fr_stranded", "type": "string", "default": "fr-unstranded"},  # 是否链特异性
            {"name": "strand_direct", "type": "string", "default": "none"},  # 链特异性时选择正负链
            {"name": "assemble_method", "type": "string", "default": "cufflinks"},  # 选择拼接软件
            {"name": "sample_gtf", "type": "outfile", "format": "gene_structure.gtf"},  # 输出的gtf文件
            {"name": "merged_gtf", "type": "outfile", "format": "gene_structure.gtf"},  # 输出的合并文件
            {"name": "cuff_gtf", "type": "outfile", "format": "gene_structure.gtf"},  # compare后的gtf文件
            {"name": "new_gtf", "type": "outfile", "format": "gene_structure.gtf"},  # 新转录本注释文件
            {"name": "new_fa", "type": "outfile", "format": "sequence.fasta"},  # 新转录本注释文件
        ]
        self.add_option(options)
        self.tools = []
        self.step.add_steps('stringtie', 'cufflinks', 'stringtie_merge', 'cuffmerge', 'cuffcompare', 'gffcompare',
                            'new_transcripts', 'transcript_abstract')
        self.sum_tools = []
        self.gtfs = []

    def check_options(self):
        """
        检查参数
        """
        if not self.option('sample_bam_dir'):
            raise OptionError('必须输入样本文件夹，文件夹里的文件为bam格式')
        if not self.option('ref_fa'):
            raise OptionError('必须输入参考序列ref.fa')
        if not self.option('ref_gtf'):
            raise OptionError('必须输入参考序列ref.gtf')
        if self.option("fr_stranded") != "fr-unstranded" and not self.option("strand_direct").is_set:
            raise OptionError("当链特异性时必须选择正负链")
        return True

    def finish_update(self, event):
        step = getattr(self.step, event['data'])
        step.finish()
        self.step.update()

    def stringtie_run(self):
        n = 0
        samples = os.listdir(self.option('sample_bam_dir').prop['path'])
        for f in samples:
            f = os.path.join(self.option('sample_bam_dir').prop['path'], f)
            stringtie = self.add_tool('assemble.stringtie')
            self.step.add_steps('stringtie_{}'.format(n))
            stringtie.set_options({
                "sample_bam": f,
                "ref_fa": self.option('ref_fa').prop['path'],
                "ref_gtf": self.option('ref_gtf').prop['path'],
            })
            step = getattr(self.step, 'stringtie_{}'.format(n))
            step.start()
            stringtie.on('end', self.finish_update, 'stringtie_{}'.format(n))
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
        stringtie_merge = self.add_tool("assemble.stringtie_merge")
        stringtie_merge.set_options({
            "assembly_GTF_list.txt": gtffile_path,
            "ref_fa": self.option('ref_fa').prop['path'],
            "ref_gtf": self.option('ref_gtf').prop['path'],
        })
        stringtie_merge.on('end', self.gffcompare_run)
        stringtie_merge.run()
        self.sum_tools.append(stringtie_merge)
        self.step.stringtie_merge.finish()
        self.step.gffcompare.start()
        self.step.update()

    def cufflinks_run(self):
        n = 0
        samples = os.listdir(self.option('sample_bam_dir').prop['path'])
        for f in samples:
            f = os.path.join(self.option('sample_bam_dir').prop['path'], f)
            cufflinks = self.add_tool('assemble.cufflinks')
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
        cuffmerge = self.add_tool("assemble.cuffmerge")
        cuffmerge.set_options({
            "assembly_GTF_list.txt": gtffile_path,
            "ref_fa": self.option('ref_fa').prop['path'],
            "ref_gtf": self.option('ref_gtf').prop['path'],
        })
        cuffmerge.on('end', self.gffcompare_run)
        cuffmerge.run()
        self.sum_tools.append(cuffmerge)
        self.step.cuffmerge.finish()
        self.step.gffcompare.start()
        self.step.update()

    def gffcompare_run(self):
        merged_gtf = ""
        if self.option("assemble_method") == "cufflinks":
            merged_gtf = os.path.join(self.work_dir, "Cuffmerge/output/merged_gtf")
        elif self.option("assemble_method") == "stringtie":
            merged_gtf = os.path.join(self.work_dir, "StringtieMerge/output/merged_gtf")
        gffcompare = self.add_tool("assemble.gffcompare")
        gffcompare.set_options({
             "merged_gtf": merged_gtf,
             "ref_gtf": self.option('ref_gtf').prop['path'],
         })
        gffcompare.on('end', self.new_transcripts_run)
        gffcompare.run()
        self.sum_tools.append(gffcompare)
        self.step.gffcompare.finish()
        self.step.new_transcripts.start()
        self.step.update()

    def new_transcripts_run(self):
        tmap = ""
        merged_gtf = ""
        if self.option("assemble_method") == "cufflinks":
            tmap = os.path.join(self.work_dir, "Cuffmerge/output/cuffcmp.merged_gtf.tmap")
            merged_gtf = os.path.join(self.work_dir, "Cuffmerge/output/merged_gtf")
        elif self.option("assemble_method") == "stringtie":
            tmap = os.path.join(self.work_dir, "StringtieMerge/output/cuffcmp.merged_gtf.tmap")
            old_merged_gtf = os.path.join(self.work_dir, "StringtieMerge/output/merged_gtf")
            merged_gtf = os.path.join(self.work_dir, "merged_gtf")
            merged_add_code(old_merged_gtf, tmap, merged_gtf)
            os.system('cp -r %s %s' % (merged_gtf, old_merged_gtf))
        new_transcripts = self.add_tool("assemble.new_transcripts")
        new_transcripts.set_options({
            "tmap": tmap,
            "merged_gtf": merged_gtf,
            "ref_fa": self.option('ref_fa').prop['path'],
        })
        new_transcripts.on('end', self.set_output)
        new_transcripts.run()
        self.sum_tools.append(new_transcripts)
        self.step.new_transcripts.finish()
        self.step.update()


    def linkdir(self, dirpath, dirname):
        """
        link一个文件夹下的所有文件到本module的output目录
        :param dirpath: 传入文件夹路径
        :param dirname: 新的文件夹名称
        :return:
        """
        allfiles = os.listdir(dirpath)
        newdir = os.path.join(self.work_dir, dirname)
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
        self.logger.info("完成统计样本中基因转录本个数")
        self.trans_stat()
        self.count_genes_trans_exons()
        if self.option("assemble_method") == "cufflinks":
            gtf_file = 'Cufflinks'
            merge = 'Cuffmerge'
            compare = 'Cuffcompare'
        else:
            gtf_file = 'Stringtie'
            merge = 'StringtieMerge'
            compare = 'Gffcompare'
        new_transcripts = 'NewTranscripts'
        statistics = 'Statistics'
        gtf_dir = os.path.join(self.output_dir, gtf_file)
        os.mkdir(gtf_dir)
        merge_dir = os.path.join(self.output_dir, merge)
        os.mkdir(merge_dir)
        compare_dir = os.path.join(self.output_dir, compare)
        os.mkdir(compare_dir)
        new_transcripts_dir = os.path.join(self.output_dir, new_transcripts)
        os.mkdir(new_transcripts_dir)
        statistics_dir = os.path.join(self.output_dir, statistics)
        os.mkdir(statistics_dir)
        old_dir = self.work_dir + '/assembly_newtranscripts/'
        for files in os.listdir(old_dir):
            self.logger.info(files)
            if files.endswith("_out.gtf") or files.endswith("_out.fa"):
                os.system('cp %s %s' % (old_dir + files, gtf_dir + "/" + files))
            elif files.endswith("merged_gtf") or files.endswith("merged.fa"):
                os.system('cp %s %s' % (old_dir + files, merge_dir + "/" + files))
            elif files.startswith("cuffcmp."):
                os.system('cp %s %s' % (old_dir + files, compare_dir + "/" + files))
            elif files.endswith(".txt"):
                os.system('cp %s %s' % (old_dir + files, statistics_dir + "/" + files))
            elif files.startswith("new_transcripts.") or files.startswith("new_genes.") or files.startswith("old_trans.gtf") or files.startswith("old_genes.gtf"):
                os.system('cp %s %s' % (old_dir + files, new_transcripts_dir + "/" + files))
        self.end()

    def run(self):
        if self.option("assemble_method") == "cufflinks":
            self.cufflinks_run()
        elif self.option("assemble_method") == "stringtie":
            self.stringtie_run()
        super(AssemblyModule, self).run()

    def get_list(self):
        gtffile_path = os.path.join(self.work_dir, "assembly_gtf.txt")
        global gtffile_path
        with open(gtffile_path, "w+") as w:
            for gtf in self.tools:
                for f in os.listdir(gtf.output_dir):
                    m = re.match(".+\.gtf", f)
                    if m:
                        file_path = os.path.join(gtf.output_dir, f)
                        w.write(file_path + "\n")

    def get_numberlist(self):
        file_list = []
        numberlist_path = os.path.join(self.work_dir, "number_list.txt")
        with open(numberlist_path, "w+") as w:
            a = os.listdir(self.work_dir+'/assembly_newtranscripts')
            for f in a:
                file_list.append(f)
                if f.endswith("_out.gtf") or f.endswith("merged_gtf"):
                    files = os.path.join(self.work_dir+'/assembly_newtranscripts', f)
                    r = open(files)
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
                    num_count = f + "\t" + str(len(list1)) + "\t" + str(len(list2)) + "\n"
                    w.write(num_count)
                    r.close()

    def trans_stat(self):
        all_file = os.listdir(self.work_dir+'/assembly_newtranscripts')
        for f in all_file:
            if f.endswith("merged.fa"):
                files = os.path.join(self.work_dir + '/assembly_newtranscripts', f)
                steps = [200, 300, 600, 1000]
                for step in steps:
                    step_count(files, self.work_dir+"/" + f + ".txt", 10, step,
                               self.work_dir + "/assembly_newtranscripts/trans_count_stat_" + str(step) + ".txt")
                self.logger.info("步长统计完成")
                self.logger.info("开始统计class_code")
            if f.endswith("merged_gtf"):
                files = os.path.join(self.work_dir + '/assembly_newtranscripts', f)
                code_count = os.path.join(self.work_dir + "/assembly_newtranscripts", "code_num.txt")
                class_code_count(files, code_count)
                if len(open(code_count).readline().split('\t')) == 3:
                    self.logger.info("完成class_code统计")
                else:
                    raise Exception("class_code统计失败！")
        self.logger.info("开始统计基因转录本外显子关系")

    def count_genes_trans_exons(self):
        all_file = os.listdir(self.work_dir + '/assembly_newtranscripts')
        for f in all_file:
            if f.endswith("old_genes.gtf") or f.endswith("old_trans.gtf") or f.endswith("new_genes.gtf") or f.endswith("new_transcripts.gtf"):
                files = os.path.join(self.work_dir + '/assembly_newtranscripts', f)
                gene_trans = os.path.join(self.work_dir, f + ".trans")
                trans_exon = os.path.join(self.work_dir, f + ".exon")
                gene_trans_exon(files, self.option("assemble_method"), gene_trans, trans_exon)
                # count_trans_or_exons(gene_trans, final_trans_file)
                # count_trans_or_exons(trans_exon, final_exon_file)
        gtf_files = os.listdir(self.work_dir)
        for f in gtf_files:
            if f.endswith('old_genes.gtf.trans') or f.endswith('new_genes.gtf.trans') or f.endswith('new_transcripts.gtf.exon') or f.endswith('old_trans.gtf.exon'):
                files = os.path.join(self.work_dir, f)
                steps = [1, 5, 10, 20]
                for step in steps:
                    middle_txt = os.path.join(self.work_dir, f + "_" + str(step) + ".txt")
                    final_txt = os.path.join(self.work_dir + '/assembly_newtranscripts', f + "_" + str(step) + ".txt")
                    count_trans_or_exons(files, step, middle_txt, final_txt)
        self.logger.info("完成统计基因转录本外显子关系")

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        if self.option("assemble_method") == "stringtie":
            result_dir.add_relpath_rules([
                [r".", "", "结果输出目录"],
                ["Stringtie", "", "拼接后的各样本文件夹"],
                ["StringtieMerge", "", "拼接组装合并之后结果文件夹"],
                ["StringtieMerge/merged_gtf", "gtf", "样本合并之后的注释文件"],
                ["StringtieMerge/merged.fa", "fasta", "样本合并之后的序列文件"],
                ["Gffcompare", "", "进行新转录本预测后的结果文件夹"],
                ["Gffcompare/cuffcmp.annotated.gtf", "", "进行新转录本预测后的结果文件"],
                ["Gffcompare/cuffcmp.merged_gtf.refmap", "", "进行新转录本预测后的结果文件"],
                ["Gffcompare/cuffcmp.merged_gtf.tmap", "", "进行新转录本预测后的结果文件"],
                ["NewTranscripts", "", "新转录本结果文件夹"],
                ["NewTranscripts/new_transcripts.gtf", "gtf", "新转录本注释文件"],
                ["NewTranscripts/new_transcripts.fa", "fa", "新转录本序列文件"],
                ["NewTranscripts/new_genes.gtf", "gtf", "新基因注释文件"],
                ["NewTranscripts/new_genes.fa", "fa", "新基因序列文件"],
                ["NewTranscripts/old_trans.gtf", "gtf", "已知转录本注释文件"],
                ["NewTranscripts/old_genes.gtf", "gtf", "已知基因注释文件"],
                ["Statistics", "", "统计信息的结果文件夹"],
                ["Statistics/code_num.txt", "txt", "class_code统计文件"],
            ])
            result_dir.add_regexp_rules([
                [r"Stringtie/.*_out\.gtf$", "gtf", "样本拼接之后的注释文件"],
                [r"Stringtie/.*_out\.fa$", "fasta", "样本拼接之后序列文件"],
                [r"Statistics/trans_count_stat_.*\.txt$", "txt", "新转录本步长统计文件"],
                [r"Statistics/old_.*\.txt$", "txt", "统计结果文件"],
                [r"Statistics/new_.*\.txt$", "txt", "统计结果文件"],

            ])
        if self.option("assemble_method") == "cufflinks":
            result_dir.add_relpath_rules([
                [r".", "", "结果输出目录"],
                ["Cufflinks", "", "拼接后的各样本文件夹"],
                ["Cuffmerge", "", "拼接组装合并之后结果文件夹"],
                ["Cuffmerge/merged_gtf", "gtf", "样本合并之后的注释文件"],
                ["Cuffmerge/merged.fa", "fasta", "样本合并之后的序列文件"],
                ["Gffcompare", "", "进行新转录本预测后的结果文件夹"],
                ["Gffcompare/cuffcmp.annotated.gtf", "", "进行新转录本预测后的结果文件"],
                ["Gffcompare/cuffcmp.merged_gtf.refmap", "", "进行新转录本预测后的结果文件"],
                ["Gffcompare/cuffcmp.merged_gtf.tmap", "", "进行新转录本预测后的结果文件"],
                ["NewTranscripts", "", "新转录本结果文件夹"],
                ["NewTranscripts/new_transcripts.gtf", "gtf", "新转录本注释文件"],
                ["NewTranscripts/new_transcripts.fa", "fa", "新转录本序列文件"],
                ["NewTranscripts/new_genes.gtf", "gtf", "新基因注释文件"],
                ["NewTranscripts/new_genes.fa", "fa", "新基因序列文件"],
                ["NewTranscripts/old_trans.gtf", "gtf", "已知转录本注释文件"],
                ["NewTranscripts/old_genes.gtf", "gtf", "已知基因注释文件"],
                ["Statistics", "", "统计信息的结果文件夹"],
                ["Statistics/code_num.txt", "txt", "class_code统计文件"],
            ])
            result_dir.add_regexp_rules([
                [r"Cufflinks/.*_out\.gtf$", "gtf", "样本拼接之后的注释文件"],
                [r"Cufflinks/.*_out\.fa$", "fasta", "样本拼接之后序列文件"],
                [r"Statistics/trans_count_stat_.*\.txt$", "txt", "新转录本步长统计文件"],
                [r"Statistics/old_.*\.txt$", "txt", "统计结果文件"],
                [r"Statistics/new_.*\.txt$", "txt", "统计结果文件"],

            ])
        super(AssemblyModule, self).end()