# -*- coding: utf-8 -*-
# __author__
from biocluster.module import Module
import os
import re
from biocluster.core.exceptions import OptionError
import glob
from mbio.files.sequence.file_sample import FileSampleFile
import shutil

class ExpressModule(Module):
    def __init__(self,work_id):
        super(ExpressModule,self).__init__(work_id)
        self.step.add_steps("featurecounts", "mergefeaturecounts", "diff_Rexp")
        options=[
            {"name": "fq_type", "type": "string", "default": "PE"},  # PE OR SE
            {"name": "ref_gtf", "type": "infile", "format": "ref_rna.reads_mapping.gtf"},  # 参考基因组的gtf文件
            {"name": "gtf_type", "type":"string","default": "ref"},  #ref，merge_cufflinks，merge_stringtie三种参数
            {"name": "sample_bam", "type": "infile", "format": "ref_rna.assembly.bam_dir"},  # 所有样本的bam文件夹
            # {"name": "bam", "type": "infile", "format": "ref_rna.assembly.bam"},  # 样本比对后的bam文件
            {"name": "strand_specific", "type": "string", "default": "None"},  # PE测序，是否链特异性, 默认是0, 无特异性
            {"name": "strand_dir", "type": "string", "default": "None"},  # 链特异性时选择正链, 默认不设置此参数"forward" "reverse"
            {"name": "feature_id", "type": "string", "default": "gene_id"},  # 默认计算基因的count值，可以选择exon，both等
            # {"name": "merge_files", "type": "infile", "format": "ref_rna.assembly.bam_dir"},  # 输出多个样本count表的文件夹
            {"name": "express_method", "type": "string", "default": "featurecounts"},  # 选择计算表达量的方法 "htseq","both"
            {"name": "sort_type", "type": "string", "default": "pos"},  # 按照位置排序 "name" htseq参数
            # {"name": "count", "type": "infile", "format": "denovo_rna.express.express_matrix"},  # 输入文件，基因技术矩阵
            # {"name": "fpkm", "type": "infile", "format": "denovo_rna.express.express_matrix"},  # 输入文件，基因表达量矩阵
            {"name": "cpu", "type": "int", "default": 10},  # 设置CPU
            {"name": "max_memory", "type": "string", "default": "100G"},  # 设置内存
            {"name": "diff_rate", "type": "float", "default": 0.1},  # edger离散值
            {"name": "min_rowsum_counts", "type": "int", "default": 2},  # 离散值估计检验的最小计数值
            # {"name": "sample_list", "type": "string", "default": ''},  # 选择计算表达量的样本名，多个样本用‘，’隔开,有重复时没有该参数
            {"name": "control_file", "type": "infile", "format": "denovo_rna.express.control_table"},  # 对照组文件，格式同分组文件
            {"name": "edger_group", "type": "infile", "format": "meta.otu.group_table"},  # 有生物学重复的时候的分组文件
            {"name": "diff_ci", "type": "float", "default": 0.05},  # 显著性水平
            # {"name": "diff_count", "type": "outfile", "format": "denovo_rna.express.express_matrix"},  # 差异基因计数表
            # {"name": "diff_fpkm", "type": "outfile", "format": "denovo_rna.express.express_matrix"},  # 差异基因表达量表
            # {"name": "gene_file", "type": "outfile", "format": "denovo_rna.express.gene_list"},
            #{"name": "diff_list_dir", "type": "outfile", "format": "denovo_rna.express.gene_list_dir"},
            {"name": "gname", "type": "string", "default": "group"},  # 分组方案名称
			{"name": "method", "type": "string", "default": "edgeR"}  # 分析差异基因选择的方法
        ]
        self.add_option(options)
        self.featurecounts = self.add_tool("ref_rna.express.featureCounts")
        self.mergefeaturecounts = self.add_tool("ref_rna.express.mergefeaturecounts")
        #self.tablemakerballgown = self.add_tool("ref_rna.express.tablemakerballgown")
        self.diff_Rexp = self.add_tool("ref_rna.express.diff_Rexp")
        self.tool_lists = []
        self.samples = []
        self.sumtool = []

    def check_options(self):
        if not self.option('fq_type'):
            raise OptionError('必须设置测序类型：PE OR SE')
        if self.option('fq_type') not in ['PE', 'SE']:
            raise OptionError('测序类型不在所给范围内')
        if self.option("strand_specific") == 1:
            if not self.option("firststrand") and not self.option("secondstrand"):
                raise OptionError("链特异性时需要选择正链或者负链")
        if not self.option("ref_gtf").is_set:
            raise OptionError("需要输入gtf文件")
        if self.option("feature_id") not in ["gene_id", "exon", "both"]:
            raise OptionError("计算基因或者外显子的count值")
        if not self.option("sample_bam").is_set:
            raise OptionError("请传入bam的文件夹")
        if not self.option('control_file').is_set:
             raise OptionError("必须设置输入文件：上下调对照组参考文件")
        if self.option("diff_ci") >= 1 or self.option("diff_ci") <= 0:
            raise OptionError("显著性水平不在(0,1)范围内")
        # if self.option("diff_rate") > 1 or self.option("diff_rate") <= 0:
        #     raise OptionError("期望的差异基因比率不在(0，1]范围内")
        #samples, genes = self.option('count').get_matrix_info()
        return True

    def featurecounts_run(self):
        n=0
        """发送信息给前端"""
        self.step.featurecounts.start()
        self.step.update()
        tool_opt = {
            "ref_gtf": self.option("ref_gtf").prop['path'],
            "strand_specific": self.option("strand_specific"),
            "feature_id": self.option("feature_id"),
            "fq_type": self.option('fq_type')
        }
        if self.option("strand_specific").find("None") != -1:
            if self.option("strand_dir").find("None") !=-1:
                if self.option("strand_dir").find("forward") != -1 or self.option("strand_dir").find("reverse") != -1:
                    tool_opt.update({
                        'strand_dir': self.option("strand_dir")
                    })
        s_files = os.listdir(self.option("sample_bam").prop['path'])
        for f in s_files:
            if re.search(r'bam$', f):
                sample = f.split('bam')[0]
                tool_opt.update({
                    'bam': self.option('sample_bam').prop['path'] + "/" + f
                })
                n += 1
                self.logger.info(n)
                self.featurecounts=self.add_tool("ref_rna.express.featureCounts")
                print tool_opt
                self.featurecounts.set_options(tool_opt)
                self.tool_lists.append(self.featurecounts)
                self.featurecounts.run()
        self.sumtool.append(self.featurecounts)
        print self.tool_lists
        """绑定下一个将要运行的步骤"""
        self.on_rely(self.tool_lists, self.set_output, "featurecounts")
        self.on_rely(self.tool_lists, self.set_step, {'end': self.step.featurecounts, 'start': self.step.mergefeaturecounts})

    def mergefeaturecounts_run(self):
        dir_path=self.output_dir + "/featurecounts/"
        for files in os.listdir(dir_path):
            if files.find('control_file') !=-1:
                os.remove(os.path.join(dir_path,files))
        self.mergefeaturecounts.set_options({
            "mergefeaturecounts_files": self.output_dir + "/featurecounts/"
        })
        self.sumtool.append(self.mergefeaturecounts)
        self.mergefeaturecounts.on('end', self.set_output, 'mergefeaturecounts')
        self.mergefeaturecounts.on('end', self.set_step, {'end': self.step.mergefeaturecounts, 'start': self.step.diff_Rexp})
        """把差异表达分析绑定在一起"""
        self.mergefeaturecounts.run()
        self.wait()
        self.logger.info("mergefeaturecounts 运行结束！")

    def diff_Rexp_run(self):
        self.logger.info('开始进行差异表达分析！')
        fpkm_count_dir = self.output_dir + "/mergefeaturecounts_express"
        for f in os.listdir(fpkm_count_dir):
            if f.find("fpkm") != -1:
                fpkm_path=os.path.join(fpkm_count_dir, f)
                self.logger.info(fpkm_path)
            else:
                count_path=os.path.join(fpkm_count_dir, f)
                self.logger.info(count_path)
        edger_group_path = self.option('edger_group').prop['path']
        control_file_path = self.option('control_file').prop['path']
        self.logger.info(edger_group_path)
        self.logger.info(control_file_path)
        tool_opt = {
           "count": count_path,
           "fpkm": fpkm_path,
            "control_file": control_file_path,
            "edger_group": edger_group_path,
            "gname": "group",
            "method": self.option("method"),
            'diff_ci': self.option("diff_ci")
        }
        if self.option("edger_group").is_set:
             tool_opt['edger_group'] = self.option("edger_group").prop['path']
             tool_opt['gname'] = self.option('gname')
        self.diff_Rexp.set_options(tool_opt)
        self.sumtool.append(self.diff_Rexp)
        self.diff_Rexp.on("end", self.set_output, 'diff_Rexp')
        self.diff_Rexp.on("end", self.set_step, {"end": self.step.diff_Rexp})
        self.diff_Rexp.run()
        self.wait()

    def htseq_run(self):
        self.step.htseq.start()
        self.step.update
        tool_opt = {
               "ref_gtf": self.option("ref_gtf").prop['path'],
               "strand_specific": self.option("strand_specific"),
               "sort_type": self.option("sort_type"),
               "gtf_type": self.option("gtf_type")
        }
        if self.option("strand_dir") !="None":
            tool_opt.update({
               "strand_dir": self.option("strand_dir")
            })
        s_files=os.listdir(self.option('sample_bam').prop['path'])
        for f in s_files:
            if re.search(r'bam$', f):
               sample = f.split('bam')[0]
               tool_opt.update({
                   'bam': self.option('sample_bam'.prop['path']+'/'+f)
               })
               n+=1
               self.logger.info(n)
               self.htseq=self.add_tool('ref_rna.express.htseq')
               print tool_opt
               self.htseq.set_options(tool_opt)
               self.tool_lists.append(self.htseq)
               self.htseq.run()
            self.sumtool.append(self.htseq)
            print self.tool_lists
            self.on_rely(self.tool_lists, self.set_output, 'htseq')
            self.on_rely(self.tool_lists, self.set_step, {'end': self.step.htseq,'start': self.step.mergehtseq})

    def mergehtseq_run(arg):
        dir_path = self.output_dir + '/htseq'
        self.mergehtseq.set_options({
            "merge_files": self.output_dir + '/htseq'
        })
        self.sumtools.append(self.mergehtseq)
        self.mergehtseq.on('end', self.control_file_run)
        self.mergefeaturecounts.on('end', self.diff_Rexp_run)
        self.mergehtseq.on('end', self.set_output,'mergehtseq')
        self.mergehtseq.run()
        self.step.mergehtseq.finish()
        self.step.update()
        self.wait()

    # def diff_fpkm(self):
    #     """提取差异基因的fpkm和count表"""
    #     files=os.listdir(self.output_dir+'/diff')
    #     for f in files:
    #         if f.find('results_name') != -1:
    #             f_path = os.path.join(self.output_dir + '/diff', f)

    def get_list():
        list_path = self.option("sample_bam").prop['path']
        sample_number = os.listdir(list_path)
        return sample_number

    def linkdir(self, dirpath, dirname, output_dir):
        allfiles = os.listdir(dirpath)
        newdir = os.path.join(output_dir, dirname)
        if not os.path.exists(newdir):
            os.mkdir(newdir)
        oldfiles = [os.path.join(dirpath,i)for i in allfiles]
        newfiles = [os.path.join(newdir,i)for i in allfiles]
        for newfile in newfiles:
            if os.path.exists(newfile):
                os.remove(newfile)
        for i in range(len(allfiles)):
            os.link(oldfiles[i], newfiles[i])

    def set_step(self, event):
        if 'start' in event['data'].keys():
            event['data']['start'].start()
        if 'end' in event['data'].keys():
            event['data']['end'].finish()
        self.step.update()

    def set_output(self, event):
        obj=event['bind_object']
        try:
            self.logger.info("设置输出结果")
            if event['data'] == "featurecounts" or event['data'] == "htseq":
                for tool in self.tool_lists:
                    self.linkdir(tool.output_dir, event['data'], self.output_dir)
                # self.option("merge_files").set_path(os.path.join(self.output_dir, event['data']))
                # self.change_sample_name(self.output_dir+'/featurecounts')
                self.mergefeaturecounts_run()
            elif event['data'] == "mergefeaturecounts" or event['data'] =="mergehtseq":
                self.linkdir(obj.output_dir, event['data']+"_express", self.output_dir)
                # for root,dirs,files in os.walk(obj.output_dir):
                #     for f in files:
                #         file_path = os.path.join(obj.output_dir, f)
                #         file_dir = event['data']+"_express/" + f
                #         # self.logger.info(file_path)
                #         # self.logger.info(file_dir)
                #         if f.find("fpkm") != -1:
                #             gene_fpkm_path = os.path.join(self.output_dir, file_dir)
                #             self.option("fpkm").set_path(gene_fpkm_path)
                #         if f.find("count") != -1:
                #             gene_count_path = os.path.join(self.output_dir, file_dir)
                #             self.option("count").set_path(gene_count_path)
                self.logger.info("设置gene count 和 fpkm 输出结果成功！")
                self.diff_Rexp_run()
                # self.option('control_file').set_path(self.output_dir+'/control_file.txt')
            elif event['data'] == "diff_Rexp":
                self.logger.info(event['data'])
                file_path = self.work_dir+"/DiffRexp/output"
                dir_path = os.listdir(file_path)
                # self.logger.info(file_path)
                self.linkdir(file_path, 'diff', self.output_dir)
                module_path = self.output_dir + "/diff"
                # for f in dir_path:
                #     if f.find("Rscript") != -1:
                #         remove_path = os.path.join(file_path, f)
                #         os.remove(remove_path)
                #     if f.find('results_select') != -1:
                #         fpkm_path = os.path.join(module_path, f)
                #         self.option("diff_fpkm").set_path(fpkm_path)
                #     if f.find('results_select') != -1:
                #         count_path = os.path.join(module_path, f)
                #         self.option("diff_count").set_path(count_path)
                #     if re.match(r'results_name', f):
                #         gene_list_path = os.path.join(module_path, f)
                #         self.option('gene_file').set_path(gene_list_path)
            self.logger.info("end!")
                # self.linkdir(dir_path, "diff_exp", self.output_dir)
        except:
            self.logger.info("输出结果失败")
        self.end()

    def run(self):
        if self.option("express_method") == "featurecounts":
            self.featurecounts_run()
        elif self.option("express_method") == "htseq":
            self.htseq_run()
        elif self.option("express_method") == "both":
            self.featurecounts_run()
            self.htseq_run()
        super(ExpressModule, self).run()

    def end(self):
        repaths=[
            [".","","表达量分析模块结果输出目录"],
            ['./express/gene_count', 'txt', '基因count值'],
            ['./express/gene_fpkm', 'txt', '基因fpkm值'],
            ['./diff_express/diff_count', '', '差异基因的count值'],
            ['./diff_express/diff_fpkm', '', '差异基因的fpkm值'],
            ['./diff_express/gene_file', '', '差异基因的列表'],
        ]
        regexps=[
            [r"./featurecounts/sample", "", "featurecounts分析结果输出目录"],
        ]
        #sdir=self.add_upload_dir(self.output_dir)
        #sdir.add_relpath_rules(repaths)
        #sdir.add_regexp_rules(regexps)
        super(ExpressModule, self).end()

# def draw_diff_gene_count_fpkm(gene_file_path, fpkm_path, out_fpkm_path):
#     with open(gene_file_path, "r+") as f, open(fpkm_path, "r+") as s, open(out_fpkm_path,"w+") as k:
#         gene_list=[]
#         i=0
#         for line in f:
#             if line:
#                 line1=line.strip()
#                 if len(line1)==1:
#                     gene_list.append(line1)  #假设会有差异表达基因
#                 else:
#                     print "没有差异表达基因的产生！"
#         fpkm_gene_list=[]
#         for l in s:
#                 i+=1
#                 l1=l.strip().split("\t")
#                 _write="\t".join(l1)+"\n"
#                 if i==1:
#                     k.write(_write)
#                     next
#                 else:
#                 if len(l1)>1:
#                     if l1[0] in gene_list:
#                         k.write(_write)
