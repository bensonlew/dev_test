# -*- coding: utf-8 -*-
# __author__ = 'yuguo'

"""Usearch OTU聚类工具"""

from biocluster.tool import Tool
from biocluster.agent import Agent
import os
from biocluster.core.exceptions import OptionError


class UsearchOtuAgent(Agent):
    """
    Usearch：uparse
    version v7
    author：yuguo
    last_modify:2015.11.03
    """

    def __init__(self, parent=None):
        super(UsearchOtuAgent, self).__init__(parent)
        options = [
            {'name': 'fasta', 'type': 'infile', 'format': 'sequence.fasta'},  # 输入fasta文件，序列名称格式为'>sampleID_seqID'.
            {'name': 'identity', 'type': 'float', 'default': 0.97},  # 相似性值，范围0-1.
            {'name': 'otu_table', 'type': 'outfile', 'format': 'meta.otu.otu_table'},  # 输出结果otu表
            {'name': 'otu_rep', 'type': 'outfile', 'format': 'sequence.fasta'},  # 输出结果otu代表序列
            {'name': 'otu_seqids', 'type': 'outfile', 'format': 'meta.otu.otu_seqids'},  # 输出结果otu中包含序列列表
            {'name': 'otu_biom', 'type': 'outfile', 'format': 'meta.otu.biom'}  # 输出结果biom格式otu表
        ]
        self.add_option(options)
        self.step.add_steps('OTUCluster')
        self.on('start', self.step_start)
        self.on('end', self.step_end)

    def step_start(self):
        self.step.OTUCluster.start()
        self.step.update()

    def step_end(self):
        self.step.OTUCluster.finish()
        self.step.update()

    def check_options(self):
        """
        检查参数设置
        """
        if not self.option("fasta").is_set:
            raise OptionError("必须设置输入fasta文件.")
        if self.option("identity") < 0 or self.option("identity") > 1:
            raise OptionError("identity值必须在0-1范围内.")
        return True

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"],
            ["otu_reps.fasta", "sequence.fasta", "代表序列"],
            ["otu_seqids.txt", "xls", "OTU代表序列对应表"],
            ["otu_table.biom", 'meta.otu.biom', "OTU表对应的Biom文件"],
            ["otu_table.xls", "meta.otu.otu_table", "OTU表"]
        ])
        super(UsearchOtuAgent, self).end()

    def set_resource(self):
        """
        设置所需资源，需在之类中重写此方法 self._cpu ,self._memory
        :return:
        """
        self._cpu = 10
        self._memory = ''


class UsearchOtuTool(Tool):
    """
    UsearchOTU tool
    """

    def __init__(self, config):

        super(UsearchOtuTool, self).__init__(config)
        self._version = "v7.0"
        self.usearch_path = "bioinfo/meta/usearch-v7.0/"
        self.script_path = "bioinfo/meta/scripts/"
        self.qiime_path = "program/Python/bin/"

    def cmd1(self):
        cmd = self.usearch_path+"uparse -derep_prefix meta.fasta -output meta_derepprefix.fasta -sizeout"
        return cmd

    def cmd2(self):
        cmd = self.usearch_path+"uparse -sortbysize meta_derepprefix.fasta -output meta_derepprefix_sorted.fasta -minsize 2"
        return cmd

    def cmd3(self):
        ratio = str(100-float(self.option('identity'))*100)
        cmd = self.usearch_path+"uparse -cluster_otus meta_derepprefix_sorted.fasta -otus cluster.fasta -otu_radius_pct "+ratio
        return cmd

    def cmd4(self):
        cmd = self.usearch_path+"uparse -usearch_global meta.fasta -db cluster.fasta -strand plus -id "+str(self.option('identity'))+" -uc map.uc"
        return cmd

    def cmd5(self):
        cmd = self.script_path+"""uc2otuseqids.pl -i map.uc -o cluster.seqids"""
        return cmd

    def cmd6(self):
        os.system("""cat cluster.seqids|awk '{split($0,line,\"\\t\");new=line[1];for(i=2;i<NF+1;i++){match(line[i],/_[^_]+$/);smp=substr(line[i],1,RSTART-1);id=substr(line[i],RSTART+1,RLENGTH);nsmp=smp;gsub(/_/,\".\",nsmp);new=new\"\\t\"nsmp\"_\"id;print nsmp\"\\t\"smp;print line[i]\"\\t\"smp >\"cluster.groups\"};print new >\"cluster.seqids.tmp\";}'|sort|uniq >name.check""")
        os.system("""awk '{ print $1,\"OTU\"NR >\"cluster2otu.rename\";$1=\"OTU\"NR;print $0 }' cluster.seqids|sed 's/ /\\t/g' >otu_seqids.txt""")
        os.system("""awk '{$1=\"OTU\"NR;print $0}' cluster.seqids.tmp|sed 's/ /\\t/g' > otu.seqids.tmp""")
        cmd = self.qiime_path+"""make_otu_table.py -i otu.seqids.tmp  -o otu_table.biom"""
        return cmd

    def cmd7(self):
        os.system("""cat name.check|awk '{gsub(/\\./,\"\\\\.\",$1);print \"sed '\\''s/\\\"\"$1\"\\\"/\\\"\"$2\"\\\"/g'\\''  otu_table.biom >otu_table.biom.tmp\\nmv otu_table.biom.tmp otu_table.biom\";}' >otu.name.check.sh""")
        os.system("""sh otu.name.check.sh""")
        cmd = self.qiime_path+"""biom convert -i otu_table.biom -o otu_table.txt  --table-type \"OTU table\"  --to-tsv"""
        return cmd

    def cmd8(self):
        os.system("""cat otu_table.txt|sed -n '2p'|sed 's/#//' >otu_table.xls""")
        os.system("""cat otu_table.txt|sed -n '3,$p'|sort -V |sed 's/\\.0//g' >>otu_table.xls""")
        cmd = self.qiime_path+"""pick_rep_set.py -i otu_seqids.txt -f meta.fasta -m most_abundant -o otu_reps.fasta"""
        return cmd

    def set_output(self):
        self.logger.info("设置输出结果")
        # self.logger.info(self.work_dir+'/otu_table.xls')
        # self.logger.info(self.output_dir+'/otu_table.xls')
        os.system('rm -rf '+self.output_dir)
        os.system('mkdir '+self.output_dir)
        os.link(self.work_dir+'/otu_table.xls', self.output_dir+'/otu_table.xls')
        self.option('otu_table').set_path(self.output_dir+'/otu_table.xls')
        self.option('otu_table').check()
        os.link(self.work_dir+'/otu_reps.fasta', self.output_dir+'/otu_reps.fasta')
        self.option('otu_rep').set_path(self.output_dir+'/otu_reps.fasta')
        self.option('otu_rep').check()
        os.link(self.work_dir+'/otu_seqids.txt', self.output_dir+'/otu_seqids.txt')
        self.option('otu_seqids').set_path(self.output_dir+'/otu_seqids.txt')
        self.option('otu_seqids').check()
        os.link(self.work_dir+'/otu_table.biom', self.output_dir+'/otu_table.biom')
        self.option('otu_biom').set_path(self.output_dir+'/otu_table.biom')
        self.option('otu_biom').check()
        self.logger.info("OK,all things done.")

    def run(self):
        super(UsearchOtuTool, self).run()
        self.logger.info("将输入文件链接到工作目录")
        if os.path.exists(self.work_dir+'/meta.fasta'):
            os.remove(self.work_dir+'/meta.fasta')
        os.link(self.option("fasta").prop['path'], self.work_dir+'/meta.fasta')
        self.logger.info("OK")
        i = 0
        while i < 8:
            i += 1
            self.logger.info("开始运行cmd"+str(i))
            cmd = getattr(self, 'cmd'+str(i))()
            command = self.add_command('cmd'+str(i), cmd)
            command.run()
            self.wait(command)
            if command.return_code == 0:
                self.logger.info("运行cmd"+str(i)+"完成")
            else:
                self.set_error("cmd"+str(i)+"运行出错!")
                raise Exception("cmd{}运行出错".format(i))
        self.set_output()
        self.end()
