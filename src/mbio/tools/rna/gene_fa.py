#!/usr/bin/python
# -*- coding: utf-8 -*-
# __author__: khl

import os,re
# import subprocess
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
import json, time

class GeneFaAgent(Agent):
    """
    提取新基因的fa文件，新基因的bed文件，新转录本的bed文件，新转录本的fa文件
    """
    def __init__(self, parent):
        super(GeneFaAgent, self).__init__(parent)
        options = [
            {"name":"ref_gff3", "type":"string"},  #ref gff文件
            {"name":"new_gtf", "type":"string"},  #新基因的gtf文件
            {"name":"ref_genome_custom","type":"string"}, #ref fa文件
            {"name":"assembly_method","type":"string","default":"stringtie"}, #拼接方法
            {"name":"gene_fa","type":"outfile","format":"sequence.fasta"}, #结果文件 基因的fa文件
            {"name":"gene_bed","type":"outfile","format":"gene_structure.bed"} #新基因的bed文件
        ]
        self.add_option(options)
        self.step.add_steps("gene_fa")
        self.on("start",self.step_start)
        self.on("end",self.step_end)

    def step_start(self):
        self.step.gene_fa.start()
        self.step.update()

    def step_end(self):
        self.step.gene_fa.finish()
        self.step.update()

    def check_options(self):
        pass

    def set_resource(self):
        self._cpu = 4
        self._memory = '5G'

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "gene.fa结果目录"],
        ])
        super(GeneFaAgent, self).end()

class GeneFaTool(Tool):

    def __init__(self, config):
        super(GeneFaTool, self).__init__(config)
        self.python_path = self.config.SOFTWARE_DIR + '/program/Python/bin/'
        self.getfasta_path  = "/bioinfo/rna/bedtools2-master/bin/bedtools"

    def get_ref_gene_bed(self,ref_gff,bed_path=None):
        """获得参考基因的bed文件"""
        # tmp = os.path.join(bed_path, 'tmp.gff')
        bed = os.path.join(bed_path, 'ref_bed')
        # cmd = """awk '{if($3=="gene"){print}}' %s > %s """ % (ref_gff, tmp)
        # cmd = """awk '{if($3=="gene"){print $0}}' {} > {}""".format(ref_gff,tmp)
        # os.system(cmd)
        gene_id_total = []
        with open(ref_gff, 'r+') as f1, open(bed, 'w+') as f2:
            for lines in f1:
                if re.search(r'gene',lines):
                    line = lines.strip().split("\t")
                    if len(line) > 5:
                        if re.search(r'gene', line[2]):
                            m_ = re.search(r'ID=gene:(\w+);', lines)
                            if m_:
                                gene_id = m_.group(1)
                            else:
                                continue
                                # raise Exception("{}没有提取到相应的信息".format(lines))
                        else:
                            continue
                            # raise Exception("{}没有提取到相应的信息".format(lines))
                        if gene_id not in gene_id_total:
                            start = int(line[3]) - 1  #gtf转bed start位置需要减去1
                            f2.write(line[0] + "\t" + str(start) + "\t" + str(line[4]) + "\t" + gene_id + "\t0\t" + line[6] + "\n")
                        else:
                            pass
                    else:
                        continue
                else:
                    continue
        print "提取的ref_bed文件路径为{}".format(bed)
        return bed

    def get_new_gene_bed(self, new_transcript_gtf=None, new_gene_bed=None, tmp_path=None, assembly_method='stringtie',
                              query_type=None,class_code_info=None):
        """
        :param new_transcript_gtf: stringtie或cufflinks生成的新转录本的gtf文件
        :param new_gene_bed: 基因的bed文件
        :param tmp_path: tmp文件的路径
        :param assembly_method: 组装方法
        :param query_type: 基因或转录本
        :param class_code_info: 列表形式,比如新基因只提取出class_code为['u']的就可以，新转录本需要提取出['i','j','u','o','x']
        :return:
        """
        # 此函数只适应于stringtie拼接的结果文件,cufflinks
        # cufflinks 组装生成的新基因 'XLOC_xx' 新转录本 'TCONS_xx' 样式
        start = time.time()
        print query_type
        if not os.path.exists(new_transcript_gtf):
            raise Exception("{}文件不存在".format(new_transcript_gtf))
        else:
            gene_location_info = {}
            tmp = os.path.join(tmp_path, "tmp_{}".format(query_type))
            if assembly_method=='stringtie':
                """stringtie组装生成的gtf文件第三列含有transcript，因此把transcript提取出来"""
                cmd = """awk '{if($3=="transcript"){print $0}}' %s > %s""" % (new_transcript_gtf, tmp)
                os.system(cmd)
                print '提取第三列为新转录本的序列'
            if assembly_method=='cufflinks':
                """cufflinks组装生成的gtf文件第三列不含有transcript,只是exon和cds等信息"""
                cmd = """cp {} > {}""".format(new_transcript_gtf, tmp)
                os.system(cmd)
            with open(tmp, 'r+') as f1, open(new_gene_bed, 'w+') as f2:
                for lines in f1:
                    line = lines.strip().split("\t")
                    m_ = re.search(r'class_code\s+\"(\w+)\";', lines)
                    if m_:
                        class_code = m_.group(1)
                        if class_code in class_code_info:
                                if assembly_method == 'stringtie':
                                    gene_id_m = re.search(r'gene_id\s+\"(\w+.\d+)\";', lines)
                                    trans_id_m = re.search(r'transcript_id\s+\"(\w+.\d+.\d+)\";', lines)
                                if assembly_method == 'cufflinks':
                                    gene_id_m = re.search(r'gene_id\s+\"(\w+)\"', lines)
                                    trans_id_m = re.search(r'transcript_id\s+\"(\w+)\"', lines)
                                if gene_id_m:
                                    gene_id = gene_id_m.group(1)
                                else:
                                    continue
                                if trans_id_m:
                                    trans_id = trans_id_m.group(1)
                                else:
                                    continue
                                if query_type == 'gene':
                                    seq_id = gene_id
                                if query_type == 'transcript':
                                    seq_id = trans_id
                                    # print "query_type为转录本时seq_id为{}".format(seq_id)
                                # start = int(line[3]) - 1
                                start = int(line[3])-1  # gff转为bed格式需要start坐标减去1
                                if seq_id not in gene_location_info.keys():
                                    gene_location_info[seq_id] = {}
                                    gene_location_info[seq_id]['start'] = start
                                    gene_location_info[seq_id]['end'] = line[4]
                                    gene_location_info[seq_id]['chr'] = line[0]
                                    gene_location_info[seq_id]['str'] = line[6]
                                else:
                                    if query_type == 'gene':
                                        if gene_location_info[seq_id]['start'] > start:
                                            gene_location_info[seq_id]['start'] = start
                                        if gene_location_info[seq_id]['end'] <= line[4]:
                                            gene_location_info[seq_id]['end'] = line[4]
                                    else:
                                        continue
                if gene_location_info:
                    for keys, values in gene_location_info.items():
                        info = [values['chr'], values['start'], values['end'], keys, '0', values['str']]
                        info_tmp = [str(i) for i in info]
                        f2.write("\t".join(info_tmp) + "\n")
            # print new_gene_bed
            end = time.time()
            duration = end - start
            print '提取新基因的bed序列耗时{}s'.format(str(duration))
            return new_gene_bed

    def cat_gene_bed(self,new_bed,ref_bed,bed_path,filename):
        output_path = os.path.join(bed_path,filename)
        cmd = """cat {} {} > {}""".format(new_bed, ref_bed, output_path)
        os.system(cmd)
        return output_path

    def get_gene_fasta(self,gene_bed,ref_fa,fa_path,filename):
        """通过getfasta得到基因的fa文件"""
        out_fa = os.path.join(fa_path, filename)
        cmd = "%s getfasta -fi %s -bed %s -fo %s -name -s" % (self.getfasta_path, ref_fa, gene_bed, out_fa)
        self.logger.info("开始打印cmd命令!")
        self.logger.info(cmd)

        fa_cmd = self.add_command("gene_fa",cmd).run()
        self.wait(fa_cmd)
        if fa_cmd.return_code == 0:
            self.logger.info("%s运行完成" % fa_cmd)
        else:
            self.set_error("%s运行出错" % fa_cmd)
        print "生成基因的fa序列"
        return out_fa

    def get_sequence_seq(self, sequence_path, _type="transcript", assembly_method='stringtie'):
        """提取gene和transcript序列信息是同一个函数,暂时没有用到这个函数"""
        start = time.time()
        seq = dict()
        j = 0
        with open(sequence_path, 'r+') as f1:
            for lines in f1:
                line = lines.strip()
                if re.search(r'>', line):
                    j += 1
                    if line.startswith('>MSTRG'):
                        m_ = re.search(r'\>(\w+\.\w+\.\d+).+gene=(\w+\.\w+)', line)  #新基因 新转录本 (stringtie)
                        if not m_:
                            m_ = re.search(r'\>(\w+\.\w+\.\d+).+gene=(\w+)', line)  # 已知基因 新转录本 (stringtie)
                    # elif line.startswith('>TCONS'):
                    #     m_ = re.search(r'\>(\w+\_\w+).+gene=(\w+\_\w+)',line)
                    else:
                        m_ = re.search(r'\>(\w+).+gene=(\w+)', line)  #已知基因 已知转录本 (stringtie) 或者cufflinks以上三种情况都适用
                    if m_:
                        if _type == "transcript":
                            if j > 1:
                                seq[trans_id] = {}
                                seq[trans_id]['sequence'] = sequence
                                seq[trans_id]['length'] = len(sequence)
                            trans_id = m_.group(1)
                            gene_id = m_.group(2)
                            # if trans_id not in seq.keys():
                            #     seq[trans_id] = {}
                            sequence = ''
                        else:
                            if j > 1:
                                seq[gene_id] = sequence
                            gene_id = m_.group(2)
                            # if gene_id not in seq.keys():
                            #     seq[gene_id]= {}
                            sequence = ''
                else:
                    sequence += line
                if _type == 'transcript':
                    seq[trans_id] = {}
                    seq[trans_id]['sequence'] = sequence
                    seq[trans_id]['length'] = len(sequence)
                else:
                    seq[gene_id] = sequence
        if not seq:
            print '提取{}序列信息为空'.format(_type)
        print "共统计出{}行信息".format(str(j))
        end = time.time()
        duration = end - start
        m, s = divmod(duration, 60)
        h, m = divmod(m, 60)
        print('{}序列提取运行的时间为{}h:{}m:{}s'.format(_type, h, m, s))
        return seq

    def run(self):
        super(GeneFaTool, self).run()
        ref_bed = self.get_ref_gene_bed(self.option("ref_gff3"),self.work_dir)
        new_bed = self.get_new_gene_bed(self.option("new_gtf"),self.work_dir + "/new_bed",self.work_dir,
                                        assembly_method=self.option("assembly_method"),query_type="gene",class_code_info=['u'])
        cat_bed = self.cat_gene_bed(new_bed,ref_bed,self.work_dir,"cat_bed")
        self.option("gene_bed").set_path(cat_bed)
        self.get_gene_fasta(cat_bed,self.option("ref_genome_custom"),self.output_dir,'gene.fa')
        self.end()

    def set_output(self):
        self.logger.info("设置结果目录")
        self.option("gene_fa").set_path(self.output_dir + "/gene.fa")
        self.logger.info("设置gene.fa路径成功")
