#!/usr/bin/python
# -*- coding: utf-8 -*-
import os
import re
from bson.objectid import ObjectId
import types
import json, time
import datetime
from bson.son import SON
from biocluster.api.database.base import Base, report_check
from biocluster.config import Config
from collections import OrderedDict
import unittest


class RefrnaGeneDetail(Base):
    def __init__(self, bind_object):
        super(RefrnaGeneDetail, self).__init__(bind_object)
        self._db_name = Config().MONGODB + '_ref_rna'
        # self.db = Config().mongo_client[Config().MONGODB + "_ref_rna"]

    @staticmethod
    def biomart(biomart_path):
        """
        为了获得已知基因的description, gene_type信息
        :param biomart_path:
        :return: dict, gene_id:  {"trans_id": [trans_id], "gene_name": [gene_name], "chromosome": [chromosome],
                                 "gene_type": [gene_type], "description": [desc], "strand": [strand],
                                 "pep_id": [pep_id], "start": [start], "end": [end]}
        """

        def check(_id):
            if not _id:
                return '-'
            else:
                return _id

        ss = 0
        biomart_info = dict()
        with open(biomart_path, 'r+') as f1:
            for lines in f1:
                if not lines.strip():
                    continue
                ss += 1
                line = lines.strip('\n').split("\t")
                gene_id = check(line[0])
                trans_id = check(line[1])
                gene_name = check(line[2])
                chromosome = check(line[9])
                gene_type = check(line[17])
                desc = check(line[8])
                strand_tmp = check(line[12])
                if strand_tmp == "1":
                    strand = "+"
                elif strand_tmp == "-1":
                    strand = "-"
                else:
                    strand = "."
                start = check(str(line[10]))
                end = check(str(line[11]))
                # location = "{}:{}-{}".format(check(line[12]), check(str(line[10])), check(str(line[11])))
                pep_id = check(line[6])

                if gene_id not in biomart_info.keys():
                    biomart_info[gene_id] = {"trans_id": [trans_id], "gene_name": [gene_name],
                                             "chromosome": [chromosome], "gene_type": [gene_type],
                                             "description": [desc], "strand": [strand], "pep_id": [pep_id],
                                             "start": [start], "end": [end]}
                else:
                    biomart_info[gene_id]['trans_id'].append(trans_id)
                    biomart_info[gene_id]['gene_name'].append(gene_name)
                    biomart_info[gene_id]['chromosome'].append(chromosome)
                    biomart_info[gene_id]['gene_type'].append(gene_type)
                    biomart_info[gene_id]['description'].append(desc)
                    # biomart_info[gene_id]['location'].append(location)
                    biomart_info[gene_id]['pep_id'].append(pep_id)
                    biomart_info[gene_id]['strand'].append(strand)
                    biomart_info[gene_id]['start'].append(start)
                    biomart_info[gene_id]['end'].append(end)
        if not biomart_info:
            print "没有生成biomart信息"
        print "biomart共统计出{}行信息".format(str(ss))
        return biomart_info

    @staticmethod
    def get_cds_seq(cds_path):
        """
        从已经下载好的cds序列文件中提取转录本对应的cds序列。
        :param cds_path: cds序列文件的绝对路径
        :return: dict，转录本id：{"name": cds_id, "sequence": cds_sequence, "sequence_length": len(cds_sequence)}
        """
        start = time.time()
        trans = dict()
        with open(cds_path, 'r+') as f1:
            j = 0
            trans_id, cds_id, cds_sequence = '', '', ''
            for lines in f1:
                if not lines.strip():
                    continue
                line = lines.strip()
                if re.search(r'>', line):
                    j += 1
                    cds_m = re.search(r'>(\w+\.\w+).\w+.+gene:(\w+)', line)
                    trans_m = re.search(r'>(\w+).\w+', line)
                    if cds_m:
                        if j > 1:
                            if trans_id not in trans.keys():
                                trans[trans_id] = {"name": cds_id, "sequence": cds_sequence,
                                                   "sequence_length": len(cds_sequence)}
                        cds_id = cds_m.group(1)
                        trans_id = trans_m.group(1)
                        cds_sequence = ''
                else:
                    cds_sequence += line
            if trans_id not in trans.keys():
                trans[trans_id] = {"name": cds_id, "sequence": cds_sequence, "sequence_length": len(cds_sequence)}
        if not trans:
            print '提取cds序列信息为空'
        print "共统计出{}行以'>'开头的信息".format(str(j))
        end = time.time()
        duration = end - start
        m, s = divmod(duration, 60)
        h, m = divmod(m, 60)
        print('cds提取运行的时间为{}h:{}m:{}s'.format(h, m, s))
        return trans

    @staticmethod
    def get_pep_seq(pep_path):
        """
        get transcript's pep info, including protein sequence
        :param pep_path:
        :return: dict, trans[trans_id]={"name": pep_id, "sequence": pep_sequence, "sequence_length": len(pep_sequence)}
        """
        start = time.time()
        trans = dict()
        with open(pep_path, 'r+') as f1:
            j = 0
            pep_sequence, pep_id = '', ''
            for lines in f1:
                line = lines.strip()
                if not line:
                    continue
                if re.search(r'>', line):
                    j += 1
                    if j > 1:
                        if trans_id not in trans.keys():
                            trans[trans_id] = {"name": pep_id, "sequence": pep_sequence,
                                               "sequence_length": len(pep_sequence)}
                    pep_m = re.search(r'>(\w+\.\w+)', line)
                    if pep_m:
                        pep_id = pep_m.group(1)
                    trans_m = re.search(r'transcript:(\w+)', line)
                    if trans_m:
                        trans_id = trans_m.group(1)
                    pep_sequence = ''
                else:
                    pep_sequence += line
            if trans_id not in trans.keys():
                trans[trans_id] = {"name": pep_id, "sequence": pep_sequence, "sequence_length": len(pep_sequence)}
        if not trans:
            print '提取cds序列信息为空'
        print "共统计出{}行以'>'开头的信息".format(str(j))
        end = time.time()
        duration = end - start
        m, s = divmod(duration, 60)
        h, m = divmod(m, 60)
        print('pep提取运行的时间为{}h:{}m:{}s'.format(h, m, s))
        return trans

    def get_accession_entrez_id(self, pep_accession):
        collection = self.db["sg_entrez_id"]
        data = collection.find_one({"entrez_pep_id": pep_accession})
        if data:
            entrez_id = data["entrez_gene_id"]
            return entrez_id
        else:
            print "{}没有找对对应的entrez_id信息".format(pep_accession)
            return "-"

    def get_new_gene_description_entrez(self, query_id, blast_id=None):
        """根据新基因nr比对的结果 找出蛋白的accession号，同时根据蛋白的accession号从sg_entrez_id中获得基因的entrez_id
            并添加该entrez_id的ncbi链接
            query_id, 查询的基因或转录本的id
            blast_id, sg_annotaion_blast_detail中的blast_id，是ObjectId对象
        """
        if not isinstance(blast_id, ObjectId):
            if isinstance(blast_id, types.StringTypes):
                blast_id = ObjectId(blast_id)
            else:
                raise Exception('blast_id 必须为ObjectId对象或其对应的字符串！')
        collection = self.db["sg_annotation_blast_detail"]
        data = collection.find_one({"blast_id": blast_id, "database": "nr", "anno_type": "gene", 'query_id': query_id})
        if data:
            hit_name = data["hit_name"]
            pep_accession = hit_name.split("|")[3]
            entrez_id = self.get_accession_entrez_id(pep_accession)
            description = data['description']
            return entrez_id, description
        else:
            return '-', '-'

    def query_entrezid(self, ensem_gene_id=None, is_new=None, blast_id=None):
        """
        只能对参考基因组上的ensembl id查询
        :param ensem_gene_id:
        :param is_new:
        :param blast_id: sg_annotaion_blast_detail中的blast_id，是ObjectId对象
        :return:
        """
        collection = self.db["sg_entrez_id"]
        if not is_new:
            data = collection.find_one({"ensem_gene_id": ensem_gene_id})
            if data:
                return int(data['geneid'])
            else:
                return '-'
        else:
            entrez_id, description = self.get_new_gene_description_entrez(query_id=ensem_gene_id, blast_id=blast_id)
            return entrez_id, description

    @staticmethod
    def gene_location(new_gene_path):
        """
        Get gene location from provided bed file
        :param new_gene_path: absolute path of novel gene file
        :return: dict, gene:{chr, strand, start, end}
        """
        new_gene_info = dict()
        with open(new_gene_path, 'r+') as f1:
            f1.readline()
            for lines in f1:
                line = lines.strip('\n').split("\t")
                if line[3] not in new_gene_info.keys():
                    new_gene_info[line[3]] = {"chr": line[0], "strand": line[5],
                                              "start": int(line[1])+1, "end": line[2]}
                else:
                    raise Exception("在提取新基因的位置信息中,基因{}有重复的位置信息".format(line[3]))
        return new_gene_info

    @staticmethod
    def gene2transcript(class_code):
        """
        根据输入文件提取基因和转录本对应的关系字典
        :param class_code: 文件名，tab分割，第一列是转录本，第二列是基因，第一行是header
        :return: dict, gene_id:[tid1,tid2]
        """
        start = time.time()
        with open(class_code, 'r+') as f1:
            f1.readline()
            extract_class_code = {}
            for lines in f1:
                if not lines.strip():
                    continue
                line = lines.strip('\n').split("\t")
                if line[1] not in extract_class_code.keys():
                    extract_class_code[line[1]] = [line[0]]
                else:
                    extract_class_code[line[1]].append(line[0])
            if extract_class_code:
                end = time.time()
                duration = end - start
                m, s = divmod(duration, 60)
                h, m = divmod(m, 60)
                print('class_code提取运行的时间为{}h:{}m:{}s'.format(h, m, s))
                return extract_class_code
            else:
                print '没有从{}提取出基因对应的转录本信息'.format(class_code)
                raise Exception("error")

    @staticmethod
    def fasta2dict(fasta_file):
        """
        提取gene和transcript序列信息是同一个函数
        :param fasta_file:
        :return: dict, seq_id: sequence
        """
        start = time.time()
        with open(fasta_file, 'r+') as fasta:
            seq = dict()
            j = 0
            seq_id, sequence = '', ''
            for lines in fasta:
                line = lines.strip()
                if not line or line.startswith('#'):
                    continue
                if line.startswith('>'):
                    j += 1
                    if j > 1:
                        seq[seq_id] = sequence
                        sequence = ''

                    # get seq name
                    seq_id = line.split()[0].strip('>')
                    if '(' in seq_id:
                        seq_id = seq_id.split('(')[0]
                    elif '|' in seq_id:
                        seq_id = seq_id.split('|')[0]
                    else:
                        pass
                else:
                    sequence += line
            # save the last sequence
            seq[seq_id] = sequence
        if not seq:
            print '提取序列信息为空'

        end = time.time()
        duration = end - start
        m, s = divmod(duration, 60)
        h, m = divmod(m, 60)
        print "共统计出{}条序列信息".format(len(seq))
        print('从{}提取序列耗时{}h:{}m:{}s'.format(fasta_file, h, m, s))
        return seq

    def add_express_diff_class_code_detail(self, class_code, class_code_id):
        if not isinstance(class_code_id, ObjectId):
            # 如果class_cod_id不是bson的ObjectId对象，则继续判断是不是字符串。
            # MongoDB使用了BSON这种结构来存储数据和网络数据交换。
            if isinstance(class_code_id, types.StringTypes):
                class_code_id = ObjectId(class_code_id)
            else:
                raise Exception('class_code_id必须为ObjectId对象或其对应的字符串！')
        data_list = list()
        with open(class_code, 'r+') as f:
            f.readline()
            for line in f:
                line = line.strip().split("\t")
                tmp_data = [('assembly_trans_id', line[0]),
                            ('assembly_gene_id', line[1]),
                            ('class_code', line[2]),
                            ('gene_name', line[3]),
                            ('class_code_id', class_code_id),
                            ("type", "express_diff")]
                tmp_data = SON(tmp_data)
                data_list.append(tmp_data)
        try:
            collection = self.db["sg_express_class_code_detail"]
            collection.insert_many(data_list)
        except Exception, e:
            print("导入%s表出错:%s" % (class_code, e))
        else:
            print("导入%s表成功！" % (class_code))

    def add_gene_detail_class_code_detail(self, class_code, biomart_path=None, class_code_id=None, blast_id=None,
                                          gene_location_path=None, trans_location_path=None, cds_path=None,
                                          pep_path=None, transcript_path=None, gene_path=None, species=None):
        """
        :param biomart_path: 获取已知基因的description和gene_type信息
        :param class_code: class_code文件
        :param class_code_id: class_code_id 的主表
        :param blast_id: nr统计表 sg_annotation_blast_detail 的导表函数id
        :param gene_location_path: 基因的bed文件
        :param trans_location_path: 转录本的bed文件
        :param cds_path: cds文件路径
        :param pep_path: pep文件路径
        :param transcript_path: 转录本的fa文件
        :param gene_path: 基因的fa文件
        :param species: 物种名称(拉丁文)
        """
        start = time.time()

        data_list = list()
        gene2trans = self.gene2transcript(class_code)  # 获得基因和转录本的对应关系
        if gene_location_path:
            gene_location_info = self.gene_location(gene_location_path)  # 获得所有基因的bed信息(包括ref和new)
        if trans_location_path:
            trans_location_info = self.gene_location(trans_location_path)  # 获得转录本的bed信息(包括ref和new)
        if cds_path:
            trans_cds_info = self.get_cds_seq(cds_path)  # 转录本和cds对应关系
        if pep_path:
            trans_pep_info = self.get_pep_seq(pep_path)  # 转录本和pep对应关系
        if biomart_path:
            biomart_data = self.biomart(biomart_path)  # 获取已知基因的description, gene_type信息
        if transcript_path:
            trans_sequence = self.fasta2dict(transcript_path)  # 提取转录本的fa序列
        if gene_path:
            gene_sequence_dict = self.fasta2dict(gene_path)
        if not species:
            raise Exception('species must be specified')
        if not blast_id:
            raise Exception('nr统计表 sg_annotation_blast_detail 的导表函数id')

        gene_id_list = list()
        line_id = 0
        with open(class_code, 'r+') as f1:
            f1.readline()
            for lines in f1:
                line_id += 1
                # if line_id <=2:
                if line_id % 1000 == 0:
                    print line_id
                if not lines.strip() or lines.startswith('#'):
                    continue
                line = lines.strip('\n').split("\t")
                description, gene_type = '', ''
                if line[1] in gene_id_list:
                    continue
                else:
                    gene_id_list.append(line[1])
                    transcript_info = OrderedDict()
                    is_new = True
                    if line[2] == 'u':
                        entrez_id = self.query_entrezid(line[1], is_new=False)  # 获得已知基因的entrez_id
                        is_new = False
                    else:
                        # 获得新基因的entrez_id
                        entrez_id, description = self.query_entrezid(line[1], is_new=True, blast_id=blast_id)

                    trans_list = gene2trans[line[1]]
                    transcript_num = len(trans_list)
                    if not trans_list:
                        print '{}基因没有对应的转录本信息'.format(line[1])
                        raise Exception("error!")
                    else:
                        transcript = ",".join(trans_list)

                    for ind, trans_ll in enumerate(trans_list):
                        if not re.search('MSTRG', trans_ll) and not re.search(r'TCONS', trans_ll):
                            # 表示是已知转录本, 已知转录本输入的是cds和pep信息, 其键值索引是转录本的ensembl编号信息
                            if trans_ll not in transcript_info.keys():
                                transcript_info[trans_ll] = dict()
                                count_none = 0
                                if trans_ll in trans_cds_info.keys():
                                    transcript_info[trans_ll]["cds"] = trans_cds_info[trans_ll]
                                else:
                                    count_none += 1
                                if trans_ll in trans_pep_info.keys():
                                    transcript_info[trans_ll]["pep"] = trans_pep_info[trans_ll]
                                else:
                                    count_none += 1
                                if trans_ll in trans_sequence.keys():
                                    transcript_info[trans_ll]["length"] = trans_sequence[trans_ll]
                                else:
                                    # transcript_info.pop(trans_ll)
                                    transcript_info[trans_ll]["length"] = 0
                                if count_none == 2:
                                    transcript_info.pop(trans_ll)
                        else:
                            # 新转录本输入的是其在new_trans_list中的编号信息
                            if trans_ll not in transcript_info.keys():
                                if trans_ll in trans_location_info.keys():
                                    # 新转录本的start，end信息
                                    tmp_value = trans_location_info[trans_ll]
                                    transcript_info[str(ind)] = {"start": tmp_value['start'], 'end': tmp_value['end']}
                                if trans_ll in trans_sequence.keys():
                                    # 新转录本的sequence，length信息
                                    transcript_info[str(ind)]['sequence'] = trans_sequence[str(ind)]['sequence']
                                    transcript_info[str(ind)]['sequence'] = trans_sequence[str(ind)]['length']

                    if biomart_path:
                        if line[1] in biomart_data.keys():
                            # 获取已知基因的descriptio，gene_type信息
                            description = biomart_data[line[1]]["description"][0]
                            gene_type = biomart_data[line[1]]['gene_type'][0]
                        elif description:
                            description = 'entrez_description: ' + description
                        else:
                            pass

                    if line[1] in gene_location_info.keys():
                        start = gene_location_info[line[1]]['start']
                        end = gene_location_info[line[1]]['end']
                        strand = gene_location_info[line[1]]['strand']
                        chrom = gene_location_info[line[1]]['chr']

                    else:
                        start, end, strand, chrom = '-', '-', '-', '-'

                    if line[1] in gene_sequence_dict.keys():
                        gene_sequence = gene_sequence_dict[line[1]]
                        gene_length = len(gene_sequence)
                    else:
                        try:
                            gene_sequence = '-'
                            gene_length = int(end) - int(start)+1
                        except Exception:
                            print "{}没有获取对应的gene_sequence序列信息".format(line[1])
                            raise Exception("error")
                    data = [
                        ("is_new", is_new),
                        ("type", "gene_detail"),
                        ("class_code_id", class_code_id),
                        ("gene_id", line[1]),
                        ("entrez_id", entrez_id),
                        ("description", description),
                        ("strand", strand),
                        ("start", start),
                        ("location", "{}-{}".format(str(start), str(end))),
                        ("end", end),
                        ("chrom", chrom),
                        ("gene_name", line[3]),
                        ("transcript", transcript),
                        ("transcript_number", transcript_num),
                        ("gene_type", gene_type),
                        ("gene_sequence", gene_sequence),
                        ("gene_length", gene_length),
                        ("gene_ncbi", "https://www.ncbi.nlm.nih.gov/gquery/?term={}".format(line[1])),
                        ("gene_ensembl", "http://www.ensembl.org/{}/Gene/Summary?g={}".format(species, line[1])),
                    ]
                    if not transcript_info:
                        data.append(("trans_info", None))
                    else:
                        data.append(("trans_info", transcript_info))
                    # print data
                    data = SON(data)
                    data_list.append(data)

        print "共统计出{}基因".format(str(len(gene_id_list)))
        end = time.time()
        duration = end - start
        m, s = divmod(duration, 60)
        h, m = divmod(m, 60)
        print('整个程序运行的时间为{}h:{}m:{}s'.format(h, m, s))
        try:
            collection = self.db["sg_express_class_code_detail"]
            collection.insert_many(data_list)
        except Exception, e:
            print("导入%s表出错:%s" % (class_code, e))
        else:
            print("导入%s表成功！" % (class_code))

    def add_class_code(self, assembly_method, name=None, major_gene_detail=False, major_express_diff=False, species=None,
                       class_code_path=None, gene_location_path=None, trans_location_path=None, biomart_path=None,
                       cds_path=None, pep_path=None, transcript_path=None, gene_path=None, blast_id=None, test_this=False):
        if not test_this:
            task_id = self.bind_obj.sheet.task_id
            project_sn = self.bind_obj.sheet.project_sn
        else:
            task_id = 'demo_test_gdq'
            project_sn = 'demo_test_gdq'
        if not species:
            raise Exception('species must be specified')
        if not blast_id:
            raise Exception('nr统计表 sg_annotation_blast_detail 的导表函数id')

        data = [
            ('task_id', task_id),
            ('project_sn', project_sn),
            ('assembly_method', assembly_method),
            ('desc', 'class_code信息'),
            ('created_ts', datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
            ('status', 'end'),
            ('name', name if name else 'Classcode_' + str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S")))
        ]
        if not os.path.exists(transcript_path):
            raise Exception("{}文件不存在!".format(transcript_path))
        else:
            transcript_length = 0
            with open(transcript_path, 'r+') as f1:
                for lines in f1:
                    if lines.startswith('>'):
                        continue
                    else:
                        transcript_length += int(len(lines.strip()))
            data.append(('transcripts_total_length', transcript_length))
        collection = self.db["sg_express_class_code"]
        try:
            class_code_id = collection.insert_one(SON(data)).inserted_id
        except Exception:
            print("导入class_code主表信息错误")
        else:
            print("导入class_code主表信息成功!")
        if major_gene_detail:
            self.add_gene_detail_class_code_detail(class_code=class_code_path, biomart_path=biomart_path,
                                                   class_code_id=class_code_id, gene_location_path=gene_location_path,
                                                   trans_location_path=trans_location_path,
                                                   cds_path=cds_path, pep_path=pep_path,
                                                   transcript_path=transcript_path,
                                                   gene_path=gene_path, species=species,
                                                   blast_id=blast_id)
        if major_express_diff:
            self.add_express_diff_class_code_detail(class_code_path, class_code_id)


class TestFunction(unittest.TestCase):
    class TmpRefrnaGeneDetail(RefrnaGeneDetail):
        def __init__(self):
            self._db_name = Config().MONGODB + '_ref_rna'
            self._db = None
            self._config = Config()
    biomart_path = "/mnt/ilustre/users/sanger-dev/app/database/refGenome/Vertebrates/Mammalia/Mus_musculus/ref/" \
                   "mmusculus_gene_ensembl_gene.txt"
    pep_path = "/mnt/ilustre/users/sanger-dev/app/database/refGenome/Vertebrates/Mammalia/Mus_musculus/ref/" \
               "Mus_musculus.GRCm38.pep.all.fa"
    cds_path = "/mnt/ilustre/users/sanger-dev/app/database/refGenome/Vertebrates/Mammalia/Mus_musculus/" \
               "Mus_musculus.GRCm38.cds.all.fa"
    gene_bed = '/mnt/ilustre/users/sanger-dev/workspace/20170724/Single_gene_fa_5/GeneFa/ref_new_bed'
    trans_bed = '/mnt/ilustre/users/sanger-dev/workspace/20170724/Single_gene_fa_5/GeneFa/ref_new_trans_bed'
    gene_path = "/mnt/ilustre/users/sanger-dev/workspace/20170724/Single_gene_fa_2/GeneFa/output/gene.fa"
    transcript_path = "/mnt/ilustre/users/sanger-dev/workspace/20170706/" \
                      "Single_rsem_stringtie_mouse_total_2/Express1/TranscriptAbstract/output/exons.fa"
    class_code = "/mnt/ilustre/users/sanger-dev/workspace/20170702/" \
                 "Single_rsem_stringtie_mouse_total_1/Express/MergeRsem/class_code"

    a = TmpRefrnaGeneDetail()
    blast_id = a.db["sg_annotation_blast"].find_one({"task_id": "demo_0711"})
    blast_id = blast_id['_id']
    a.add_class_code(assembly_method="stringtie", name=None, major_gene_detail=True, major_express_diff=True,
                     biomart_path=biomart_path, gene_location_path=gene_bed, trans_location_path=trans_bed,
                     class_code_path=class_code,  cds_path=cds_path, pep_path=pep_path,
                     species='Mus_musculus', transcript_path=transcript_path, gene_path=gene_path, blast_id=blast_id,
                     test_this=True)

if __name__ == '__main__':
    unittest.main()
