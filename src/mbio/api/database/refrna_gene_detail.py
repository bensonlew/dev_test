#!/usr/bin/python
# -*- coding: utf-8 -*-
import os
import re
import time
import datetime
import types
import unittest
from collections import OrderedDict, defaultdict
from bson.objectid import ObjectId
from bson.son import SON
from biocluster.api.database.base import Base, report_check
from biocluster.config import Config


class RefrnaGeneDetail(Base):
    def __init__(self, bind_object):
        super(RefrnaGeneDetail, self).__init__(bind_object)
        self._db_name = Config().MONGODB + '_ref_rna'

    @staticmethod
    def biomart(biomart_path, biomart_type='type1'):
        """
        为了获得已知基因的description, gene_type信息
        :param biomart_path: this file must be tab separated.
        :param biomart_type: the type of biomart file
        :return: dict, gene_id:  {"trans_id": [trans_id], "gene_name": [gene_name],
        "chromosome": [chromosome], "gene_type": [gene_type], "description": [desc],
         "strand": [strand], "pep_id": [pep_id], "start": [start], "end": [end]}
        """
        if biomart_type == "type1":
            gene_id_ind = 0
            trans_id_ind = 1
            gene_name_ind = 2
            chromosome_ind = 8
            gene_type_ind = 16
            desc_ind = 7
            strand_ind = 11
            start_ind = 9
            end_ind = 10
            pep_id_ind = 6
        elif biomart_type == 'type2':
            gene_id_ind = 0
            trans_id_ind = 1
            gene_name_ind = 2
            chromosome_ind = 6
            gene_type_ind = 14
            desc_ind = 5
            strand_ind = 9
            start_ind = 7
            end_ind = 8
            pep_id_ind = 4
        elif biomart_type == 'type3':
            gene_id_ind = 0
            trans_id_ind = 1
            gene_name_ind = 0
            chromosome_ind = 4
            gene_type_ind = 12
            desc_ind = 3
            strand_ind = 7
            start_ind = 5
            end_ind = 6
            pep_id_ind = 2
        else:
            raise ValueError('biomart_type should be one of type1, type2, type 3')

        biomart_info = dict()
        with open(biomart_path) as f:
            for line in f:
                if not line.strip():
                    continue
                line = line.replace('\t\t', '\t-\t')
                tmp_list = line.strip('\n').split("\t")
                gene_id = tmp_list[gene_id_ind]
                trans_id = tmp_list[trans_id_ind]
                gene_name = tmp_list[gene_name_ind]
                if biomart_type == 'type3':
                    gene_name = '-'
                chromosome = tmp_list[chromosome_ind]
                gene_type = tmp_list[gene_type_ind]
                desc = tmp_list[desc_ind]
                strand_tmp = tmp_list[strand_ind]
                if strand_tmp == "1":
                    strand = "+"
                elif strand_tmp == "-1":
                    strand = "-"
                elif strand_tmp == "0":
                    strand = "."
                else:
                    strand = strand_tmp
                start = tmp_list[start_ind]
                end = tmp_list[end_ind]
                pep_id = tmp_list[pep_id_ind]

                biomart_info.setdefault(gene_id, defaultdict(list))
                biomart_info[gene_id]['trans_id'].append(trans_id)
                biomart_info[gene_id]['gene_name'].append(gene_name)
                biomart_info[gene_id]['chromosome'].append(chromosome)
                biomart_info[gene_id]['gene_type'].append(gene_type)
                biomart_info[gene_id]['description'].append(desc)
                biomart_info[gene_id]['pep_id'].append(pep_id)
                biomart_info[gene_id]['strand'].append(strand)
                biomart_info[gene_id]['start'].append(start)
                biomart_info[gene_id]['end'].append(end)

        if not biomart_info:
            print("biomart information is None")
        # raw check
        if (not start.isdigit()) or (not end.isdigit()):
            raise NameError('we find "start" or "end" is not digit. Maybe biomart_type is wrong')
        print('Information of {} genes was parsed from biomart file'.format(len(biomart_info)))
        return biomart_info

    @staticmethod
    def get_cds_seq(cds_path):
        """
        从已经下载好的cds序列文件中提取转录本对应的cds序列。
        :param cds_path: cds序列文件的绝对路径
        :return: dict，转录本id：{"name": cds_id, "sequence": cds_sequence,
                                "sequence_length": len(cds_sequence)}
        """
        cds_dict = dict()
        cds_pattern = re.compile(r'>([^\s]+)')
        with open(cds_path, 'r+') as f:
            j = 0
            trans_id, cds_id, cds_sequence = '', '', ''
            for line in f:
                if not line.strip():
                    continue
                if line.startswith('>'):
                    j += 1
                    if j > 1:
                        seq_len = len(cds_sequence)
                        cds_dict[trans_id] = dict(name=cds_id, sequence=cds_sequence,
                                                  sequence_length=seq_len)
                        cds_dict[cds_id] = dict(name=cds_id, sequence=cds_sequence,
                                                sequence_length=seq_len)
                    cds_id = cds_pattern.match(line).group(1)
                    if '.' in cds_id:
                        trans_id = cds_id[:cds_id.rfind('.')]
                    else:
                        trans_id = cds_id
                    # cds_id and trans_id will be both saved as gtf may use either one of them
                    cds_sequence = ''
                else:
                    cds_sequence += line.strip()
            else:
                seq_len = len(cds_sequence)
                cds_dict[trans_id] = dict(name=cds_id, sequence=cds_sequence,
                                          sequence_length=seq_len)
                cds_dict[cds_id] = dict(name=cds_id, sequence=cds_sequence,
                                        sequence_length=seq_len)
        if not cds_dict:
            print('提取cds序列信息为空')
        print("共统计出{}条转录本的cds信息".format(len(cds_dict)))
        return cds_dict

    @staticmethod
    def get_pep_seq(pep_path):
        """
        get transcript's pep info, including protein sequence
        :param pep_path:
        :return: dict, trans_id={"name": pep_id, "sequence": pep_sequence,
                                 "sequence_length": len(pep_sequence)}
        """
        pep_dict = dict()
        j, trans_id, trans_id_else, pep_sequence, pep_id = 0, '', '', '', ''
        pep_pattern = re.compile(r'>([^\s]+)')
        trans_pattern = re.compile(r'transcript:([^\s]+)')

        with open(pep_path) as f:
            for line in f:
                if not line.strip():
                    continue
                if line.startswith('>'):
                    j += 1
                    if j > 1:
                        seq_len = len(pep_sequence)
                        pep_dict[trans_id] = dict(name=pep_id, sequence=pep_sequence,
                                                  sequence_lengt=seq_len)
                        pep_dict[trans_id_else] = dict(name=pep_id, sequence=pep_sequence,
                                                       sequence_lengt=seq_len)
                    pep_id = pep_pattern.match(line).group(1)
                    trans_id = trans_pattern.search(line).group(1)
                    if '.' in trans_id:
                        trans_id_else = trans_id[:trans_id.rfind('.')]
                    else:
                        trans_id_else = trans_id
                    pep_sequence = ''
                else:
                    pep_sequence += line.strip()
            else:
                seq_len = len(pep_sequence)
                pep_dict[trans_id] = dict(name=pep_id, sequence=pep_sequence,
                                          sequence_lengt=seq_len)
                pep_dict[trans_id_else] = dict(name=pep_id, sequence=pep_sequence,
                                               sequence_lengt=seq_len)
        if not pep_dict:
            print('提取蛋白序列信息为空')
        print("共统计出{}条转录本的蛋白序列信息".format(len(pep_dict)))
        return pep_dict

    def get_accession_entrez_id(self, pep_accession):
        collection = self.db["sg_entrez_id"]
        data = collection.find_one({"entrez_pep_id": pep_accession})
        if data:
            return data["entrez_gene_id"]
        else:
            print("没有找到｛｝对应的entrez_id信息".format(pep_accession))
            return "-"

    def get_new_gene_description_entrez(self, query_id, blast_id=None):
        """根据新基因nr比对的结果 找出蛋白的accession号，
        同时根据蛋白的accession号从sg_entrez_id中获得基因的entrez_id并添加该entrez_id的ncbi链接
        :param query_id: 查询的基因或转录本的id
        :param blast_id: sg_annotaion_blast_detail中的blast_id，是ObjectId对象
        """
        if not isinstance(blast_id, ObjectId):
            if isinstance(blast_id, types.StringTypes):
                blast_id = ObjectId(blast_id)
            else:
                raise Exception('blast_id 必须为ObjectId对象或其对应的字符串！')
        collection = self.db["sg_annotation_blast_detail"]
        data = collection.find_one({"blast_id": blast_id, "database": "nr",
                                    "anno_type": "gene", 'query_id': query_id})
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
            entrez_id, description = self.get_new_gene_description_entrez(query_id=ensem_gene_id,
                                                                          blast_id=blast_id)
            return entrez_id, description

    @staticmethod
    def gene_location(gene_bed_path):
        """
        Get gene location from provided bed file, this bed should have at least 6 columns.
        :param gene_bed_path: absolute path of novel gene file. First line will be skipped.
        :return: dict, gene:{chr, strand, start, end}
        """
        gene_info = dict()
        with open(gene_bed_path, 'r+') as f:
            _ = f.readline()
            for line in f:
                if not line.strip():
                    continue
                line = line.strip('\n').split("\t")
                if gene_info.get(line[3]) is None:
                    gene_info[line[3]] = {"chr": line[0], "strand": line[5],
                                          "start": str(int(line[1])+1), "end": line[2]}
                else:
                    raise Exception("{}中基因{}在多行出现".format(gene_bed_path, line[3]))
        return gene_info

    @staticmethod
    def gene2transcript(class_code):
        """
        根据输入文件提取基因和转录本对应的关系字典
        :param class_code: 文件名，tab分割，至少包含两列。第一列是转录本，第二列是基因，第一行是header.
        First line will be skipped.
        :return: dict, gene_id:[tid1,tid2]
        """
        with open(class_code, 'r+') as f1:
            f1.readline()
            gene2trans_dict = {}
            for lines in f1:
                if not lines.strip():
                    continue
                line = lines.strip('\n').split("\t")
                gene2trans_dict.setdefault(line[1], list())
                gene2trans_dict[line[1]].append(line[0])
            if gene2trans_dict:
                return gene2trans_dict
            else:
                error_info = '没有从{}提取出基因对应的转录本信息'.format(class_code)
                raise Exception(error_info)

    @staticmethod
    def fasta2dict(fasta_file):
        """
        提取gene和transcript序列信息，都会用这个函数
        :param fasta_file:
        :return: dict, seq_id: sequence
        """
        seq = dict()
        match_name = re.compile(r'>([^\s]+)').match
        with open(fasta_file, 'r+') as fasta:
            j = 0
            seq_id, sequence = '', ''
            for line in fasta:
                if line.startswith('#') or not line.strip():
                    continue
                if line.startswith('>'):
                    j += 1
                    if j > 1:
                        seq[seq_id] = sequence
                        sequence = ''
                    # get seq name
                    seq_id = match_name(line).group(1)
                    if '(' in seq_id:
                        seq_id = seq_id.split('(')[0]
                    elif '|' in seq_id:
                        seq_id = seq_id.split('|')[0]
                    else:
                        pass
                else:
                    sequence += line.strip()
            else:
                # save the last sequence
                seq[seq_id] = sequence

        if not seq:
            print '提取序列信息为空'
        print "从{}共统计出{}条序列信息".format(fasta_file, len(seq))
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
        with open(class_code) as f:
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
            print("导入%s表成功！" % class_code)

    def add_gene_detail_class_code_detail(self, class_code, biomart_path=None,
                                          biomart_type="type1", class_code_id=None, blast_id=None,
                                          gene_location_path=None, trans_location_path=None,
                                          cds_path=None, pep_path=None, transcript_path=None,
                                          gene_path=None, species=None):
        """
        :param biomart_path: 获取已知基因的description和gene_type信息
        :param biomart_type: biomart_type
        :param class_code: class_code文件
        :param class_code_id: class_code_id 的主表, 应该指被添加信息的表的id
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
            gene_location_info = self.gene_location(gene_location_path)
        if trans_location_path:
            trans_location_info = self.gene_location(trans_location_path)
        if cds_path:
            trans_cds_info = self.get_cds_seq(cds_path)  # 转录本和cds对应关系
        if pep_path:
            trans_pep_info = self.get_pep_seq(pep_path)  # 转录本和pep对应关系
        if biomart_path:
            biomart_data = self.biomart(biomart_path, biomart_type=biomart_type)
        if transcript_path:
            trans_sequence = self.fasta2dict(transcript_path)  # 提取转录本的fa序列
        if gene_path:
            gene_sequence_dict = self.fasta2dict(gene_path)
        if not species:
            raise Exception('species must be specified')
        if not blast_id:
            raise Exception('需提供nr统计表 sg_annotation_blast_detail 的导表函数id')

        gene_id_list = list()
        line_id = 0
        with open(class_code) as f1:
            f1.readline()
            for line in f1:
                line_id += 1
                if line_id % 1000 == 0:
                    print(line_id)
                if line.startswith('#') or not line.strip():
                    continue
                line = line.strip('\n').split("\t")
                description, gene_type, gene_name = '', '', ''
                gene_id = line[1]
                if gene_id in gene_id_list:
                    continue
                gene_id_list.append(gene_id)
                if line[2] != 'u':
                    entrez_id = self.query_entrezid(gene_id, is_new=False)  # 获得已知基因的entrez_id
                    is_new = False
                else:
                    is_new = True
                    # 获得新基因的entrez_id
                    entrez_id, description = self.query_entrezid(gene_id, is_new=True,
                                                                 blast_id=blast_id)

                trans_list = gene2trans[line[1]]
                transcript_num = len(trans_list)
                if not trans_list:
                    error_info = '{}基因没有对应的转录本信息'.format(gene_id)
                    raise Exception(error_info)
                else:
                    transcript = ",".join(trans_list)

                transcript_info = OrderedDict()
                for ind, trans_ll in enumerate(trans_list):
                    if not re.search('MSTRG', trans_ll) and not re.search(r'TCONS', trans_ll):
                        # 表示是已知转录本, 已知转录本输入的是cds和pep信息, 其键值索引是转录本的ensembl编号信息
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
                            transcript_info[trans_ll]["sequence"] = trans_sequence[trans_ll]
                            transcript_info[trans_ll]["length"] = len(trans_sequence[trans_ll])
                        else:
                            transcript_info[trans_ll]["length"] = 0
                            transcript_info[trans_ll]["sequence"] = '-'
                        if count_none == 2:
                            transcript_info.pop(trans_ll)
                    else:
                        # 新转录本输入的是其在new_trans_list中的编号信息
                        if trans_ll in trans_location_info.keys():
                            # 新转录本的start，end信息
                            tmp_value = trans_location_info[trans_ll]
                            transcript_info[str(ind)] = dict(start=tmp_value['start'],
                                                             end=tmp_value['end'])
                        else:
                            print('{} was not found in {}'.format(trans_ll, trans_location_path))
                        if trans_ll in trans_sequence.keys():
                            # 新转录本的sequence，length信息
                            transcript_info[str(ind)]['sequence'] = trans_sequence[trans_ll]
                            transcript_info[str(ind)]['length'] = len(trans_sequence[trans_ll])
                        else:
                            print('{} was not found in {}'.format(trans_ll, transcript_path))

                    if gene_id in biomart_data.keys():
                        # 获取已知基因的descriptio，gene_type信息
                        description = biomart_data[gene_id]["description"][0]
                        gene_type = biomart_data[gene_id]['gene_type'][0]
                        if len(line[3]) <= 1:
                            gene_name = biomart_data[gene_id]['gene_name'][0]
                        else:
                            gene_name = line[3]

                    if gene_id in gene_location_info.keys():
                        start = gene_location_info[gene_id]['start']
                        end = gene_location_info[gene_id]['end']
                        strand = gene_location_info[gene_id]['strand']
                        chrom = gene_location_info[gene_id]['chr']
                    else:
                        start, end, strand, chrom = '-', '-', '-', '-'

                    if gene_id in gene_sequence_dict.keys():
                        gene_sequence = gene_sequence_dict[gene_id]
                        gene_length = len(gene_sequence)
                    else:
                        gene_sequence = '-'
                        gene_length = 0
                        print('{} was not found in {}'.format(gene_id, gene_path))
                    data = [
                        ("is_new", is_new),
                        ("type", "gene_detail"),
                        ("class_code_id", class_code_id),
                        ("gene_id", gene_id),
                        ("entrez_id", entrez_id),
                        ("description", description),
                        ("strand", strand),
                        ("start", start),
                        ("location", "{}-{}".format(str(start), str(end))),
                        ("end", end),
                        ("chrom", chrom),
                        ("gene_name", gene_name),
                        ("transcript", transcript),
                        ("transcript_number", transcript_num),
                        ("gene_type", gene_type),
                        ("gene_sequence", gene_sequence),
                        ("gene_length", gene_length),
                        ("gene_ncbi", "https://www.ncbi.nlm.nih.gov/gquery/?term={}".format(gene_id)),
                        ("gene_ensembl", "http://www.ensembl.org/{}/Gene/Summary?g={}".format(species, gene_id)),
                    ]
                    if not transcript_info:
                        data.append(("trans_info", None))
                    else:
                        data.append(("trans_info", transcript_info))
                    # print data
                    data = SON(data)
                    data_list.append(data)

        print("共统计出{}基因".format(len(gene_id_list)))
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
            print("导入%s表成功！" % class_code)

    def add_class_code(self, assembly_method, name=None, major_gene_detail=False,
                       major_express_diff=False, species=None,
                       class_code_path=None, gene_location_path=None, trans_location_path=None,
                       biomart_path=None, biomart_type="type1", cds_path=None, pep_path=None,
                       transcript_path=None, gene_path=None, blast_id=None, test_this=False):
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
            ('name', name if name else 'Classcode_'+datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
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
            self.add_gene_detail_class_code_detail(class_code=class_code_path,
                                                   biomart_path=biomart_path,
                                                   biomart_type=biomart_type,
                                                   class_code_id=class_code_id,
                                                   gene_location_path=gene_location_path,
                                                   trans_location_path=trans_location_path,
                                                   cds_path=cds_path,
                                                   pep_path=pep_path,
                                                   transcript_path=transcript_path,
                                                   gene_path=gene_path,
                                                   species=species,
                                                   blast_id=blast_id)
        if major_express_diff:
            self.add_express_diff_class_code_detail(class_code_path, class_code_id)


class TestFunction(unittest.TestCase):
    class TmpRefrnaGeneDetail(RefrnaGeneDetail):
        def __init__(self):
            self._db_name = Config().MONGODB + '_ref_rna'
            self._db = None
            self._config = Config()

    def test(self):
        base_path = "/mnt/ilustre/users/sanger-dev/app/database/Genome_DB_finish/vertebrates" \
                    "/Mus_musculus/Ensemble_release_89/"
        biomart_path = base_path + "biomart/Mus_musculus.GRCm38.biomart_gene.txt"
        pep_path = base_path + "cds/Mus_musculus.GRCm38.pep.all.fa"
        cds_path = base_path + "cds/Mus_musculus.GRCm38.cds.all.fa"
        gene_bed = '/mnt/ilustre/users/sanger-dev/workspace/20170724/Single_gene_fa_5/GeneFa/ref_new_bed'
        trans_bed = '/mnt/ilustre/users/sanger-dev/workspace/20170724/Single_gene_fa_5/GeneFa/ref_new_trans_bed'
        gene_path = "/mnt/ilustre/users/sanger-dev/workspace/20170724/Single_gene_fa_2/GeneFa/output/gene.fa"
        transcript_path = "/mnt/ilustre/users/sanger-dev/workspace/20170706/" \
                          "Single_rsem_stringtie_mouse_total_2/Express1/TranscriptAbstract/output/exons.fa"
        class_code = "/mnt/ilustre/users/sanger-dev/workspace/20170702/" \
                     "Single_rsem_stringtie_mouse_total_1/Express/MergeRsem/class_code"

        a = self.TmpRefrnaGeneDetail()
        blast_id = a.db["sg_annotation_blast"].find_one({"task_id": "demo_0711"})
        blast_id = blast_id['_id']
        a.add_class_code(assembly_method="stringtie", name=None, major_gene_detail=True, major_express_diff=True,
                         biomart_path=biomart_path, gene_location_path=gene_bed, trans_location_path=trans_bed,
                         class_code_path=class_code,  cds_path=cds_path, pep_path=pep_path,
                         species='Mus_musculus', transcript_path=transcript_path, gene_path=gene_path, blast_id=blast_id,
                         test_this=True)

if __name__ == '__main__':
    unittest.main()
