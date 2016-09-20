# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
from biocluster.iofile import File
# import os
from biocluster.core.exceptions import FileError
from Bio.Blast import NCBIXML



class BlastXmlFile(File):
    """
    定义blast+比对输出类型为5结果文件的xml格式， 测试blast+为2.3.0版本
    """

    def __init__(self):
        super(BlastXmlFile, self).__init__()

    def get_info(self):
        """
        获取文件属性
        """
        super(BlastXmlFile, self).get_info()
        blast_info = self.get_xml_info()
        self.set_property('query_num', blast_info[0])
        self.set_property('hit_num', blast_info[1])
        self.set_property('hsp_num', blast_info[2])

    def get_xml_info(self):
        """
        获取blast结果xml的信息

        :return
        """
        blastxml = NCBIXML.parse(open(self.path))
        try:
            query_count = 0
            align_count = 0
            hsp_count = 0
            for query in blastxml:
                query_count += 1
                for align in query.alignments:
                    align_count += 1
                    for hsp in align.hsps:
                        hsp_count += 1
        except ValueError:
            raise FileError('传入文件类型不正确，无法解析，请检查文件是否正确，或者生成文件的blast版本不正确，本程序测试版本blast+2.3.0')
        except Exception as e:
            raise FileError('未知原因导致blastxml文件解析失败:{}'.format(e))
        return query_count, align_count, hsp_count

    def check(self):
        """
        检测文件是否满足要求，发生错误时应该触发FileError异常
        """
        if super(BlastXmlFile, self).check():
            # 父类check方法检查文件路径是否设置，文件是否存在，文件是否为空
            blastxml = NCBIXML.parse(open(self.path))
            try:
                blastxml.next()
            except ValueError:
                raise FileError('传入文件类型不正确，无法解析，请检查文件是否正确，或者生成文件的blast版本不正确，本程序测试版本blast+2.3.0')
            except Exception as e:
                raise FileError('未知原因导致blastxml文件解析失败:{}'.format(e))
            return True

    def convert2table(self, outfile):
        """调用packages中的xml2table方法来转换到table格式"""
        from mbio.packages.align.blast.xml2table import xml2table
        xml2table(self.path, outfile)

    def sub_xml(self, query_list):
        pass

    def change_blast_version(self, fp, version='2.2.25+'):
        """
        特殊为blast2go准备，程序特定不能做当前2.3.0+版本的blast+
        """
        with open(self.path) as f, open(fp, 'w') as w:
            for line in f:
                l_str = line.strip()
                if l_str.startswith('<BlastOutput_version>'):
                    if '2.3.0+' not in line:
                        raise Exception('blast 程序版本不是2.3.0版本，此处报错不是必须用2.3.0版本，而是b2gPipe程序需要版本为2.2.25，此处特殊改为2.2.25，依然可以进行blast2go，\
                            但是不代表后续版本仍然可以使用，既然blast版本修改，请重新检查')
                    line = line.replace('2.3.0+', version)
                    w.write(line)
                    break
                else:
                    w.write(line)
            for line in f:
                w.write(line)
        return fp


if __name__ == '__main__':  # for test
    a = BlastXmlFile()
    # a.set_path('C:\\Users\\sheng.he.MAJORBIO\\Desktop\\annotation\\annotation\\NR\\transcript.fa_vs_nr.blasttable.xls')
    a.set_path("C:\\Users\\sheng.he.MAJORBIO\\Desktop\\blast_result\\Trinity_vs_nr.xml")
    a.check()
    a.get_info()
    # a.convert2table('C:\\Users\\sheng.he.MAJORBIO\\Desktop\\test.xls')
    a.change_blast_version("C:\\Users\\sheng.he.MAJORBIO\\Desktop\\blast_result\\Trinity_vs_nr.xml_new")
