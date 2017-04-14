# -*- coding: utf-8 -*-
# __author__ = 'sheng.he'
# last_modified:20160523
# 脚本的主要代码来自qiime/assign_taxonomy.py以及其使用到的相关脚本

_QIIME_RDP_TAXON_TAG = "_qiime_unique_taxon_tag_"
_QIIME_RDP_ESCAPES = [("&", "_qiime_ampersand_escape_"),
                      (">", "_qiime_greaterthan_escape_"),
                      ("<", "_qiime_lessthan_escape_"),
                      ]


import argparse
import os
import re
import subprocess
from string import strip
from itertools import count
from cStringIO import StringIO
from biocluster.config import Config
from cogent.parse.fasta import MinimalFastaParser


Classifier_properties = '# Sample ResourceBundle properties file\nbergeyTree=bergeyTrainingTree.xml\n\
probabilityList=genus_wordConditionalProbList.txt\n\
probabilityIndex=wordConditionalProbIndexArr.txt\n\
wordPrior=logWordPrior.txt\n\
classifierVersion=RDP Naive Bayesian rRNA Classifier Version 2.5, May 2012\n'


def get_argus():
    parse = argparse.ArgumentParser(
        description='''RDP训练数据库文件脚本''',
        prog='train_taxon_by_RDP.py')
    parse.add_argument('-t', '--taxonomy_file', required=True, help='需要训练的分类库文件 (necessary)')
    parse.add_argument('-s', '--sequence_fasta', required=True, help='分类文件的fasta序列 (necessary)')
    parse.add_argument('-o', '--output_dir', required=True, help='输出文件夹 (necessary)')
    parse.add_argument('-m', '--max_memory', default=50, type=int, help='最大内存:数字(单位为g)')
    parse.add_argument('-k', '--keep_tempfile', action='store_true', help='保留中间文件')
    args = parse.parse_args()
    if not os.path.isfile(args.taxonomy_file) or not os.path.isfile(args.sequence_fasta):
        raise IOError('{} 文件不存在或者不是文件'.format(args.taxonomy_file))
    if not os.path.isdir(args.output_dir):
        os.makedirs(args.output_dir)
        print('INFO:输出目录不存在，已创建目录')
    return args.taxonomy_file, args.sequence_fasta, args.output_dir.rstrip('/'), args.max_memory, args.keep_tempfile


class RdpTree(object):
    """Simple, specialized tree class used to generate a taxonomy
    file for the Rdp Classifier.
    """
    taxonomic_ranks = ' abcdefghijklmnopqrstuvwxyz'

    def __init__(self, name='Root', parent=None, counter=None):
        if counter is None:
            self.counter = count(0)
        else:
            self.counter = counter
        self.id = self.counter.next()
        self.name = name
        self.parent = parent
        self.seq_ids = []
        if parent is None:
            self.depth = 0
        else:
            self.depth = parent.depth + 1
        self.children = dict()  # name => subtree

    def insert_lineage(self, lineage):
        """Inserts an assignment into the taxonomic tree.

        Lineage must support the iterator interface, or provide an
        __iter__() method that returns an iterator.
        """
        lineage = iter(lineage)
        try:
            taxon = lineage.next()
            if taxon not in self.children:
                self.children[taxon] = self.__class__(
                    name=taxon, parent=self, counter=self.counter)
            retval = self.children[taxon].insert_lineage(lineage)
        except StopIteration:
            retval = self
        return retval

    def get_lineage(self):
        if self.parent is not None:
            return self.parent.get_lineage() + [self.name]
        else:
            return [self.name]

    def get_nodes(self):
        yield self
        for child in self.children.values():
            child_nodes = child.get_nodes()
            for node in child_nodes:
                yield node

    def dereplicate_taxa(self):
        # We check that there are no duplicate taxon names (case insensitive)
        # at a given depth. We must do a case insensitive check because the RDP
        # classifier converts taxon names to lowercase when it checks for
        # duplicates, and will throw an error otherwise.
        taxa_by_depth = {}
        for node in self.get_nodes():
            name = node.name
            depth = node.depth
            current_names = taxa_by_depth.get(depth, set())
            if name.lower() in current_names:
                node.name = name + _QIIME_RDP_TAXON_TAG + str(node.id)
            else:
                current_names.add(name.lower())
                taxa_by_depth[depth] = current_names

    def get_rdp_taxonomy(self):
        """Returns a string, in Rdp-compatible format.
        """
        # RDP uses 0 for the parent ID of the root node
        if self.parent is None:
            parent_id = 0
        else:
            parent_id = self.parent.id

        # top rank name must be norank, and bottom rank must be genus
        if self.depth == 0:
            rank_name = "norank"
        elif self.children:
            rank_name = self.taxonomic_ranks[self.depth]
        else:
            rank_name = "genus"

        fields = [
            self.id, self.name, parent_id, self.depth, rank_name]
        taxonomy_str = '*'.join(map(str, fields)) + "\n"

        # Recursively append lines from sorted list of subtrees
        child_names = self.children.keys()
        child_names.sort()
        subtrees = [self.children[name] for name in child_names]
        for subtree in subtrees:
            taxonomy_str += subtree.get_rdp_taxonomy()
        return taxonomy_str


class RdpTrainingSet(object):
    def __init__(self):
        self._tree = RdpTree()
        self.sequences = {}
        self.sequence_nodes = {}
        self.lineage_depth = None

    def add_sequence(self, seq_id, seq):
        self.sequences[seq_id] = seq

    def add_lineage(self, seq_id, lineage_str):
        for char, escape_str in _QIIME_RDP_ESCAPES:
            lineage_str = re.sub(char, escape_str, lineage_str)
        lineage = self._parse_lineage(lineage_str)
        seq_node = self._tree.insert_lineage(lineage)
        self.sequence_nodes[seq_id] = seq_node

    def dereplicate_taxa(self):
        return self._tree.dereplicate_taxa()

    def _parse_lineage(self, lineage_str):
        """Returns a list of taxa from the semi-colon-separated
        lineage string of an id_to_taxonomy file.
        """
        lineage = lineage_str.strip().split(';')
        if self.lineage_depth is None:
            self.lineage_depth = len(lineage)
        if len(lineage) != self.lineage_depth:
            raise ValueError(
                'Because the RDP Classifier operates in a bottom-up manner, '
                'each taxonomy assignment in the id-to-taxonomy file must have '
                'the same number of ranks.  Detected %s ranks in the first '
                'item of the file, but detected %s ranks later in the file. '
                'Offending taxonomy string: %s' %
                (self.lineage_depth, len(lineage), lineage_str))
        return lineage

    def get_training_seqs(self):
        """Returns an iterator of valid training sequences in
        RDP-compatible format

        Each training sequence is represented by a tuple (rdp_id,
        seq).  The rdp_id consists of two items: the original sequence
        ID with whitespace replaced by underscores, and the lineage
        with taxa separated by semicolons.
        """
        # Rdp requires unique sequence IDs without whitespace.  Can't
        # trust user IDs to not have whitespace, so we replace all
        # whitespace with an underscore.  Classification may fail if
        # the replacement method generates a name collision.
        for seq_id, node in self.sequence_nodes.iteritems():
            seq = self.sequences.get(seq_id)
            if seq is not None:
                lineage = node.get_lineage()
                rdp_id = '%s %s' % (re.sub('\s', '_', seq_id), ';'.join(lineage))
                yield rdp_id, seq

    def get_rdp_taxonomy(self):
        return self._tree.get_rdp_taxonomy()

    @staticmethod
    def fix_output_file(result_path):
        # Ultimate hack to replace mangled taxa names
        temp_results = StringIO()
        for line in open(result_path):
            line = re.sub(
                _QIIME_RDP_TAXON_TAG + "[^;\n\t]*", '', line)
            for char, escape_str in _QIIME_RDP_ESCAPES:
                line = re.sub(escape_str, char, line)
            temp_results.write(line)
        open(result_path, 'w').write(temp_results.getvalue())

    @staticmethod
    def fix_results(results_dict):
        for seq_id, assignment in results_dict.iteritems():
            lineage, confidence = assignment
            lineage = re.sub(
                _QIIME_RDP_TAXON_TAG + "[^;\n\t]*", '', lineage)
            for char, escape_str in _QIIME_RDP_ESCAPES:
                lineage = re.sub(escape_str, char, lineage)
            results_dict[seq_id] = (lineage, confidence)
        return results_dict


def format_tax(reference_sequences_fp, id_to_taxonomy_fp, outdir):
    training_set = RdpTrainingSet()
    reference_seqs_file = open(reference_sequences_fp, 'U')
    id_to_taxonomy_file = open(id_to_taxonomy_fp, 'U')

    for seq_id, seq in MinimalFastaParser(reference_seqs_file):
        training_set.add_sequence(seq_id, seq)

    for line in id_to_taxonomy_file:
        seq_id, lineage_str = map(strip, line.split('\t'))
        training_set.add_lineage(seq_id, lineage_str)

    training_set.dereplicate_taxa()

    rdp_taxonomy_fp = '{}/RdpTaxonAssigner_taxonomy.txt'.format(outdir.rstrip('/'))
    rdp_taxonomy_file = open(rdp_taxonomy_fp, 'w')
    rdp_taxonomy_file.write(training_set.get_rdp_taxonomy())

    rdp_training_seqs_fp = '{}/RdpTaxonAssigner_training_seqs.fasta'.format(outdir.rstrip('/'))
    rdp_training_seqs_file = open(rdp_training_seqs_fp, 'w')
    for rdp_id, seq in training_set.get_training_seqs():
        rdp_training_seqs_file.write('>%s\n%s\n' % (rdp_id, seq))
    rdp_taxonomy_file.close()
    rdp_training_seqs_file.close()
    return rdp_taxonomy_fp, rdp_training_seqs_fp


def run_RDP_train(seq_with_tax, tax_file, outdir, max_memory):
    """"""
    if 'RDP_JAR_PATH' in os.environ:
        RDP_fp = os.getenv('RDP_JAR_PATH')
    else:
        RDP_fp = '{}/dist/classifier.jar'.format(os.path.dirname(os.path.realpath(__file__)))
        if not os.path.exists(RDP_fp):
            raise Exception('RDP文件的默认路径不存在，请在环境变量中添加RDP_JAR_PATH')
    max_memory = str(max_memory) + 'g'
    java_path = Config().SOFTWARE_DIR + '/program/sun_jdk1.8.0/bin/java'
    cmd = java_path + ' -Xmx{} -jar {} train -o {} -s {} -t {}'.format(max_memory, RDP_fp,
                                                                       outdir, seq_with_tax, tax_file)
    print cmd
    subprocess.check_output(cmd, shell=True)


def main():
    origin_tax, origin_seq, outdir, max_memory, keep_temp = get_argus()
    tax_file, seq_file = format_tax(origin_seq, origin_tax, outdir)
    run_RDP_train(seq_file, tax_file, outdir, max_memory)
    if not keep_temp:
        os.remove(tax_file)
        os.remove(seq_file)
    open('{}/Classifier.properties'.format(outdir), 'w').write(Classifier_properties)


if __name__ == '__main__':
    main()
