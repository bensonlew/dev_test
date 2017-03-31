# -*- coding: utf-8 -*-
# __author__ = fiona
# time: 2017/3/10 18:07

import re
import subprocess
from biocluster.iofile import *
import regex
from anytree import Node, RenderTree

''' https://github.com/The-Sequence-Ontology/SO-Ontologies/blob/master/releases/so-xp.owl/so.obo'''


class Node(object):
    def __init__(self):
        self._parent = Node()
        self._children = set()
        self._value = ''
        self._ancestors = []
        self._descendants = []
        
        
    
    def is_root(self):
        return self._parent is None
    
    def is_leaf(self):
        return len(self._children) == 0
    def children(self):
        return self._children
    def add_child(self):
        
        pass
    def del_child(self):
        pass
    def update_parent(self):
        pass
    
    
    


class TermTree(object):
    def __init__(self):
        self._root = Node()
    
    def set_root(self, value):
        root = Node()
        root.value = value
        self._root = root
    
    def add_son_nodes(self, target_node, node_value_set):
        # son_nodes_set = set()
        # son_nodes_id_set = set()
        # son_nodes_name_set =set()
        son_nodes =
    
    def walk(self):
        pass


class SequenceOntologyFile(File):
    def __init__(self):
        # self._path = os.path.join(os.getcwd(), 'temp_sequence_ontology.obo')
        # self._download(site)
        self._items_info = dict()
        self._items = set()
        self._properties = {}
        self.parse()
        # self._update_date = None
    
    def check(self):
        super(SequenceOntologyFile, self).check()
    
    def _download(self, url):
        subprocess.call('wget -c -O {} {}'.format(self._path, url), shell=True)
    
    def parse(self):
        
        # so_soup = BeautifulSoup(open(self._path).read())
        # records = so_soup.find_all('td', text=re.compile(r'^(?:\[Term\]|name:|id:)'),
        #                            attrs={'class': 'blob-code blob-code-inner js-file-line'})
        # field_pattern = re.compile(r'<td.+>\[Term\]</td>')
        # cut_indexes = self.__get_ele_indexes(records, field_pattern)
        # self.__get_split_lst_by_given_indexes(records, cut_indexes)
        self._items = re.split(r'\[Term\]', open(self.path).read())
        self._items = self._items[1:len(self._items)]
        temp = re.split(r'\n+\[[^:\s]+?\]\n+', self._items[len(self._items) - 1])
        self._items.append(temp[0])
        # for item in self._items:
        #     item_m = regex.search(r'id:(.+?)\n(.+\n)+(is_a:\s+(SO:0000110).*!.+)\n', item.strip() + "\n")
        #     if item_m:
        #         pass
    
    def get_son_for_term(self, **kwargs):
        
        target_so_id = ''
        target_term_name = ''
        if 'so_id' in kwargs.keys():
            target_so_id = kwargs['so_id']
        if 'target_term_name' in kwargs.keys():
            target_term_name = kwargs['target_term_name']
        descendants_term_set = set()
        descendants_term_id_set = set()
        descendants_term_name_set = set()
        
        if target_so_id:
            for id_item in self._items:
                id_item_m = regex.search(
                    r'id:\s+(\S+)\nname:\s+(\S+)\n.+is_a:\s+' + str(target_so_id) + '\s+!\s+(\S+)\s+',
                    id_item.strip())
                if id_item_m:
                    son_term_id = id_item_m.captures(1)[0].strip()
                    son_term_name = id_item_m.captures(2)[0].strip()
                    # parent_id = item_m.captures(5)[0].strip()
                    # target_so_name = id_item_m.captures(6)[0].strip()
                    descendants_term_set.add((son_term_id, son_term_name))
                    descendants_term_id_set.add(son_term_id)
                    descendants_term_name_set.add(son_term_name)
            
            return descendants_term_set, descendants_term_id_set, descendants_term_name_set
        if target_term_name:
            for name_item in self._items:
                name_item_m = regex.search(
                    r'id:\s+(\S+)\s*\nname:\s+(\S+)\s*(.+?\n)*is_a:\s+(SO:\S+)\s+!\s+' + target_term_name,
                    name_item.strip())
                if name_item_m:
                    son_term_id = name_item_m.captures(1)[0].strip()
                    son_term_name = name_item_m.captures(2)[0].strip()
                    descendants_term_set.add((son_term_id, son_term_name))
                    descendants_term_id_set.add(son_term_id)
                    descendants_term_name_set.add(son_term_name)
            
            return descendants_term_set, descendants_term_id_set, descendants_term_name_set
        
        pass
    
    def get_descendants_for_term(self, **kwargs):
        
        target_so_id = ''
        target_term_name = ''
        if 'so_id' in kwargs.keys():
            target_so_id = kwargs['so_id']
            term_tree = TermTree()
            term_tree.set_root(target_so_id)
        if 'target_term_name' in kwargs.keys():
            target_term_name = kwargs['target_term_name']
            term_tree = TermTree()
            term_tree.set_root(target_term_name)
            # descendants_term_set = set()
            # descendants_term_id_set = set()
            # descendants_term_name_set = set()


class TermNode(Node):
    def __init__(self):
        super(TermNode, self).__init__()


if __name__ == '__main__':
    so_file_path = 'F:\\temp\\SO-Ontologies-master\\so.obo'
    so_file = SequenceOntologyFile()
    so_file.set_path(so_file_path)
    off_spring_terms, offspring_ids, offspring_names = so_file.get_descendants_for_term(
        target_term_name='sequence_feature')
    print('\n'.join(offspring_names))
