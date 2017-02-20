#!/usr/bin/python
# -*- coding: utf-8 -*-
def set_strand(method, strand_specific = False, strand_dir = None):
    """对需要输入链特异性信息的软件(stringtie,featurecounts,htseq)输出适当的参数"""
    stringtie_strand_dir = {'forward':'firststranded',"reverse":"secondstranded"}
    featurecounts_strand_dir = {"forward":1,"reverse":2}
    htseq_strand_dir = {"forward": "yes","reverse":"reverse"}
    director = {"stringtie":stringtie_strand_dir,"featurecounts":featurecounts_strand_dir,"htseq":htseq_strand_dir}
    strand = {"stringtie":"fr-unstranded","featurecounts":0,"htseq":"no"}
    method = method.lower()
    if strand_specific:
        if method in director.keys():
            if strand_dir in director[method].keys():
                return strand_specific, director[method][strand_dir]
    else:
        if method in strand.keys():
            return strand[method], strand_dir
                

    
    
