# -*- coding: utf-8 -*-
# __author__ = 'JieYao'

import os
import argparse
from biocluster.config import Config
import shutil
import networkx

global name_list
name_list = ["0"]

def search(node_name):
    global name_list
    for i in range(len(name_list)):
        if node_name == name_list[i]:
            return i
    name_list += [node_name]
    return len(name_list)-1
         

parser = argparse.ArgumentParser(description='输入蛋白质相互作用网络，输出网络信息')
parser.add_argument('-i', "--PPI_network", help="输入的PPI网络", required = True)
parser.add_argument('-c', "--cut", help='蛋白相互作用阈值', required = False)
parser.add_argument('-o', "--output", help = "输出文件输出路径", required = True)
#parser.add_argument('-top', "--top", help = "First k important interaction in graph", required = False)
args = vars(parser.parse_args())

inFile = args["PPI_network"]
outFile = args["output"]
if not os.path.exists(outFile):
    os.mkdir(outFile)
if not args["cut"]:
    cut = -1
else:
    cut = eval(args["cut"])
    
G = networkx.Graph()
name_list = ["0"]
with open(inFile, "r") as tmp_file:
    data = tmp_file.readlines()
for i in range(1,len(data)):
    s = data[i].rstrip().split("\t")
    if eval(s[2]) >= cut:
        G.add_edge(search(s[0]), search(s[1]), weight = eval(s[2]))
      
Transitivity = networkx.transitivity(G)
Clustering = networkx.clustering(G)
Degree_distribution = networkx.degree_histogram(G)
Degree_Centrality = networkx.degree_centrality(G)
Closeness_Centrality = networkx.closeness_centrality(G)
Betweenness_Centrality = networkx.betweenness_centrality(G)
with open(os.path.join(args["output"], "protein_interaction_network_degree_distribution.txt"), "w") as tmp_file:
    tmp_file.write("Degree\tNode_Num\n")
    for i in range(len(Degree_distribution)):
        tmp_file.write(str(i)+"\t"+str(Degree_distribution[i])+"\n")
with open(os.path.join(args["output"], "protein_interaction_network_by_cut.txt"), "w") as tmp_file:
    tmp_file.write("Node_Num = " + str(len(G.nodes())) + "\n")
    tmp_file.write("Edge_Num = " + str(len(G.edges())) + "\n")
    tmp_file.write("Node1_Name\tNode2_Name\tWeight\n")
    for i in G.edges():
        tmp_file.write(name_list[i[0]]+"\t"+name_list[i[1]]+"\t"+str(G[i[0]][i[1]]["weight"])+"\n")
with open(os.path.join(args["output"], "protein_interaction_network_node_degree.txt"), "w") as tmp_file:
    tmp_file.write("Node_ID\tNode_Name\tDegree\n")
    for i in range(1,len(G)+1):
        tmp_file.write(str(i)+"\t"+name_list[i]+"\t")
        tmp_file.write(str(G.degree(i))+"\n")
with open(os.path.join(args["output"], "protein_interaction_network_centrality.txt"), "w") as tmp_file:
    tmp_file.write("Node_ID\tNode_Name\tDegree_Centrality\t")
    tmp_file.write("Closeness_Centrality\tBetweenness_Centrality\n")
    for i in range(1,len(G)+1):
        tmp_file.write(str(i)+"\t"+name_list[i]+"\t")
        tmp_file.write(str(Degree_Centrality[i])+"\t")
        tmp_file.write(str(Closeness_Centrality[i])+"\t")
        tmp_file.write(str(Betweenness_Centrality[i])+"\n")
        
with open(os.path.join(args["output"], "protein_interaction_network_clustering.txt"), "w") as tmp_file:
    tmp_file.write("Node_ID\tProtein_Name\tClustering\n")
    for i in range(1,len(G)+1):
        tmp_file.write(str(i)+"\t"+name_list[i]+"\t"+str(Clustering[i])+"\n")

with open(os.path.join(args["output"], "protein_interaction_network_transitivity.txt"), "w") as tmp_file:
    tmp_file.write("Transitivity\t")
    tmp_file.write(str(Transitivity)+"\n")

