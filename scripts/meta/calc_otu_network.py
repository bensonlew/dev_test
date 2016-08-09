# -*- coding: utf-8 -*-
# __author__ = 'JieYao'
import os
import argparse
from biocluster.config import Config
import shutil
import networkx

def make_env_table(inFile, outFile):
    with open(inFile, "r") as tmp_file:
        samples_name = tmp_file.readline().rstrip().split('\t')
    with open('group.txt' , "w") as tmp_file:
        tmp_file.write("#sample\tgroup\n")
        for i in range(1,len(samples_name)):
            tmp_file.write(samples_name[i]+"\tSTD\n") 
    return './group.txt'

parser = argparse.ArgumentParser(description='输出OTU表格，生成OTU网络信息')
parser.add_argument('-i', "--otu_matrix", help="输入的OTU表", required = True)
parser.add_argument('-o', "--output", help="输出文件位置", required = True)
parser.add_argument('-m', "--env_table", help="样本分类表", required = False)
args = vars(parser.parse_args())

flag = False
inFile = args["otu_matrix"]
outFile = args["output"]
if not args["env_table"]:
    env_table = make_env_table(inFile, outFile)
    flag = True
else:
    env_table = args["env_table"]
if os.path.exists(outFile):
    shutil.rmtree(outFile)
    
"""
执行make_otu_network.py 计算otu网络的相关信息并生成文件
完成后由于make_otu_network.py生成的是一个文件夹，使用os和shutil的命令将文件全部移动到输出路径下
"""
command = Config().SOFTWARE_DIR + '/program/Python/bin/python '
command += Config().SOFTWARE_DIR + '/program/Python/bin/make_otu_network.py'
command += ' -i %s -o %s -m %s' %(inFile, outFile, env_table)
os.system(command)
if flag:
    os.remove("./group.txt")
for paths,d,filelist in os.walk(outFile):
    for filename in filelist:
        name = os.path.join(paths, filename)
        if "reduced" in name:
            os.remove(name)
        elif "/otu_network/" in name:
            shutil.move(name, outFile)
shutil.rmtree(outFile + '/otu_network')
for paths,d,filelist in os.walk(outFile):
    for filename in filelist:
        name = os.path.join(paths, filename)
        if "props" in name:
            os.remove(name)

"""
根据node表建立{节点名字---节点编号}的字典
"""
node_name = [""]
node_dictionary = {}
with open(outFile + '/real_node_table.txt', "r") as node_file:
    informations = node_file.readlines()
    for i in range(1, len(informations)):
        tmp = informations[i].rstrip().split("\t")
        node_dictionary[tmp[0]] = i
        node_name += [tmp[0]]
"""
开始使用Networkx包建立图
计算OTU网络的属性信息
"""
G = networkx.Graph()
with open(outFile + "/real_edge_table.txt" , "r") as edge_file:
    informations = edge_file.readlines()
    for i in range(1, len(informations)):
        tmp = informations[i].rstrip().split("\t")
        G.add_edge(node_dictionary[tmp[0]], node_dictionary[tmp[1]], weight = eval(tmp[2]))


"""
用实践测试单独对Sample或者是OTU构图的构图方法，
结果证明这样的构图出来的结果基本上Sample是完全图，
OTU单独构图的意义则不大，所以这种想法……失败了。
"""
"""
H = networkx.Graph()
with open(outFile + "/real_node_table.txt" , "r") as node_file:
    informations = node_file.readlines()
    for i in range(1,len(informations)):
        tmp = informations[i].rstrip().split("\t")
        if tmp[2] == "otu_node":
            break
position = i
for i in range(position):
    for j in range(position):
        H.add_edge(i,j,weight=0)
        for k in range(position,len(G)):
            if G.get_edge_data(i,k) and G.get_edge_data(j,k):
                H.edge[i][j]['weight'] += 1
        if H.edge[i][j]['weight'] == 0:
            H.remove_edge(i,j)
minx = 32767
for i in range(position):
    for j in range(position):
        if (i in H)and(j in H)and(H.get_edge_data(i,j)):
            minx = min(minx, H.edge[i][j]['weight'])

for i in range(position):
    for j in range(position):
        if (i in H)and(j in H)and(H.get_edge_data(i,j)):
            H.edge[i][j]['weight'] -= minx
            if H.edge[i][j]['weight'] <=0:
                H.remove_edge(i,j)
print H.edges()
"""
"""3计算属性表，分本3"""

#节点度中心系数，表示节点在图中的重要性
Degree_Centrality = networkx.degree_centrality(G)
#节点距离中心系数，值越大表示到其他节点距离越近，中心性越高
Closeness_Centrality = networkx.closeness_centrality(G)
#节点介数中心系数，值越大表示通过该节点的最短路径越多，中心性越高
Betweenness_Centrality = networkx.betweenness_centrality(G)
with open(os.path.join(args["output"], "network_centrality.txt"), "w") as tmp_file:
        tmp_file.write("Node_ID\tNode_Name\tDegree_Centrality\t")
        tmp_file.write("Closeness_Centrality\tBetweenness_Centrality\n")
        for i in range(1, len(G)+1):
            tmp_file.write(str(i)+"\t"+node_name[i]+"\t")
            tmp_file.write(str(Degree_Centrality[i])+"\t")
            tmp_file.write(str(Closeness_Centrality[i])+"\t")
            tmp_file.write(str(Betweenness_Centrality[i])+"\n")


#网络传递性，二分图中应该为0，否则有问题
Transitivity = networkx.transitivity(G)
#网络直径
Diameter = networkx.diameter(G)
#网络平均最短路长度
Average_shortest_path = networkx.average_shortest_path_length(G)
with open(os.path.join(args["output"], "network_attributes.txt"), "w") as tmp_file:
    tmp_file.write("Transitivity:"+str(Transitivity)+"\n")
    tmp_file.write("Diameter:"+str(Diameter)+"\n")
    tmp_file.write("Average_shortest_path_length:"+str(Average_shortest_path)+"\n")

