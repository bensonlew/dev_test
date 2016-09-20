#!/usr/bin/python
# -*- coding: utf-8 -*-
# __author__ = "JieYao"

import os
import string
import argparse
import shutil
import json
import time
import subprocess

usage = "该程序为框架下Tool的一键化测试工具，\n支持连续测试多组数据。\n会提取对应的测试结果文件放到指定目录。\n"
usage += "用法样例 python test_tool_bat.py -c xxx.py -i test -type meta.beta_diversity.xxx -p para.txt -o ./ \n"
parser = argparse.ArgumentParser(description = usage)
parser.add_argument("-c", "--code_file", help="源码文件路径", required = True)
parser.add_argument("-i", "--id", help="用户ID", required = True)
parser.add_argument("-type", "--type", help="工具所属流程和模块，格式如:\n\"meta.beta_diversity.pca\"，\n\"meta.otunetwork\"", required = True)
parser.add_argument("-p", "--para_file", help="参数文件路径,文件中每行代表一次测试，各个参数用tab隔开，以name:value的形式写,不加引号", required = True)
parser.add_argument("-o", "--output_dir", help="重设输出文件路径", required = False, default="./")
args = vars(parser.parse_args())


args['code_file'] = os.path.abspath(args['code_file'])
args['para_file'] = os.path.abspath(args['para_file'])
args['output_dir'] = os.path.abspath(args['output_dir'])

try:
    shutil.copyfile(args['code_file'], "/mnt/ilustre/users/sanger-dev/biocluster/src/mbio/tools/" + args['type'].replace(".", "/") + ".py")
except:
    print  "代码文件copy到%s下时出现错误，请检查type参数" % "/mnt/ilustre/users/sanger-dev/biocluster/src/mbio/tools/" + args['type'].replace(".", "/") + ".py"
    exit(0)

if args['output_dir'] and not os.path.exists(args['output_dir']):
    os,mkdir(args['output_dir'])

para_data = []
for s in open(args['para_file']).readlines():
    s = s.strip().split()
    if not s:
        continue
    tmp_dict = dict()
    for data in s:
        position = data.find(":")
        tmp_dict[data[:position]] = data[position+1 : len(data)]
    para_data += [tmp_dict]

option_type = dict()
with open(args['code_file'], "r") as tmp_file:
    for text in tmp_file.readlines():
        if "name" in text and "type" in text:
            text = text.strip()
            if text[-1] != "}":
                text = text[0:-1]
            try:
                data = json.loads(text)
                if "type" in data.keys() and "name" in data.keys():
                    option_type[data["name"]] = data["type"]
            except:
                continue

std_option = dict()

file_name = os.path.split(args['code_file'])[1]
json_name = "single_" + os.path.splitext(file_name)[0] + ".json"
py_name = args["output_dir"] + '/test_single_' + file_name
date = time.strftime('%Y%m%d',time.localtime(time.time()))
log_file = "/mnt/ilustre/users/sanger-dev/workspace/%s/Single_%s" %(date, args['id'])

with open(py_name, "w") as tmp_file:
    tmp_file.write("#!/mnt/ilustre/users/sanger-dev/app/program/Python/bin/python\n")
    tmp_file.write("from mbio.workflows.single import SingleWorkflow\n")
    tmp_file.write("from biocluster.wsheet import Sheet\n")
    tmp_file.write("wsheet = Sheet(\"%s\")\n" % json_name)
    tmp_file.write("wf = SingleWorkflow(wsheet)\n")
    tmp_file.write("wf.run()\n")

for i in range(len(para_data)):
    for keys in para_data[i].keys():
        if keys in option_type.keys():
            if option_type[keys] == "bool":
                para_data[i][keys] = bool(para_data[i][keys])
            elif option_type[keys] == "int":
                para_data[i][keys] = int(para_data[i][keys])
            elif option_type[keys] == "float":
                para_data[i][keys] = float(para_data[i][keys])
            else:
                continue
    std_option['type'] = "tool"
    std_option['name'] = args['type']
    std_option['options'] = para_data[i]
    std_option['id'] = args['id']

    with open(json_name, "w") as tmp_file:
        tmp_file.write(json.dumps(std_option, indent=4))

    task_ID = ""
    print "开始第%d组参数的测试。" %(i+1)
    try:
        os.system("python %s > /dev/null" % py_name)
    except:
        print "第%d组参数测试运行失败" %(i+1)
        continue
    for text in open(log_file + "/log.txt", "r").readlines():
        if "ID" not in text:
            continue
        text = text.strip()[text.find("ID")+3:]
        while len(text)>0 and text[0] not in "1234567890":
            text = text[1:]
        task_ID = text[:5]
    group_name = args['output_dir'] + "/Option_Group_%d_" %(i+1)
    if os.path.exists(group_name + task_ID):
        shutil.rmtree(group_name + task_ID)
    os.mkdir(group_name + task_ID)
    calc_path = log_file + "/" + os.path.splitext(file_name)[0].capitalize()
    for tmp_name in os.listdir(calc_path):
        files = os.path.join(calc_path, tmp_name)
        if os.path.isdir(files):
            os.system("cp -r %s %s" %(files, group_name + task_ID))
        if task_ID in files:
            os.system("cp %s %s" %(files, group_name + task_ID))
        if "sbatch" in files:
            os.system("cp %s %s" %(files, group_name + task_ID))
    shutil.move(json_name, group_name + task_ID)
    print "第%d组参数测试运行完成,测试信息与文件目录：%s" %(i+1, group_name + task_ID)


# Sample : python test_tool.bat.py -c roc.py -i JieYao -type meta.beta_diversity.roc -p para.txt -o ./
