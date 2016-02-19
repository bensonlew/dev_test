# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'
import itertools
import os
import shutil


def group_file_spilt(group_file, output_dir):
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    os.mkdir(output_dir)
    with open(group_file, 'rb') as gf:
        group_detail = {}
        first_line = gf.readline()
        while True:
            line = gf.readline().strip('\n')
            if not line:
                break
            line = line.split('\t')
            group_detail[line[0]] = line[1]
        # print(group_detail)
        group_name = {}.fromkeys(group_detail.values()).keys()
        two_group_name = list(itertools.combinations(group_name, 2))
        print(two_group_name)
        n = 1
        for two in two_group_name:
            with open('two_group_file{}'.format(n), 'wb') as rf:
                rf.write(first_line)
                # print(two)
                for detail in group_detail:
                    for one in two:
                        if group_detail[detail] == one:
                            # print(detail)
                            line = '{}\t{}'.format(detail, one)
                            # print(line)
                            rf.write('{}\n'.format(line))
            shutil.move('two_group_file{}'.format(n), output_dir)
            n += 1
    return output_dir

# group_file_spilt('map.txt', 'C:\Users\danhua.qin\Desktop\pythonscript\split_group\output')
