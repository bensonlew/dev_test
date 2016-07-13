# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'
import re


def pfam_out(domtblout):
    with open(domtblout, "r") as f, open("pfam_domain", "w") as p:
        p.write("Seq_id\tProtein_id\tPfam_id\tDomain\tDomainDescription\tProteinStart\tProtein\tPfamStart\tPfamEnd\t"
                "DomainE-Value\n")
        for line in f:
            if re.match(r"#", line):
                continue
            else:
                line = line.strip("\n").split()
                # print line
                seq_id = line[3].split("|")[0]
                description = line[22:]
                description = " ".join(description)
                # print description
                write_line = seq_id + "\t" + line[3] + "\t" + line[1] + "\t" + line[0] + "\t" + description + "\t" + line[17]
                write_line = write_line + "\t" + line[18] + "\t" + line[15] + "\t" + line[16] + "\t" + line[12] + "\n"
                p.write(write_line)
