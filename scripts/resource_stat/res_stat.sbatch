#!/bin/bash
#SBATCH -c 20
#SBATCH -D /mnt/ilustre/users/sanger-dev/sg-users/xuting/res_stat
#SBATCH -n 1
#SBATCH -N 1
#SBATCH -J res_stat
#SBATCH -p SANGERDEV
#SBATCH --mem=20G
#SBATCH -o /mnt/ilustre/users/sanger-dev/sg-users/xuting/res_stat/res_stat_%j.out
#SBATCH -e /mnt/ilustre/users/sanger-dev/sg-users/xuting/res_stat/res_stat_%j.err

cd /mnt/ilustre/users/sanger-dev/sg-users/xuting/res_stat

python /mnt/ilustre/users/sanger-dev/biocluster/scripts/resource_stat/res_stat.py -j otu_taxon_new.json -t /mnt/ilustre/users/sanger-dev/sg-users/xuting/res_stat

