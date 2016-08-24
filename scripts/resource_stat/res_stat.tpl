#!/bin/bash
#SBATCH -c ${dic["cpu"]}
#SBATCH -D ${dic["targetPath"]}
#SBATCH -n 1
#SBATCH -N 1
#SBATCH -J ${dic["name"]}
#SBATCH -p SANGERDEV
#SBATCH --mem=${dic["mem"]}G
#SBATCH -o ${dic["targetPath"]}/${dic["name"]}_%j.out
#SBATCH -e ${dic["targetPath"]}/${dic["name"]}_%j.err

cd ${dic["targetPath"]}

python ${dic["home"]}/biocluster/scripts/resource_stat/res_stat.py -j ${dic["json"]} -t ${dic["targetPath"]}
