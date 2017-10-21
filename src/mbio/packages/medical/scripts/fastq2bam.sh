#!/bin/bash
#sample name
usage() {
    echo "
    Example:
    fastq2bam.sh sample_id cpu_num ref fastq_dir targets_bedfile"
}
#fastq2bam.sh WQ235F 4 /mnt/ilustre/users/sanger-dev/sg-users/xuanhongdong/db/genome/human/hg38.chromosomal_assembly/ref.fa /mnt/ilustre/users/sanger-dev/sg-users/xuanhongdong/share/pt/fastq_gz_dir  /mnt/ilustre/users/sanger-dev/sg-users/xuanhongdong/share/pt/filter_bam_output /mnt/ilustre/users/sanger-dev/sg-users/xuanhongdong/share/pt/snp.chr.sort.3.bed
if [ -z "$1" ] || [[ $1 == -h ]] || [[ $1 == --help ]]
then
    usage
    exit
else
    s=$1
    echo $s
fi 

t=$2
echo $t
# export PATH=/mnt/ilustre/users/sanger-dev/app/gcc/5.4.0/bin:$PATH
# export LD_LIBRARY_PATH=/mnt/ilustre/users/sanger-dev/app/gcc/5.4.0/lib64:$LD_LIBRARY_PATH
# export PATH=/mnt/ilustre/users/sanger-dev/app/program/ruby-2.3.1:$PATH
# export PATH=/mnt/ilustre/users/sanger-dev/app/bioinfo/seq/bioruby-vcf-master/bin:$PATH
# export PATH=/mnt/ilustre/users/sanger-dev/app/bioinfo/seq/bioawk:$PATH
# export PATH=/mnt/ilustre/users/sanger-dev/app/bioinfo/seq/seqtk-master:$PATH
# export PATH=/mnt/ilustre/users/sanger-dev/app/bioinfo/medical/bwa-0.7.15/bin:$PATH
# export PATH=/mnt/ilustre/users/sanger-dev/app/bioinfo/medical/samblaster-0.1.22/bin:$PATH
# export PATH=/mnt/ilustre/users/sanger-dev/app/bioinfo/align/samtools-1.3.1:$PATH
# export PATH=/mnt/ilustre/users/sanger-dev/app/bioinfo/medical/bedtools-2.24.0/bin:$PATH
# export PATH=/mnt/ilustre/users/sanger-dev/app/program/sun_jdk1.8.0/bin:$PATH
# export PATH=/mnt/ilustre/users/sanger-dev/app/bioinfo/medical/bcftools-1.3.0/bin:$PATH
targets_bedfile=$5
echo $targets_bedfile
ref=$3
echo $ref

cd $4
echo $4
java_path=$7
picard_path=$6

pre_seq_num=$(zless $s"_R1.fastq.gz"|head -4000 |bioawk -c fastx 'NR<=1000{a[substr($seq,7,6)]++}END{for(i in a){print i"\t"a[i]}}'|sort -nk2|tail -1|cut -f2)
#zless

if [ $pre_seq_num -gt 500 ];then
    trim_left="-trim_left 12"
    trimfq_left="12"
    do_rarefaction=1
else
    trim_left=""
    trimfq_left="0"
    do_rarefaction=0
fi


pre_seq_num=$(zless $s"_R1.fastq.gz"|head -4000 |bioawk -c fastx 'NR<=1000{a[substr($seq,7,9)]++}END{for(i in a){print i"\t"a[i]}}'|sort -nk2|tail -1|cut -f2)
echo $pre_seq_num

if [ $pre_seq_num -gt 500 ];then
    trim_left="-trim_left 15"
    trimfq_left="15"
    do_rarefaction=1
fi

function go(){
    local s=$1
    {
        [ -f $s"_R1.fastq.gz" ] && bioawk -c fastx '{print "@"$name" X6:Z:"substr($seq,0,6)"\n"$seq"\n+\n"$qual}' $s"_R1.fastq.gz" > $s.with6N_1.fq &
        [ -f $s"_R2.fastq.gz" ] && bioawk -c fastx '{print "@"$name" X6:Z:"substr($seq,0,6)"\n"$seq"\n+\n"$qual}' $s"_R2.fastq.gz" > $s.with6N_2.fq &
        wait
    }
    # fastqc -t $((t-1)) -o /mnt/ilustre/users/fengbo.zeng/app/gada/fastqc $s"_R1.fastq.gz" &

    #查看是否有6个N
    #prinseq-lite.pl -fastq $s.with6N_1.fq -fastq2 $s.with6N_2.fq -out_good $s.dedup -out_bad $s.bad \
    #		                $trim_left && rm $s.with6N_{1,2}.fq
    echo $trimfq_left
   ## map
   seqtk mergepe $s.with6N_1.fq $s.with6N_2.fq |
   seqtk trimfq -b $trimfq_left - |
        bwa mem -p -C -R '@RG\tID:'$s'\tSM:'$s'\tPL:illumina\tPU:illumina\tLB:illumina' -t $t  $ref - |
        tee $s.sam |
        samblaster |
        samtools view -@ $t -Sb - |
        samtools sort -T $s -@ $t - > $s.mem.sort.bam &&
        rm $s.dedup_1.fastq $s.dedup_2.fastq $s.bad_*.fastq
    samtools index $s.mem.sort.bam

    ## target

    
    bedtools intersect -abam $s.mem.sort.bam -b $targets_bedfile  > $s.mem.sort.hit.bam
    
    echo "
function accept(r)
 {
 return r.getReadString().length()>2 && ! r.getCigarString().match(/S/) && r.getMappingQuality()>=30;
 }

accept(record);" > $s.filter.js
    
    $java_path -jar $picard_path FilterSamReads I=$s.mem.sort.hit.bam  O=$s.mem.sort.hit.filter.bam JS=$s.filter.js FILTER=includeJavascript
}

function qc(){
    local all_n=3246
    local name=$(echo $(grep $s id.txt |awk -F'\t' '{print $2}' -))
    echo "num: "$(samtools view -@ $t -c $s.mem.sort.bam) > $s.qc
    echo "n_dedup: "$(samtools view -@ $t -F 0x400 -c $s.mem.sort.bam) >> $s.qc
    echo "n_mapped: "$(samtools view -@ $t -F 0x4 -c $s.mem.sort.bam) >> $s.qc
    echo "n_mapped_dedup: "$(samtools view -@ $t -F 0x404 -c $s.mem.sort.bam) >> $s.qc
    echo "n_hit: "$(samtools view -@ $t -c $s.mem.sort.hit.bam) >> $s.qc
    echo "n_hit_dedup: "$(samtools view -@ $t -F 0x400 -c $s.mem.sort.hit.bam) >> $s.qc
    # echo "dp: " $(awk '{dp+=$6;n+=1}END{print dp/n}' $s.mem.sort.hit.vcf.tab) >> $s.qc
    echo "properly_paired: " $(samtools flagstat $s.mem.sort.bam | awk -F"[(%]" '/properly/ {print $2}') >> $s.qc

}


function isize(){
	  local s=$1
	  samtools view -@ $t -F 0x400 $s.mem.sort.bam |
		    bioawk -c sam '{a[$tlen]++}END{for(i in a){print '$s'"\t"i"\t"a[i]}}'  > $s.isize.xls
}

function rarefaction(){
    local s=$1
    cat /dev/null > $s.rarefaction.tab
    for i in {0..9};do
        for j in 3 6 9;do
            samtools view -@ $t -s 0.${i}${j} -h $s.sam|
                samblaster -r |
                samtools sort -T $s.$i.$j -@ $t - |
                samtools mpileup -uvf $ref -l $targets_bedfile -|
                bcftools call --multiallelic-caller --keep-alts --targets-file $targets_bedfile  |
                bio-vcf --skip-header --eval '[r.chrom,r.pos,r.ref,r.alt.join(","),r.info.dp,r.info.dp4[0..1].reduce(:+),r.info.dp4[2..3].reduce(:+)]' |
                awk -v s=$s -v p=0.${i}${j} '{print s"\t"p"\t"$0}' >> $s.rarefaction.tab
        done
    done
}

function abo0(){
    samtools mpileup -uvf $ref -l $abo_bedfile  $s.mem.sort.bam|
	      bcftools call --multiallelic-caller --keep-alts --targets-file $abo_bedfile |
        grep -v '#' > $s.abo.vcf
    if [[ $(less $s.abo.vcf | wc -l) > 0 ]];then
	      bio-vcf --skip-header --eval '[r.chrom,r.pos,r.ref,r.alt.join(","),r.info.dp,r.info.dp4[0..1].reduce(:+),r.info.dp4[2..3].reduce(:+)]' > $s.abo.tab

        samtools mpileup -uvf $hg38chr -r chr9:133257521-133257521 $s.mem.sort.bam | bcftools call --multiallelic-caller --keep-alts |grep -v '^#' > $s.abo.vcf &&
            hava_snp_and_indel=$(less $s.abo.vcf|wc -l)
        [[ ${hava_snp_and_indel} == 2 ]] && sed -i '1d' $s.abo.vcf
        samtools mpileup -uvf $hg38chr -r chr9:133256264-133256264 $s.mem.sort.bam | bcftools call --multiallelic-caller --keep-alts |grep -v '^#' >> $s.abo.vcf

        bio-vcf --skip-header --eval '[r.chrom,r.pos,r.ref,r.alt.join(","),r.info.dp,r.info.dp4[0..1].reduce(:+),r.info.dp4[2..3].reduce(:+)]' <$s.abo.vcf > $s.abo.vcf.tab

        #awk -v OFS="\t" '{$8="-";p=$6/($6+$7);if(p<0.05){$8="1/1"}else if(p>0.95){$8="0/0"} else{$8="0/1"}print}'
        abo=$(less $s.abo.vcf.tab |awk -v OFS="\t" '{$8="-";p=$6/($6+$7);if(p<0.05){$8="1/1"}else if(p>0.95){$8="0/0"} else{$8="0/1"}print}'|cut -f 8|paste -d, - -)
        grep $abo ../config/ref.tab|cut -d, -f 3 > $s.abo
        cat $s.abo
    fi
}

function main(){
    go $s
    qc $s
    #isize $s
    wait
    
    rm $s.with6N_{1,2}.fq $s.sam $s.filter.js $s.mem.sort.bam.bai $s.mem.sort.hit.reads $s.mem.sort.hit.bam $s.mem.sort.hit.filter.reads $s.mem.sort.bam
}

main