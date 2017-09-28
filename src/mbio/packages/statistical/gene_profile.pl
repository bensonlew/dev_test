#!/usr/bin/perl -w
use strict;

my $script_name = $0;

my $arg;
my $in1;
my $in2;
my $out;

while ($arg=shift) {
        if    ($arg eq "-h" ) { print_usage(); }
	elsif ($arg eq "-i1" ) { $in1 = shift; }
	elsif ($arg eq "-i2" ) { $in2 = shift; }
	elsif ($arg eq "-o" ) { $out = shift; }
}

($in1 and $in2 and $out) || print_usage();

my (%lengths,%ids,%reads,%reads_sum,%reads_sample,%rpkm,%rpkm_sum,%reads_div,%reads_div_sum,@genename,@samps);
my (%total_reads,%total_reads_relative,%total_base_relative,%total_base,%total_rpkm,%total_tpm);

if($in1=~/\.gz$/){
    open I1,"gzip -dc $in1 |" or die "can't read $in1: $!\n";
}else{
    open I1,$in1 or die "can't read $in1: $!\n";
}

my $in1name=(split /\//,$in1)[-1];
$in1name=~s/\.gz$//;
open O1,">$out/$in1name.length.txt" or die "can't write $out/$in1name.length.txt: $!\n";
print O1 "gene_ID\tgene_name\tgene_length\n";

$/=">";
<I1>;
my $i=0;
while(<I1>){
    $i++;
    chomp;
    my @list=split /\n/;
    my $name=(split /\s+/,(shift @list))[0];
    my $seq=join "",@list;
    my $len=length($seq);
    $lengths{$name}=$len;
    $ids{$name}=$i;
    push @genename,$name;
    print O1 "$i\t$name\t$len\n";
}
close I1;
close O1;
$/="\n";

open I2,$in2 or die "can't read $in2: $!\n";
<I2>;
while(<I2>){
    chomp;
    my ($samp,$insert,$soap)=(split /\s+/)[0,1,2];
    push @samps,$samp;
    my @soaps=split /,/,$soap;
    foreach my $pese(@soaps){
	if($pese =~ /\.pe$/ or $pese =~ /\.pe\.gz$/){
	    if($pese =~ /\.pe$/){
		open PE,$pese or die "can't read $pese: $!\n";
	    }elsif($pese =~ /\.pe\.gz$/){
		open PE,"gzip -dc $pese |" or die "can't read $pese: $!\n";
	    }
	    my @tmp;
	    while(<PE>){
		chomp;
		@tmp=split;
		if($tmp[3] == 1){
		    $reads{$tmp[7]}{$samp}+=0.5;
            $reads_sample{$samp}+=0.5;
		}
	    }
	    close PE;
	}elsif($pese =~ /\.se$/ or $pese =~ /\.se\.gz$/){
	    if($pese =~ /\.se$/){
		open SE,$pese or die "can't read $pese: $!\n";
	    }elsif($pese =~ /\.se\.gz$/){
		open SE,"gzip -dc $pese |" or die "can't read $pese: $!\n";
	    }
	    my(@tmp,%past);
	    while(<SE>){
		chomp;
		@tmp=split;
		if($tmp[3] == 1){
		    if($tmp[6] eq '+'){
			if(($lengths{$tmp[7]}-$tmp[8])<$insert+100){
			    $past{$tmp[7]}{$tmp[0]}=1;
			}
		    }elsif($tmp[6] eq '-'){
			if(($tmp[8]+$tmp[5])<$insert+100){
			    $past{$tmp[7]}{$tmp[0]}=1;
			}
		    }
		}
	    }
	    close SE;
	    foreach my $k(sort keys %past){
		foreach my $read(sort keys %{$past{$k}}){
		    if($past{$k}{$read}){
			$reads{$k}{$samp}++;
            $reads_sample{$samp}++;
		    }
		}
	    }
	}
    }
}

foreach my $gen(@genename){
    foreach my $sam(@samps){
	$reads{$gen}{$sam} = 0 unless(defined $reads{$gen}{$sam});
	$reads_div{$gen}{$sam} = $reads{$gen}{$sam}/$lengths{$gen};
	$reads_sum{$sam} += $reads{$gen}{$sam};
	$reads_div_sum{$sam} += $reads_div{$gen}{$sam};
    $rpkm{$gen}{$sam} = ($reads{$gen}{$sam}*(10**9))/($reads_sample{$sam}*$lengths{$gen});
    $rpkm_sum{$sam} += $rpkm{$gen}{$sam};
    }
}

open O2,">$out/reads_number.xls" or die "can't write $out/reads_number.xls: $!\n";
open O3,">$out/reads_number_relative.xls" or die "can't write $out/reads_number_relative.xls: $!\n";
open O4,">$out/reads_length_ratio_relative.xls" or die "can't write $out/reads_length_ratio_relative.xls: $!\n";
open O5,">$out/reads_length_ratio.xls" or die "can't write $out/reads_length_ratio.xls: $!\n";
open O6,">$out/RPKM.xls" or die "can't write $out/RPKM.xls: $!\n";
open O7,">$out/TPM.xls" or die "can't write $out/TPM.xls: $!\n";

my $head=join "\t",@samps;
print O2 "GeneID\t$head\tTotal\n";
print O3 "GeneID\t$head\tTotal\n";
print O4 "GeneID\t$head\tTotal\n";
print O5 "GeneID\t$head\tTotal\n";
print O6 "GeneID\t$head\tTotal\n";
print O7 "GeneID\t$head\tTotal\n";

foreach my $gen(@genename){
    print O2 $gen;
    print O3 $gen;
    print O4 $gen;
    print O5 $gen;
    print O6 $gen;
    foreach my $sam(@samps){
	print O2 "\t",$reads{$gen}{$sam}*2;
	print O3 "\t",$reads{$gen}{$sam}/$reads_sum{$sam};
	print O4 "\t",$reads_div{$gen}{$sam}/$reads_div_sum{$sam};
    print O5 "\t",$reads_div{$gen}{$sam}*2;
    print O6 "\t",$rpkm{$gen}{$sam};
    print O7 "\t",$reads_div{$gen}{$sam}*(10**6)/$reads_div_sum{$sam};
    $total_reads{$gen} += $reads{$gen}{$sam}*2;
    $total_reads_relative{$gen} += $reads{$gen}{$sam}/$reads_sum{$sam};
    $total_base_relative{$gen} += $reads_div{$gen}{$sam}/$reads_div_sum{$sam};
    $total_base{$gen} += $reads_div{$gen}{$sam}*2;
    $total_rpkm{$gen} += $rpkm{$gen}{$sam};
    $total_tpm{$gen} += $reads_div{$gen}{$sam}*(10**6)/$reads_div_sum{$sam};
    }
    print O2 "\t$total_reads{$gen}\n";
    print O3 "\t$total_reads_relative{$gen}\n";
    print O4 "\t$total_base_relative{$gen}\n";
    print O5 "\t$total_base{$gen}\n";
    print O6 "\t$total_rpkm{$gen}\n";
    print O7 "\t$total_tpm{$gen}\n";
}
close O2;
close O3;
close O4;
close O5;
close O6;
close O7;

my $num = @samps + 2;
`head -n 1 $out/reads_number.xls > $out/top100_reads_number.xls`;
`head -n 1 $out/reads_number_relative.xls > $out/top100_reads_number_relative.xls`;
`awk '{if(NR!=1) print }' $out/reads_number.xls | sort -gr -k $num | head -n 100 >> $out/top100_reads_number.xls`;
`awk '{if(NR!=1) print }' $out/reads_number_relative.xls |sort -gr -k $num  | head -n 100 >> $out/top100_reads_number_relative.xls`;

sub print_usage {
        print <<EOD;
Usage: perl $script_name options
    -i1 input gene file, required
    -i2 input information of insertsize and soap result, required
     -o output directory, required
     -h print this help

EOD
exit;
}
