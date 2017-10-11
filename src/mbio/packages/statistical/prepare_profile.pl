#!/usr/bin/perl -w
use strict;
use warnings;

die "usage: perl $0 soap.dir geneSet.fa insertsize outDir\n" unless(@ARGV == 4);
my ($inputDir,$geneSet,$insertSize,$outDir) = @ARGV;

`mkdir $outDir` unless(-e "$outDir");
my (%inserts);
if(-e $insertSize){
    open INI,$insertSize or die "$!\n";
	while(<INI>){
	chomp;
	my @temp = split;
	$inserts{$temp[0]} = $temp[1];
	}
	close INI;
}else{
    die "error: insertSize must be a file or a number: $insertSize\n" unless($insertSize =~ /^([\d\.]+)$/);
}

$outDir =~ s/\/$//;
my $mess = `find $inputDir/*/*soap*[ps]e`;
open OUL,"> $outDir/soap.info" or die "$!\n";
print OUL "#Sample\tInsertSize\tSoapResult\n";
my @file1 = split /\n/,$mess;

my %soap;
for(my $i = 0;$i < @file1;$i++){
    my @temp = split /\//,$file1[$i];
	$soap{$temp[-2]} .= "$file1[$i],";
}

foreach my $sam(keys %inserts){
    if(exists $soap{$sam}){
	    $soap{$sam} =~ s/\,$//;
		if(-e $insertSize){
		    print OUL "$sam\t$inserts{$sam}\t$soap{$sam}\n";
		}else{
		    print OUL "$sam\t$insertSize\t$soap{$sam}\n";
		}
    }else{
	    print STDERR "Error: not found soap result of $sam in $inputDir/*/*soap*[ps]e\n";
		exit;
	}
}
close OUL;
