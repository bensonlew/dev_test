#!/usr/bin/perl

use strict;
use warnings;
use Getopt::Long;
my %opts;
my $VERSION = "v2.20160713"; 

GetOptions( \%opts,"i1=s","i2=s", "o=s");
my $usage = <<"USAGE";
       Program : $0   
       Discription:plot the ROC curve for cluster algorithm   
       Version : $VERSION
       Contact : Jie Yao\@majorbio.com
       Usage :perl $0 [options]		
			-i1	* rocdata table with 3 columns.The headers are:diagnosis measure group(ignore group if don\'t have) 
			-i2     * predicted answer file from RandomForest
                        -o	* output dir
	   Example:$0 -i rocdata.txt -o .  

USAGE

die $usage if(!($opts{i1}&&$opts{i2}&&$opts{o}));


if(! -e $opts{o}){
		`mkdir $opts{o}`;
}
		

open CMD,">$opts{o}/roc_cmd.r";

print CMD "
library(pROC)
probdata <- read.table(\"$opts{i1}\",header=T,check.names=F,comment.char=\"\")
preddata <- read.table(\"$opts{i2}\",header=T,check.names=F,comment.char=\"\")


### deal with probably data
##probdata <- as.data.frame(probdata)
group_name <- colnames(probdata)
n <- length(group_name)
sample_num <- length(probdata[,1])

### deal with predicted data
sample_name <- row.names(preddata)

rocdata <- data.frame(diagnosis=rep(0,sample_num), measure=rep(0,sample_num),group=rep(\"G1\",sample_num))
for (i in rep(1:sample_num)){
for (j in rep(1:n)){
if (as.character(preddata[i,1]) == as.character(group_name[j])){
rocdata\$diagnosis[i] <- (n+j)*0.5
}
}
for (j in rep(1:n)){
rocdata\$measure[i] <- rocdata\$measure[i]+j*probdata[i,j]
}
}
write.table(rocdata,paste(\"$opts{o}\",\"roc_table.xls\", sep=\"\"),row.names=FALSE,quote=F, sep=\"\\t\")
rocdata <- read.table(paste(\"$opts{o}\",\"roc_table.xls\", sep=\"\"),header=T,check.names=F,comment.char=\"\")

### file with group
if(ncol(rocdata)==3){
colnames(rocdata) <- c(\"diagnosis\",\"measure\",\"group\")
group_set <- levels(rocdata\$group)
auc <- data.frame(PANELgroupAUC=rep(0,length(group_set)), Group=group_set)
x <- data.frame(spe=rep(0,(length(rocdata[,1])+4)),sen=rep(0,(length(rocdata[,1])+4)),group=rep(0,(length(rocdata[,1])+4)))
x <- x[-1,]
now = 1
tmp = roc(rocdata[rocdata[,3]==group_set[1],]\$diagnosis,rocdata[rocdata[,3]==group_set[1],]\$measure)
x\$spe[now:(now+length(tmp\$specificities)-1)] <- 1-tmp\$specificities
x\$sen[now:(now+length(tmp\$sensitivities)-1)] <- tmp\$sensitivities
x\$group[now:(now+length(tmp\$sensitivities)-1)] <- rep(group_set[1],length(tmp\$sensitivities))
auc\$PANELgroupAUC[1] = tmp\$auc
while (x\$spe[length(x\$spe)]+x\$sen[length(x\$sen)]==0 && x\$group[length(x\$group)]==\"0\"){
x <- x[-length(x\$spe),]
}
now <- now+length(tmp\$sensitivities)
###plot(1-tmp\$specificities,tmp\$sensitivities,type='o',pch=20,xlab=\"1-Specificity\",ylab=\"Sensitivity\")
if (length(group_set)>=2){
for (i in seq(2,length(group_set),1)){
tmp = roc(rocdata[rocdata[,3]==group_set[i],]\$diagnosis,rocdata[rocdata[,3]==group_set[i],]\$measure)
x\$spe[now:(now+length(tmp\$specificities)-1)] <- 1-tmp\$specificities
x\$sen[now:(now+length(tmp\$sensitivities)-1)] <- tmp\$sensitivities
x\$group[now:(now+length(tmp\$sensitivities)-1)] <- rep(group_set[i],length(tmp\$sensitivities))
now <- now+length(tmp\$sensitivities)
auc\$PANELgroupAUC[i] <- tmp\$auc
###lines(1-tmp\$specificities,tmp\$sensitivities,col=i,type='o',pch=20)
}
}
###lines(tmp\$specificities,tmp\$specificities)
###legend(\"bottomright\",lty=rep(1,length(group_set)),ncol=1,col=c(1:length(group_set)),legend=group_set,horiz=FALSE)
x <- as.data.frame(x)
colnames(x) <- c(\"1-Specificities\",\"Sensitivities\",\"Group\")
write.table(x, paste(\"$opts{o}\",\"roc_point.xls\", sep=\"\"), row.names=FALSE, quote=F, sep=\"\\t\")
write.table(auc,paste(\"$opts{o}\",\"auc.xls\", sep=\"\"), row.names=FALSE, quote=F, sep=\"\\t\") 
}


###file without group
if(ncol(rocdata)==2){
colnames(rocdata) <- c(\"diagnosis\",\"measure\")
auc <- data.frame(PANELgroupAUC=0)
x <- data.frame(spe=rep(0,(length(rocdata[,1])+4)),sen=rep(0,(length(rocdata[,1])+4)))
x <- x[-1,]
now = 1
tmp = roc(rocdata\$diagnosis,rocdata\$measure)
x\$spe[now:(now+length(tmp\$specificities)-1)] <- 1-tmp\$specificities
x\$sen[now:(now+length(tmp\$sensitivities)-1)] <- tmp\$sensitivities
###plot(1-tmp\$specificities,tmp\$sensitivities,type='o',pch=20,xlab=\"1-Specificity\",ylab=\"Sensitivity\")
###lines(tmp\$specificities,tmp\$specificities)
auc\$PANELgroupAUC = tmp\$auc
x <- as.data.frame(x)
while (x\$spe[length(x\$spe)]+x\$sen[length(x\$sen)]==0 ){
x <- x[-length(x\$spe),]
}
colnames(x) <- c(\"1-Specificities\",\"Sensitivities\")
write.table(x,paste(\"$opts{o}\",\"roc_point.xls\", sep=\"\"),row.names=FALSE, quote=F, sep=\"\\t\")
write.table(auc,paste(\"$opts{o}\",\"auc.xls\", sep=\"\"), row.names=FALSE, quote=F, sep=\"\\t\")
}

	
";

`R --restore --no-save < $opts{o}/roc_cmd.r`;
