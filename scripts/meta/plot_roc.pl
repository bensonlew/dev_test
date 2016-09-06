 #!/usr/bin/perl

use strict;
use warnings;
use Getopt::Long;
my %opts;
my $VERSION = "v1.20160817"; 

GetOptions( \%opts,"mode=s","i=s","group=s","o=s","n=s","method=s","name=s","ncuts=i","labels=s","labelsize=s","rocci=s","siglevel=f","w=f","h=f","facet_wrap=s","theme=s");
my $usage = <<"USAGE";
       Program : $0   
       Discription:   
       Version : $VERSION
       Contact : chengchen.ye\@majorbio.com
       Usage :perl $0 [options]		
                        -mode	*1 or 2 or 3; The Receiver:1)The relative abundance of the top n  Bacteria(could analysis with more than one group)
                                                           2)The relative abundance of special bacteria(one or more)
                                                           3)The receiver is any other factors      
			-i	*Input genus table file(or other Taxonomic level) or any other factors(mode_3).
			-group  *Group name in mapping file .
                        -o	*Output dir
                        -n	(*mode_1)Top n genus or other taxonomic level(relative abundance)
                        -method (*mode_1)If you choose the mode_2 to analyze, you can also choose the analysis "methed". If you don't have a choice, you will make a separate analysis of them.   Follow method are available:sum, average, median
                        -name	(*mode_2)the name of Bacteria 
			-ncuts	Number of cutpoints to display along each curve.Default:20
			-labels	Logical, display cutoff text labels:Default:F
			-labelsize	Size of cutoff text labels.Default:3
			-rocci	Confidence regions for the ROC curve.Default:F
			-siglevel	Significance level for the confidence regions.Default:0.05
			-w	default:6
			-h	default:5
			-facet_wrap  	Logical,display group in different panel.Default:F
			-theme  themes for display roc.Follow themes are available:theme_bw,theme_classic,theme_dark,theme_gray,theme_light.Default:theme_bw
	   Example:$0 -mode 1 -i genus.xls -group group.txt -o output_dir -n 30  -labels F -method sum
                   $0 -mode 2 -i genus.xls -group group_1.txt -o output_dir -name name.txt  -labels F -w 7.8 
                   $0 -mode 2 -i genus.xls -group group_1.txt -o output_dir -name name.txt  -labels F -method sum
                   $0 -mode 3 -i factor.txt -group group.txt -o output_dir -labels F -w 7.8

USAGE

die $usage if(!($opts{mode}&&$opts{i}&&$opts{group}&&$opts{o}));
#die $usage if(!(($opts{mode}==F)&&$opts{i}&&$opts{group}&&$opts{o}&&$opts{name}));


$opts{n}=defined $opts{n}?$opts{n}:"20";
$opts{method}=defined $opts{method}?$opts{method}:"chengchen.ye";
$opts{name}=defined $opts{name}?$opts{name}:"NULL";
$opts{ncuts}=defined $opts{ncuts}?$opts{ncuts}:20;
$opts{labels}=defined $opts{labels}?$opts{labels}:"F";
$opts{labelsize}=defined $opts{labelsize}?$opts{labelsize}:"3";
$opts{w}=defined $opts{w}?$opts{w}:6;
$opts{h}=defined $opts{h}?$opts{h}:5;
$opts{facet_wrap}=defined $opts{facet_wrap}?$opts{facet_wrap}:"F";
$opts{theme}=defined $opts{theme}?$opts{theme}:"";
$opts{rocci}=defined $opts{rocci}?$opts{rocci}:"F";
$opts{siglevel}=defined $opts{siglevel}?$opts{siglevel}:"0.05";

if(! -e $opts{o}){
		`mkdir $opts{o}`;
}
		

open CMD,">$opts{o}/roc.cmd.r";

print CMD "
	library(plotROC)

	
        group <- read.table(\"$opts{group}\",header=T,check.names=F,comment.char=\"\")
        otu_table <-read.table(\"$opts{i}\",sep=\"\t\",head=T,check.names = F,row.names=1)
        y<-length(otu_table[1,])
        



###The Receiver:A)The relative abundance of the Bacteria
if($opts{mode}==1){
        group<-as.data.frame(group)
        x<-length(group[1,])
        ###D
        D<-group[,2]

        ####M        
        otu_table<-cbind(otu_table,rowsum=rowSums(otu_table))
        otu_table<- otu_table[order(otu_table[,y+1],decreasing=T),]
        otu_table_top20<- otu_table[1:$opts{n},]
             ####sum
if(\"$opts{method}\"==\"sum\" ){
             v<-y+1
             m<-data.frame(colSums(otu_table_top20))[-v,]
}


             ####average
if(\"$opts{method}\"==\"average\"){                     
             v<-y+1
             m<-data.frame(colSums(otu_table_top20)/$opts{n})[-v,]
}
          
             ####median
if(\"$opts{method}\"==\"median\"){          
             top20<-otu_table_top20[,1:y]
             m<-as.matrix(as.matrix(top20[ceiling($opts{n}/2),])[1,])
}        

         ###Z
         Z<-c(rep(\"group1\",y))
         i<-2
     
         while (i <= x-1) {
              ###D
              D<-c(D,group[,i+1])
              ###M
              m<-c(m,m)
              ###Z
              w<-paste(\"group\",i,sep=\"\")
              Z<-c(Z,rep(w,y))
              i <- i +1
         }
         #
         M <- scale(m, center=T,scale=T)  #标准化
         if(x!=2){ 
             rocdata<- data.frame(diagnosis = D, measure = M, group = Z)       
             p <- ggplot(rocdata, aes(m = measure , d = diagnosis ,color=group)) + geom_roc(labels=$opts{labels},labelsize=$opts{labelsize},n.cuts=$opts{ncuts}) + style_roc(major.breaks = c(0, 0.25, 0.5, 0.75, 1), theme=$opts{theme},xlab=\"1-Specificity\",ylab=\"Sensitivity\") 

             if($opts{rocci}==T){
                 p <- p + geom_rocci(sig.level=$opts{siglevel})
             }

             if($opts{facet_wrap}==T){
                 p <- p + facet_wrap(~ group) + theme(axis.text = element_text(size = 4))
             }

         }

         if(x==2){

             rocdata<- data.frame(diagnosis = D, measure = M)
             p <- ggplot(rocdata, aes(m = measure , d = diagnosis )) + geom_roc(labels=$opts{labels},labelsize=$opts{labelsize},n.cuts=$opts{ncuts}) + style_roc(major.breaks = c(0, 0.25, 0.5, 0.75, 1), theme=$opts{theme},xlab=\"1-Specificity\",ylab=\"Sensitivity\") 

         if($opts{rocci}==T){
             p <- p + geom_rocci(sig.level=$opts{siglevel})
         }


        }
x<-x-1
}

###The Receiver:2)The relative abundance of special bacteria.
if($opts{mode}==2){
      
if(\"$opts{method}\"==\"chengchen.ye\"){
       name <- read.table(\"$opts{name}\",header=T,check.names=F,comment.char=\"\")
       name<-as.data.frame(name)
       x<-length(name[,1])
       a<-as.character(name[1,1])
       D<-group[,2] 
       m<-as.matrix(as.matrix(otu_table)[a,])

       Z<-c(rep(a,y))
       i<-2
       while (i <= x) {
           D<-c(D,group[,2])
           a<-as.character(name[i,1])
           m<-c(m,as.matrix(as.matrix(otu_table)[a,]))
           a<-as.character(name[i,1])
           Z<-c(Z,rep(a,y))
           i <- i +1
       }


        M <- scale(m, center=T,scale=T)  #标准化
if(x!=1){ 
        rocdata<- data.frame(diagnosis = D, measure = M, Bacteria = Z)       
	p <- ggplot(rocdata, aes(m = measure , d = diagnosis ,color=Bacteria)) + geom_roc(labels=$opts{labels},labelsize=$opts{labelsize},n.cuts=$opts{ncuts}) + style_roc(major.breaks = c(0, 0.25, 0.5, 0.75, 1), theme=$opts{theme},xlab=\"1-Specificity\",ylab=\"Sensitivity\") 

if($opts{rocci}==T){
	p <- p + geom_rocci(sig.level=$opts{siglevel})
}

if($opts{facet_wrap}==T){
	p <- p + facet_wrap(~ group) + theme(axis.text = element_text(size = 4))
	}

}

if(x==1){

        rocdata<- data.frame(diagnosis = D, measure = M)
        p <- ggplot(rocdata, aes(m = measure , d = diagnosis )) + geom_roc(labels=$opts{labels},labelsize=$opts{labelsize},n.cuts=$opts{ncuts}) + style_roc(major.breaks = c(0, 0.25, 0.5, 0.75, 1), theme=$opts{theme},xlab=\"1-Specificity\",ylab=\"Sensitivity\") 

if($opts{rocci}==T){
        p <- p + geom_rocci(sig.level=$opts{siglevel})
        }
}
}
######choose method
if(\"$opts{method}\"!=\"chengchen.ye\"){
name <- read.table(\"name.txt\",header=T,check.names=F,comment.char=\"\")
otu_table<-as.data.frame(otu_table)
nu<-length(name[,1])

genus<-otu_table[as.character(name[1,1]),]
i<-2
while (i <= nu) {
genus<-rbind(genus,otu_table[as.character(name[i,1]),])
i<-i+1
}
genus 
group<-as.data.frame(group)
x<-length(group[1,])
    ###D
        D<-group[,2]

    ####M        
        genus<-cbind(genus,rowsum=rowSums(genus))
        genus<- genus[order(genus[,y+1],decreasing=T),]		

		

             ####sum
if(\"$opts{method}\"==\"sum\" ){
             hh<-y+1
             m<-data.frame(colSums(genus))[-hh,]
}


             ####average
if(\"$opts{method}\"==\"average\"){                     
             hh<-y+1
             m<-data.frame(colSums(genus)/nu)[-hh,]
}
          
             ####median
if(\"$opts{method}\"==\"median\"){          
             genus_order<-genus[,1:y]
             m<-as.matrix(as.matrix(genus_order[ceiling(nu/2),])[1,])
}
     ###Z
         Z<-c(rep(\"group1\",y))
         i<-2
     
         while (i <= x-1) {
              ###D
              D<-c(D,group[,i+1])
              ###M
              m<-c(m,m)
              ###Z
              w<-paste(\"group\",i,sep=\"\")
              Z<-c(Z,rep(w,y))
              i <- i +1
         }
         #
         M <- scale(m, center=T,scale=T)  #标准化    
         if(x!=2){ 
             rocdata<- data.frame(diagnosis = D, measure = M, group = Z)       
             p <- ggplot(rocdata, aes(m = measure , d = diagnosis ,color=group)) + geom_roc(labels=$opts{labels},labelsize=$opts{labelsize},n.cuts=$opts{ncuts}) + style_roc(major.breaks = c(0, 0.25, 0.5, 0.75, 1), theme=$opts{theme},xlab=\"1-Specificity\",ylab=\"Sensitivity\") 

             if($opts{rocci}==T){
                 p <- p + geom_rocci(sig.level=$opts{siglevel})
             }

             if($opts{facet_wrap}==T){
                 p <- p + facet_wrap(~ group) + theme(axis.text = element_text(size = 4))
             }

         }

         if(x==2){

             rocdata<- data.frame(diagnosis = D, measure = M)
             p <- ggplot(rocdata, aes(m = measure , d = diagnosis )) + geom_roc(labels=$opts{labels},labelsize=$opts{labelsize},n.cuts=$opts{ncuts}) + style_roc(major.breaks = c(0, 0.25, 0.5, 0.75, 1), theme=$opts{theme},xlab=\"1-Specificity\",ylab=\"Sensitivity\") 

         if($opts{rocci}==T){
             p <- p + geom_rocci(sig.level=$opts{siglevel})
         }


        }




x<-x-1



}
}




###The Receiver:3)The receiver is any other factors 
if($opts{mode}==3){

factor <-read.table(\"$opts{i}\",sep=\"\t\",head=T,check.names = F,row.names=1)
x<-length(factor[1,])

D<-group[,2] 

m<-factor[,1]      
####Z
factor_name <-read.table(\"$opts{i}\",sep=\"\t\",check.names = F,row.names=1)
name<-as.character(factor_name[1,1])
len<-length(factor[,1])
Z<-c(rep(name,len))
i<-2
while (i <= x) {
    D<-c(D,D)
        m<-c(m,factor[,i])           
            name<-as.character(factor_name[1,i])
                Z<-c(Z,rep(name,len))     
                    i <- i +1
                    }
M <- scale(m, center=T,scale=T)

if(x!=1){ 
        rocdata<- data.frame(diagnosis = D, measure = M, factor = Z)       
        p <- ggplot(rocdata, aes(m = measure , d = diagnosis ,color=factor)) + geom_roc(labels=$opts{labels},labelsize=$opts{labelsize},n.cuts=$opts{ncuts}) + style_roc(major.breaks = c(0, 0.25, 0.5, 0.75, 1), theme=$opts{theme},xlab=\"1-Specificity\",ylab=\"Sensitivity\") 

if($opts{rocci}==T){
        p <- p + geom_rocci(sig.level=$opts{siglevel})
}

if($opts{facet_wrap}==T){
        p <- p + facet_wrap(~ group) + theme(axis.text = element_text(size = 4))
        }

}

if(x==1){

        rocdata<- data.frame(diagnosis = D, measure = M)
        p <- ggplot(rocdata, aes(m = measure , d = diagnosis )) + geom_roc(labels=$opts{labels},labelsize=$opts{labelsize},n.cuts=$opts{ncuts}) + style_roc(major.breaks = c(0, 0.25, 0.5, 0.75, 1), theme=$opts{theme},xlab=\"1-Specificity\",ylab=\"Sensitivity\") 

if($opts{rocci}==T){
        p <- p + geom_rocci(sig.level=$opts{siglevel})
        }

}


}








### Caculate the Area under the ROC curve
p.auc <- calc_auc(p)
write.table(p.auc,\"$opts{o}/roc_aucvalue.xls\",col.names=T,row.names=F,sep=\"\t\",quote=F)


if($opts{mode}==1){


###paste AUC to graph
if(x==1){

test_auc<-paste(\"AUC=\",round(p.auc[1,3] * 100, 2),\"%\",sep = \"\")

p<-p + geom_text(x=0.8,y=0.1,label=test_auc,size=4)


}



if(x==2){
test_auc1<-paste(\"AUC_group1=\",round(p.auc[1,3] * 100, 2),\"%\",sep = \"\")
test_auc2<-paste(\"AUC_group2=\",round(p.auc[2,3] * 100, 2),\"%\",sep = \"\")
p<-p + geom_text(x=0.8,y=0.21,label=test_auc1,size=4,colour=\"black\")
p<-p + geom_text(x=0.8,y=0.15,label=test_auc2,size=4,colour=\"black\")

}


if(x==3){
test_auc1<-paste(\"AUC_group1=\",round(p.auc[1,3] * 100, 2),\"%\",sep = \"\")
test_auc2<-paste(\"AUC_group2=\",round(p.auc[2,3] * 100, 2),\"%\",sep = \"\")
test_auc3<-paste(\"AUC_group3=\",round(p.auc[3,3] * 100, 2),\"%\",sep = \"\")
p<-p + geom_text(x=0.8,y=0.18,label=test_auc1,size=4,colour=\"black\")
p<-p + geom_text(x=0.8,y=0.12,label=test_auc2,size=4,colour=\"black\")
p<-p + geom_text(x=0.8,y=0.06,label=test_auc3,size=4,colour=\"black\")
}

}

if($opts{mode}==2){
      
if(\"$opts{method}\"==\"chengchen.ye\"){

###auc_name
auc_name<-as.data.frame(sort(name[,1],decreasing=F))

###paste AUC to graph
if(x==1){

test_auc<-paste(\"AUC=\",round(p.auc[1,3] * 100, 2),\"%\",sep = \"\")

p<-p + geom_text(x=0.8,y=0.1,label=test_auc,size=4)


}



if(x==2){
test_auc1<-paste(\"AUC\(\",as.character(auc_name[1,1]),\"\)=\",round(p.auc[1,3] * 100, 2),\"%\",sep = \"\")
test_auc2<-paste(\"AUC\(\",as.character(auc_name[2,1]),\"\)=\",round(p.auc[2,3] * 100, 2),\"%\",sep = \"\")
p<-p + geom_text(x=Inf,y=-Inf,hjust=1.2,vjust=-4,label=test_auc1,size=4,colour=\"black\")
p<-p + geom_text(x=Inf,y=-Inf,hjust=1.2,vjust=-2,label=test_auc2,size=4,colour=\"black\")

}


if(x==3){
test_auc1<-paste(\"AUC\(\",as.character(auc_name[1,1]),\"\)=\",round(p.auc[1,3] * 100, 2),\"%\",sep = \"\")
test_auc2<-paste(\"AUC\(\",as.character(auc_name[2,1]),\"\)=\",round(p.auc[2,3] * 100, 2),\"%\",sep = \"\")
test_auc3<-paste(\"AUC\(\",as.character(auc_name[3,1]),\"\)=\",round(p.auc[3,3] * 100, 2),\"%\",sep = \"\")
p<-p + geom_text(x=Inf,y=-Inf,hjust=1.1,vjust=-6,label=test_auc1,size=4,colour=\"black\")
p<-p + geom_text(x=Inf,y=-Inf,hjust=1.2,vjust=-4,label=test_auc2,size=4,colour=\"black\")
p<-p + geom_text(x=Inf,y=-Inf,hjust=1.1,vjust=-2,label=test_auc3,size=4,colour=\"black\")
}


}

if(\"$opts{method}\"!=\"chengchen.ye\"){

###paste AUC to graph
if(x==1){

test_auc<-paste(\"AUC=\",round(p.auc[1,3] * 100, 2),\"%\",sep = \"\")

p<-p + geom_text(x=0.8,y=0.1,label=test_auc,size=4)


}



if(x==2){
test_auc1<-paste(\"AUC_group1=\",round(p.auc[1,3] * 100, 2),\"%\",sep = \"\")
test_auc2<-paste(\"AUC_group2=\",round(p.auc[2,3] * 100, 2),\"%\",sep = \"\")
p<-p + geom_text(x=0.8,y=0.3,label=test_auc1,size=4,colour=\"black\")
p<-p + geom_text(x=0.8,y=0.25,label=test_auc2,size=4,colour=\"black\")

}


if(x==3){
test_auc1<-paste(\"AUC_group1=\",round(p.auc[1,3] * 100, 2),\"%\",sep = \"\")
test_auc2<-paste(\"AUC_group2=\",round(p.auc[2,3] * 100, 2),\"%\",sep = \"\")
test_auc3<-paste(\"AUC_group3=\",round(p.auc[3,3] * 100, 2),\"%\",sep = \"\")
p<-p + geom_text(x=0.8,y=0.22,label=test_auc1,size=4,colour=\"black\")
p<-p + geom_text(x=0.8,y=0.16,label=test_auc2,size=4,colour=\"black\")
p<-p + geom_text(x=0.8,y=0.1,label=test_auc3,size=4,colour=\"black\")
}
}
}



if($opts{mode}==3){


###auc_name

auc_name<-as.data.frame(sort(factor_name[1,],decreasing=F))


###paste AUC to graph
if(x==1){

test_auc<-paste(\"AUC=\",round(p.auc[1,3] * 100, 2),\"%\",sep = \"\")

p<-p + geom_text(x=0.8,y=0.1,label=test_auc,size=4)


}



if(x==2){
test_auc1<-paste(\"AUC_\",as.character(auc_name[1,1]),\"=\",round(p.auc[1,3] * 100, 2),\"%\",sep = \"\")
test_auc2<-paste(\"AUC_\",as.character(auc_name[1,2]),\"=\",round(p.auc[2,3] * 100, 2),\"%\",sep = \"\")
p<-p + geom_text(x=Inf,y=-Inf,hjust=1.2,vjust=-4,label=test_auc1,size=4,colour=\"black\")
p<-p + geom_text(x=Inf,y=-Inf,hjust=1.2,vjust=-2,label=test_auc2,size=4,colour=\"black\")

}


if(x==3){
test_auc1<-paste(\"AUC_\",as.character(auc_name[1,1]),\"=\",round(p.auc[1,3] * 100, 2),\"%\",sep = \"\")
test_auc2<-paste(\"AUC_\",as.character(auc_name[1,2]),\"=\",round(p.auc[2,3] * 100, 2),\"%\",sep = \"\")
test_auc3<-paste(\"AUC_\",as.character(auc_name[1,3]),\"=\",round(p.auc[3,3] * 100, 2),\"%\",sep = \"\")
p<-p + geom_text(x=0.8,y=0.18,label=test_auc1,size=4,colour=\"black\")
p<-p + geom_text(x=0.8,y=0.12,label=test_auc2,size=4,colour=\"black\")
p<-p + geom_text(x=0.8,y=0.06,label=test_auc3,size=4,colour=\"black\")
}








}


pdf(\"$opts{o}/roc_curve.pdf\",width=$opts{w},height=$opts{h})
p
dev.off()

	
";






`R --restore --no-save < $opts{o}/roc.cmd.r`;
