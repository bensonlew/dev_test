method<-'${method}'
distance_method <- "'${distance_method}'"
input_matrix<-"${input_matrix}"
lognorm<-${lognorm}
cltype<-"${cltype}" #### both row column none

library(cluster)
library(mclust)
library(Biobase)
library(fpc)
library(ape)


k<-0

data = read.delim(input_matrix, header=T, check.names=F, sep="\t")
rownames(data) = data[,1] # set rownames to gene identifiers
data = data[,2:length(data[1,])] # remove the gene column since its now the rowname value
data = as.matrix(data) # convert to matrix
colnames(data)<-colnames(data)

if(nrow(data)>3333){data<-data[c(1:3333),]}

gc()

if ((method == "cor_heatmap") || (method == "all")){
    dir.create("analysis_cor_heatmap")

if(lognorm!=0){
    data = log(data+1,lognorm)
    final_data = t(scale(t(data))) # center and scale rows
    hc_genes = agnes(final_data, diss=FALSE, metric=distance_method) # cluster genes
	hc_cor_sample = cor(final_data, method="spearman" )
    hc_samples = hclust(as.dist(1-cor(final_data, method="spearman")), method="complete") # cluster conditions
	write.table(hc_cor_sample,"analysis_cor_heatmap/cor_heatmap.xls",sep="\t",col.names=T,row.names=T,quote=F)
}
if(lognorm==0){
    final_data = t(scale(t(data)))
    hc_genes = agnes(final_data,diss=FALSE, metric=distance_method) # cluster genes
	hc_cor_sample = cor(final_data, method="spearman" )
    hc_samples = hclust(as.dist(1-cor(final_data, method="spearman")), method="complete") # cluster conditions
	write.table(hc_cor_sample,"analysis_cor_heatmap/cor_heatmap.xls",sep="\t",col.names=T,row.names=T,quote=F)
}
}


if(k==0){
     pamk <- pamk(final_data,diss=FALSE)
     k <- pamk$nc
 }

if ((method == "tree") || (method == "all")){
    dir.create("analysis_tree")

    #genes_tree <- as.phylo(as.hclust(hc_genes))
    samples_tree <- as.phylo(hc_samples)
    #write.tree(genes_tree,"analysis_tree/genes_tree.txt")
    write.tree(samples_tree,"analysis_tree/samples_tree.txt")
	write.table(final_data,"analysis_tree/all_heatmap.xls",sep="\t",col.names=T,row.names=T,quote=F)

}

gc()

if((method == "PCA")||(method == "all")){
	dir.create("analysis_PCA")
	
	data_pca <- final_data
	
	pca_analysis <-prcomp(data_pca, scal = FALSE)
	
	pca_sites <- pca_analysis$x
	
	pca_summary<-summary(pca_analysis)
	pca_importance <- pca_summary$importance[2, ]
    pca_rotation <- pca_summary$rotation
   
    

	write.table(pca_rotation,"analysis_PCA/pca_rotation.xls",sep="\t",col.names=T,row.names=T,quote=F)
	


	
}
