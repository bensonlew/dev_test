library(pheatmap)
library(psych)
library(ape)
data=read.table("${inputfile}",header=T,row.names=1,comment.char = '',sep="\t")
#pmcorr <- pheatmap(data,cluster_rows=T,cluster_cols=T,display_numbers=T,fontsize_number=4,number_format = "%.2f")

col_pmcorr <- pheatmap(data,clustering_method="${col_cluster_method}", clustering_distance_rows="correlation",clustering_distance_cols="correlation",silent=TRUE,cluster_rows=T,cluster_cols=T,display_numbers=T,fontsize_number=4,number_format = "%.2f")
row_pmcorr <- pheatmap(data,clustering_method="${row_cluster_method}", clustering_distance_rows="correlation",clustering_distance_cols="correlation",silent=TRUE,cluster_rows=T,cluster_cols=T,display_numbers=T,fontsize_number=4,number_format = "%.2f")

corr_tre_col <- as.phylo(col_pmcorr$tree_col)
corr_tre_row <- as.phylo(row_pmcorr$tree_row)
write.tree(corr_tre_col, "${col_tree}")
write.tree(corr_tre_row, "${row_tree}")