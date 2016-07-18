otu_table <- read.table("${otu_file}",comment.char = "",sep = "\t",row.names=1, header = TRUE, check.names=FALSE)
otu_table <- t(otu_table)
# fix(otu_table)
env_factor <- read.table("${env_file}", comment.char = "",sep = "\t",row.names=1, header = TRUE, check.names=FALSE)
# fix(env_factor)
centroids <- FALSE
biplot <- FALSE
for (i in env_factor) {
    if (is.factor(i)){
        centroids <- TRUE
    } else{
        biplot <- TRUE
    }
}
library(vegan)
capscale.result<- capscale(otu_table~${env_formula},data.frame(env_factor),dist="${distance_algorithm}")
pdf('${output_dir}/db_rda.pdf')
plot_values <- plot(capscale.result)
dev.off()
write.table(plot_values$sites, '${output_dir}/db_rda_sites.xls', sep = '\t', col.names = NA, quote = FALSE)
write.table(plot_values$species, '${output_dir}/db_rda_species.xls', sep = '\t', col.names = NA, quote = FALSE)
if (centroids){
    write.table(plot_values$centroids, '${output_dir}/db_rda_centroids.xls', sep = '\t', col.names = NA, quote = FALSE)
}
if (biplot){
    write.table(plot_values$biplot * attr(plot_values$biplot,"arrow.mul"), '${output_dir}/db_rda_biplot.xls', sep = '\t', col.names = NA, quote = FALSE)
}
sink('${output_dir}/env_data.temp')
if (centroids) {
    print('centroids:TRUE')
}else{
    print('centroids:FALSE')
}
if (biplot) {
    print('biplot:TRUE')
}else{
    print('biplot:FALSE')
}
sink(NULL)
