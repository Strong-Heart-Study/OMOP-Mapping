library(data.table)
library(dplyr)
library(egg)
library(ggplot2)
library(stringr)
library(wesanderson)
# library(lessR)
# library(tidyr)
# library(plyr)



# set plot colors
mycols = c( "#E69F00", "#56B4E9", "#009E73",
            "#F0E442", "#0072B2", "#D55E00", "#CC79A7")



#-----------------------------------------------------------------------------------------------
# LOAD DATA
#-----------------------------------------------------------------------------------------------
infile = "data/variable_value_source.csv"
df_raw = read.table(infile, header=T, sep = ",", comment.char = "")
dt_vvpair_src = data.table( df_raw[df_raw$phase %in% c("Phase123", "Phase4", "Phase5",  "Phase6"), ] )


infile = "data/variable_value_destination.csv"
df_raw = read.table(infile, header=T, sep = ",", comment.char = "")
dt_vvpair_dsn = data.table(df_raw)


infile = "data/source.csv"
df_raw = read.table(infile, header=T, sep = ",", comment.char = "")
df_src = data.frame( df_raw[df_raw$phase %in% c("Phase123", "Phase4", "Phase5",  "Phase6"), ] )

infile = "data/source_to_concept_map_annotated.csv"
df_raw = read.table(infile, header=T, sep = ",", comment.char = "")
df_map = df_raw[df_raw$mapping_type != "unmapped",]

# factorize
tmp = as.character(df_map$SHS_domain)
table(tmp)
tmp = str_replace(tmp, 'Abs', 'Abstraction')
tmp = str_replace(tmp, 'MM', 'Mortality Morbidity ')
table(tmp)
df_map$SHS_domain = as.factor(tmp)
df_map$vocabulary_id = as.factor(df_map$vocabulary_id)
df_map$domain_id = as.factor(df_map$domain_id)
df_map$concept_class_id = as.factor(df_map$concept_class_id)

unmapped_vbl = sort( setdiff(df_src$vble, df_map$source_code) )
length(unmapped_vbl)
#sort(unmapped_vbl)
#write.table(sort(unmapped_vbl), file="data/unmapped_variables.txt",  quote=F, row.names=F, col.names=FALSE)





#-----------------------------------------------------------------------------------------------
# heatmap of mapping_type vs. destination domain
df_plot = df_map %>% count(SHS_domain, concept_class_id, .drop = FALSE)
colnames(df_plot) = c("source_domain", "destination_concept_class", "n")
df_plot
tmp = df_plot$destination_concept_class
tmp = str_replace(tmp, 'SHS', 'SHS custom')
df_plot$destination_concept_class = factor(tmp)
p1 = ggplot(df_plot, aes(x = destination_concept_class, y = source_domain, fill = n)) +
  geom_tile(color = "black") +
  geom_text(aes(label = n), color = "black", size = 6) +
  scale_fill_gradient(low = "white", high = "brown1") +
  guides(fill = guide_colourbar(barwidth = 0.5,  barheight = 10)) +
  coord_fixed() +
  theme(axis.text.x = element_text(angle = 30, hjust=1, size = 8),
        axis.text.y = element_text(size = 8)) +
  ylab("Source domain") +
  xlab("Destination concept class")
p1
ggsave(filename = "figure/heatmap_SHS_domain_vs_concept_class_id.pdf",
       plot = p1, device = "pdf", width = 12, height = 8)





# bar plot of mapping type count
mytab = table(df_map$mapping_type)
df_plot = data.frame( mapping_type = names(mytab), n = as.numeric( mytab) )
df_plot$pct = round(100*df_plot$n/sum(df_plot$n), 1)
df_plot$display_text = paste(df_plot$n, " (", df_plot$pct, "%)", sep="")
df_plot
p1 = ggplot(df_plot, aes(x = reorder(mapping_type, n), y = n, fill = mapping_type) ) +
  geom_bar(stat = "identity", fill=color_barplot_fill) +
  geom_text(stat="identity", aes(label=display_text), hjust= -0.05, size = sz_barplot_value) +
  #scale_fill_manual(values= mycols[1:nrow(df_plot)]) +
  theme(legend.position="none") +
  # , axis.text.x = element_text(angle = 45, hjust=1)) +
  xlab("mapping type") + 
  ylab("number of mapped variables") +
  ylim(c(0, 360)) +
  coord_flip()
p1
ggsave(filename = "figure/barplot_by_mapping_type.pdf",
       plot = p1, device = "pdf", width = 10, height = 8)








#-----------------------------------------------------------------------------------------------
# bar plot of overall mapping of variables
df_plot = data.frame( mapping_status = factor(c("yes", "no"), levels = c("yes", "no")),
                      n = c(nrow(df_map), 10))
df_plot$pct = round(100*df_plot$n/sum(df_plot$n), 1)
df_plot
df_plot$display_text = c("494 (98 %)", "10 (2 %)")

p1 = ggplot(df_plot, aes(x = mapping_status, y = n, fill = mapping_status) ) +
  geom_bar(stat = "identity") +
  geom_text(stat="identity", 
            aes(label=display_text), vjust= -0.5, size = 5) +
  xlab("mapping status") + 
  ylab("number of variables") +
  ylim(c(0, 600)) +
  scale_fill_manual(values=mycols[1:nrow(df_plot)]) +
  theme(legend.position = "none",
        text = element_text(size=20))
ggsave(filename = "figure/barplot_mapping_rate.pdf", plot = p1, device = "pdf")



##-----------------------------------------------------------------------------------
## plot mapped variables
##-----------------------------------------------------------------------------------

sz_barplot_value = 3
color_barplot_fill = "orange"
my_ymax = 350

# bar plot by domain
mytab = table(df_map$domain_id)
df_plot = data.frame( domain = names(mytab), n = as.numeric( mytab) )
df_plot$pct = round(100*df_plot$n/sum(df_plot$n), 1)
df_plot$display_text = paste(df_plot$n, " (", df_plot$pct, "%)", sep="")
df_plot
p1 = ggplot(df_plot, aes(x = reorder(domain, n), y = n, fill = domain) ) +
  geom_bar(stat = "identity", fill=color_barplot_fill) +
  geom_text(stat="identity", aes(label=display_text), hjust= -0.2, size = sz_barplot_value) +
  #scale_fill_manual(values= mycols[1:nrow(df_plot)]) +
  theme(legend.position="none") +
  xlab("domain") + 
  ylab("number of mapped variables") +
  ylim(c(0, my_ymax)) +
  coord_flip()
p1
# ggsave(filename = "figure/barplot_mapping_by_domain.pdf", plot = p1, device = "pdf")


# bar plot by concept class
mytab = table(df_map$concept_class_id)
df_plot = data.frame( concept_class = names(mytab), n = as.numeric( mytab) )
df_plot$pct = round(100*df_plot$n/sum(df_plot$n), 1)
df_plot$display_text = paste(df_plot$n, " (", df_plot$pct, "%)", sep="")
df_plot
p2 = ggplot(df_plot, aes(x = reorder(concept_class, n), y = n, fill = concept_class) ) +
  geom_bar(stat = "identity", fill=color_barplot_fill) +
  geom_text(stat="identity", aes(label=display_text), hjust= -0.2, size = sz_barplot_value) +
 # scale_fill_manual(values= rainbow(nrow(df_plot))) +
  theme(legend.position="none") + 
  # , axis.text.x = element_text(angle = 45, hjust=1)) +
  xlab("concept class") + 
  ylab("number of mapped variables") +
  ylim(c(0, my_ymax)) +
  coord_flip()
p2
#ggsave(filename = "figure/barplot_mapping_by_concept_class.pdf", plot = p1, device = "pdf")



# bar plot by  vocabulary
mytab = table(df_map$vocabulary_id)
df_plot = data.frame( vocabulary = names(mytab), n = as.numeric( mytab) )
df_plot$pct = round(100*df_plot$n/sum(df_plot$n), 1)
df_plot$display_text = paste(df_plot$n, " (", df_plot$pct, "%)", sep="")
df_plot
p3 = ggplot(df_plot, aes(x = reorder(vocabulary, n), y = n, fill = vocabulary) ) +
  geom_bar(stat = "identity", fill=color_barplot_fill) +
  geom_text(stat="identity", aes(label=display_text), hjust= -0.2, size = sz_barplot_value) +
  #scale_fill_manual(values= mycols[1:nrow(df_plot)]) +
  theme(legend.position="none") +
  xlab("vocabulary") + 
  ylab("number of mapped variables") +
  ylim(c(0, my_ymax)) +
  coord_flip()
p3
# ggsave(filename = "figure/barplot_mapping_by_vocabulary.pdf", plot = p1, device = "pdf")



# bar plot of mapping type count
mytab = table(df_map$mapping_type)
df_plot = data.frame( mapping_type = names(mytab), n = as.numeric( mytab) )
df_plot$pct = round(100*df_plot$n/sum(df_plot$n), 1)
df_plot$display_text = paste(df_plot$n, " (", df_plot$pct, "%)", sep="")
df_plot
p4 = ggplot(df_plot, aes(x = reorder(mapping_type, n), y = n, fill = mapping_type) ) +
  geom_bar(stat = "identity", fill=color_barplot_fill) +
   geom_text(stat="identity", aes(label=display_text), hjust= -0.2, size = sz_barplot_value) +
  #scale_fill_manual(values= mycols[1:nrow(df_plot)]) +
  theme(legend.position="none") +
  # , axis.text.x = element_text(angle = 45, hjust=1)) +
  xlab("mapping type") + 
  ylab("number of mapped variables") +
  ylim(c(0, my_ymax)) +
  coord_flip()
p4

p0 = grid.arrange(p1, p2, p3, p4, nrow = 2)



ggsave(filename = "figure/barplot_in_grid_mapped_variables.pdf", 
       width = 16, height = 10,
       plot = p0, device = "pdf")



##-------------------------------------------------------------------------------------
# heatmap of source domain vs. destination domain
df_plot = df_map %>% count(SHS_domain, domain_id, .drop = FALSE)
colnames(df_plot) = c("source_domain", "destination_domain", "n")
df_plot
p1 = ggplot(df_plot, aes(x = destination_domain, y = source_domain, fill = n)) +
  geom_tile(color = "black") +
  geom_text(aes(label = n), color = "black", size = 8) +
  scale_fill_gradient(low = "white", high = "brown1") +
  guides(fill = guide_colourbar(barwidth = 0.5,  barheight = 20)) +
  coord_fixed() +
  theme_bw(base_size = 16)
p1
ggsave(filename = "figure/heatmap_source_to_destination_domains.pdf",
       plot = p1, device = "pdf")





# Conformance =  Do data values adhere to specified standards and formats?
# Completeness = Are data values present?
# Plausibility = Are data values believable?







