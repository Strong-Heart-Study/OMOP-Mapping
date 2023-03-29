library(data.table)
library(dplyr)
library(ggplot2)
library(wesanderson)
# library(lessR)
# library(tidyr)
# library(plyr)

infile = "data/source.csv"
df_raw = read.table(infile, header=T, sep = ",", comment.char = "")
df_src = data.frame( df_raw[df_raw$phase %in% c("Phase123", "Phase4", "Phase5",  "Phase6"), ] )


infile = "data/source_to_concept_map.csv"
df_raw = read.table(infile, header=T, sep = ",", comment.char = "")
df_map = df_raw[df_raw$mapping_type != "unmapped",]

# factorize
df_map$SHS_domain = as.factor(df_map$SHS_domain)
df_map$vocabulary_id = as.factor(df_map$vocabulary_id)
df_map$domain_id = as.factor(df_map$domain_id)
df_map$concept_class_id = as.factor(df_map$concept_class_id)

unmapped_vbl = sort( setdiff(df_src$vble, df_map$source_code) )
length(unmapped_vbl)
sort(unmapped_vbl)
write.table(sort(unmapped_vbl), file="data/unmapped_variables.txt",
            quote=F, row.names=F, col.names=FALSE)


infile = "data/variable_value_source.csv"
df_raw = read.table(infile, header=T, sep = ",", comment.char = "")
dt_vvpair_src = data.table( df_raw[df_raw$phase %in% c("Phase123", "Phase4", "Phase5",  "Phase6"), ] )


infile = "data/variable_value_destination.csv"
df_raw = read.table(infile, header=T, sep = ",", comment.char = "")
dt_vvpair_dsn = data.table(df_raw)


# compare the number of males and females
dt_vvpair_src[vble == "SEX", .(n = sum(source_patient_count)), by = phase]

sum(dt_vvpair_src[vble == "SEX" & value == "M", source_patient_count])  # 3871
dt_vvpair_dsn[source_code == "M"] # 2787

sum(dt_vvpair_src[vble == "SEX" & value == "F", source_patient_count]) # 5717
dt_vvpair_dsn[source_code == "F", destionation.patient.count] # 3980


# source total = 5717
sum(dt_vvpair_src[vble == "SEX" & value == "M", source_patient_count]) +
  sum(dt_vvpair_src[vble == "SEX" & value == "F", source_patient_count]) 

# destination total = 6767
dt_vvpair_dsn[source_code == "M", destionation.patient.count] + 
  dt_vvpair_dsn[source_code == "F", destionation.patient.count] 




sum(df_map$target_concept_id > 2*10^7)
nrow(df_map)
sum(df_map$target_concept_id > 2*10^7) / nrow(df_map)

df_map[df_map$target_concept_id > 2*10^7,]


length(unique(df_map$concept_code))

length( unique(df_map$target_concept_id[ df_map$target_concept_id  > 2*10^7]))
29/125



mycols = c( "#E69F00", "#56B4E9", "#009E73",
            "#F0E442", "#0072B2", "#D55E00", "#CC79A7")


unmapped_vbl


# bar plot of overall mapping of variables
df_plot = data.frame( mapping_status = factor(c("yes", "no"), levels = c("yes", "no")),
                         n = c(nrow(df_map), 31))
df_plot$pct = round(100*df_plot$n/sum(df_plot$n), 1)
df_plot$display_text = c("494 (94.1%)", "31 (5.9%)")
p1 = ggplot(df_plot, aes(x = mapping_status, y = n, fill = mapping_status) ) +
  geom_bar(stat = "identity") +
  geom_text(stat="identity", 
            aes(label=display_text), vjust= -0.5, size = 10) +
  xlab("mapping status") + 
  ylab("number of variables") +
  ylim(c(0, 600)) +
  scale_fill_manual(values=mycols[1:nrow(df_plot)]) +
  theme(legend.position = "none",
        text = element_text(size=30))
ggsave(filename = "figure/piechart_mapping.pdf", plot = p1, device = "pdf")



# bar plot by  vocabulary
mytab = table(df_map$vocabulary_id)
df_plot = data.frame( vocabulary = names(mytab), n = as.numeric( mytab) )
df_plot$pct = round(100*df_plot$n/sum(df_plot$n), 1)
df_plot$display_text = paste(df_plot$n, " (", df_plot$pct, "%)", sep="")
df_plot
p1 = ggplot(df_plot, aes(x = reorder(vocabulary, n), y = n, fill = vocabulary) ) +
  geom_bar(stat = "identity") +
  geom_text(stat="identity", aes(label=display_text), hjust= -0.1, size = 5) +
  scale_fill_manual(values= mycols[1:nrow(df_plot)]) +
  xlab("vocabulary") + 
  ylab("number of mapped variables") +
  ylim(c(0, 300)) +
  theme_grey(base_size = 22) + 
  coord_flip()
p1
ggsave(filename = "figure/barplot_mapping_by_vocabulary.pdf", plot = p1, device = "pdf")

# bar plot by domain
mytab = table(df_map$domain_id)
df_plot = data.frame( domain = names(mytab), n = as.numeric( mytab) )
df_plot$pct = round(100*df_plot$n/sum(df_plot$n), 1)
df_plot$display_text = paste(df_plot$n, " (", df_plot$pct, "%)", sep="")
df_plot
p1 = ggplot(df_plot, aes(x = reorder(domain, n), y = n, fill = domain) ) +
  geom_bar(stat = "identity") +
  geom_text(stat="identity", aes(label=display_text), hjust= -0.1, size = 5) +
  scale_fill_manual(values= mycols[1:nrow(df_plot)]) +
  xlab("domain") + 
  ylab("number of mapped variables") +
  ylim(c(0, 250)) +
  theme_grey(base_size = 22) + 
  coord_flip()
p1
ggsave(filename = "figure/barplot_mapping_by_domain.pdf", plot = p1, device = "pdf")



# bar plot by concept class
mytab = table(df_map$concept_class_id)
df_plot = data.frame( concept_class = names(mytab), n = as.numeric( mytab) )
df_plot$pct = round(100*df_plot$n/sum(df_plot$n), 1)
df_plot$display_text = paste(df_plot$n, " (", df_plot$pct, "%)", sep="")
df_plot
p1 = ggplot(df_plot, aes(x = reorder(concept_class, n), y = n, fill = concept_class) ) +
  geom_bar(stat = "identity") +
  geom_text(stat="identity", aes(label=display_text), hjust= -0.1, size = 5) +
  scale_fill_manual(values= rainbow(nrow(df_plot))) +
  xlab("concept class") + 
  ylab("number of mapped variables") +
  ylim(c(0, 180)) +
  theme_grey(base_size = 22) + 
  coord_flip()
p1
ggsave(filename = "figure/barplot_mapping_by_concept_class.pdf", plot = p1, device = "pdf")


# bar plot of mapping type count
mytab = table(df_map$mapping_type)
df_plot = data.frame( mapping_type = names(mytab), n = as.numeric( mytab) )
df_plot$pct = round(100*df_plot$n/sum(df_plot$n), 1)
df_plot$display_text = paste(df_plot$n, " (", df_plot$pct, "%)", sep="")
df_plot
p1 = ggplot(df_plot, aes(x = reorder(mapping_type, n), y = n, fill = mapping_type) ) +
  geom_bar(stat = "identity") +
  geom_text(stat="identity", aes(label=display_text), hjust= -0.1, size = 5) +
  scale_fill_manual(values= mycols[1:nrow(df_plot)]) +
  xlab("mapping type") + 
  ylab("number of mapped variables") +
  ylim(c(0, 400)) +
  theme_grey(base_size = 22) + 
  coord_flip()
p1
ggsave(filename = "figure/barplot_mapping_by_mapping_type.pdf", plot = p1, device = "pdf")




# heatmap of source domain vs. destination domain
df_plot = df_map %>% count(SHS_domain, domain_id, .drop = FALSE)
colnames(df_plot) = c("source_domain", "destination_domain", "n")
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



# heatmap of mapping_type vs. destination domain
df_plot = df_map %>% count(mapping_type, domain_id, .drop = FALSE)
colnames(df_plot) = c("mapping_type", "destination_domain", "n")
df_plot
p1 = ggplot(df_plot, aes(x = destination_domain, y = mapping_type, fill = n)) +
  geom_tile(color = "black") +
  geom_text(aes(label = n), color = "black", size = 6) +
  scale_fill_gradient(low = "white", high = "brown1") +
  guides(fill = guide_colourbar(barwidth = 0.5,  barheight = 20)) +
  coord_fixed() +
  theme(axis.text.x = element_text(angle = 45, hjust=1, size = 12),
        axis.text.y = element_text(size = 12)) 
ggsave(filename = "figure/heatmap_mapping_type_to_destination_domains.pdf",
       plot = p1, device = "pdf", width = 20, height = 8)


# Conformance =  Do data values adhere to specified standards and formats?
# Completeness = Are data values present?
# Plausibility = Are data values believable?







