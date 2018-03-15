# get list of datasets with filenames and paths from metadata.json

library(data.table)
library(PCS)

meta <- getMeta()

d <- NULL

datasets <- meta$handle

for (dataset in datasets){
	meta_entry <- getMeta(handle = dataset)
	t <- meta_entry$title
	p <- meta_entry$raw_location
	for (ff in meta_entry$files){
		f <- paste("", ff$filename, sep = "")
		d <- rbind(d, data.frame(t, p, f))
	}
}

write.csv2(d, 'filelist_reorganization.csv', quote=FALSE, row.names=FALSE)