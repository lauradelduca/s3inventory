## brazil_mdic_perc_per_code.R
## Brazil MDIC 1997-2004 percentage of total weight per HS code
## Laura Del Duca


rm(list=ls(all=TRUE))

require(stringr)
require(gsubfn)
require(dplyr)
require(readxl)
require(data.table)
require(libamtrack)
require(aws.s3)

options(scipen=99999999)

setwd('C:/Users/laura.delduca/Desktop/code')
current_folder <- '0608'
script_folder <- 's3inventory/comtrade_checks'

source('R_aws.s3_credentials.R')					# load AWS S3 credentials


keys <- get_bucket_df(bucket = 'trase-storage', prefix = 'data/1-TRADE/CD/EXPORT/BRAZIL/MDIC/NCM8/')

for (yy in 1997:2004){
	
	keys <- subset(keys, grepl(yy, Key))
	f <- as.vector(keys$Key)
	
	obj <- get_object(object = f, bucket = 'trase-storage')
	data <- read.csv(text = rawToChar(obj), sep = ';', quote = '', row.names = NULL, stringsAsFactors=FALSE)
		
	data <- data.frame(lapply(data, function(x) {gsub('"', '', x)}))
	
	data <- data[,c('X.CO_NCM.', 'X.KG_LIQUIDO.')]
	
	data$X.CO_NCM. <- as.numeric(as.character(data$X.CO_NCM.))
	data$X.CO_NCM. <- AT.add.leading.zeros(data$X.CO_NCM., digits = 8)
	
	codes <- c('1201', '1208', '1507', '2304')
	data <- data[substr(data$X.CO_NCM., 1, 4) %in% codes,]

	data$X.KG_LIQUIDO. <- as.numeric(as.character(data$X.KG_LIQUIDO.))
	
	data <- aggregate(	data$X.KG_LIQUIDO., 
						by = list(data$X.CO_NCM.), 
						FUN = sum, 
						na.rm = TRUE)
	setnames(data, old = c('Group.1', 'x'), new = c('HS code', 'kg'))
	
	total_kg <- sum(data$kg)
	data$perc <- (data$kg / total_kg) * 100
	
	
	# just for testing... save a copy locally
	write.table(	data, 
					paste0(current_folder, '/', 'MDIC_perc_', yy, '.csv'), 
					quote = FALSE, 
					row.names = FALSE, 
					dec = '.', 
					sep = ';')

	# write table to S3:
	# write to an in-memory raw connection
	zz <- rawConnection(raw(0), "r+")
	write.table(data, zz, quote = FALSE, row.names = FALSE, dec = '.', sep = ';')
	# upload the object to S3
	put_object(	file = rawConnectionValue(zz), 
				bucket = 'trase-storage', 
				object = paste0('data/1-TRADE/CD/EXPORT/BRAZIL/MDIC/NCM8/perc_by_code_files/EXP_', yy, '_perc.csv') )
	# close the connection
	close(zz)
	
}
