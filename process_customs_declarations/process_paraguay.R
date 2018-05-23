## Preprocessing of Paraguay customs declarations trade data 2007 - 2018 from Ministry of Trade
## processing includes 2018 but this should not be uploaded as it is an approximation
## Laura Del Duca


## MinTrade data comes in one xlsx file with one sheet per year, varying column names/content
## set properties to 'read only'
## open each one, check that file has no obvious errors: has data for correct year, country, rows as expected etc

## enable editing, and on each sheet, replace all ';' with '.'
## save each sheet as csv, here I did PARAGUAY_YEAR.csv

## xlsx was uploaded to S3 'PARAGUAY/MINTRADE/ORIGINALS'
## each csv file in an 'ORIGINALS' folder in the appropriate 'PARAGUAY/MINTRADE/YEAR'



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
current_folder <- '0522'
script_folder <- 's3inventory/comtrade_checks'

source('R_aws.s3_credentials.R')					# load AWS S3 credentials


for (yy in 2007:2018){
	
	# load csv originals keys for all years, store in vector 'paraguay_originals_YEAR_keys'
	orig <- get_bucket_df(bucket = 'trase-storage', prefix = paste0('data/1-TRADE/CD/EXPORT/PARAGUAY/MINTRADE/', yy))	
	keys <- subset(orig, grepl("ORIGINALS/.*.csv$", Key) )
	keys <- as.vector(keys$Key)
	assign(paste0('paraguay_originals_', yy, '_keys'), keys)
	
	obj <- get_object(object = keys[1], bucket = 'trase-storage')
	data <- read.csv(text = rawToChar(obj), sep = ';', quote = '', row.names = NULL)

	# make sure the files look correct
	print(keys[1])
	print(data[1:3,])
	print(ncol(data))
	
	# remove all empty rows: get index of all rows that have NAs across all columns and remove
	k <- which( apply(data, 1, function(x) all(is.na(x))) )
	if(length(k)>0) data<- data[-k,]
	
	# remove special characters from column names, if needed
	# setting encoding of whole file to utf8: 
	# fread with encoding = 'UTF-8' option is not sufficient so correcting colnames manually
	print(names(data))	
	
	# in all columns check again that ; is replaced with .
	data <- data.frame(lapply(data, function(x) {gsub(";", ".", x)}))
	
	# remove commas from numeric columns
	data$Cantidad <- as.numeric(gsub(",", "", data$Cantidad))
	data$Kilo.Neto <- as.numeric(gsub(",", "", data$Kilo.Neto))	
	data$Valor.Fob.Dolar <- as.numeric(gsub(",", "", data$Valor.Fob.Dolar))	
	# convert from factor to character, remove '.' from NCM column, format to 11 digits
	data$NCM <- as.numeric(gsub("\\.", "", as.character(data$NCM)))
	data$NCM <- AT.add.leading.zeros(data$NCM, digits = 11)
	# create 6-digit HS column from NCM
	data$HS6 <- substr(data$NCM, 1, 6)
	
	
	# just for testing... save a copy locally
	write.table(	data, 
					paste0(current_folder, '/', 'CD_PARAGUAY_', yy, '_TEST.csv'), 
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
				object = paste0('data/1-TRADE/CD/EXPORT/PARAGUAY/MINTRADE/', yy, '/CD_PARAGUAY_', yy, '.csv') )
	# close the connection
	close(zz)
	
}


# clean up
gc()


## test new files with comtrade_check.R weight_table
## correct folder structure on aws
