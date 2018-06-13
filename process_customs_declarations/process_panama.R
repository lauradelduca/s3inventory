## Preprocessing of Panama customs declarations trade data 2013 - 2017 from SICEX2.5 (codes 01-99)
## originals are read-only, ';' substituted, csv added
## Laura Del Duca


## download xlsx files, set properties to 'read only'
## open each one, check that file has no obvious errors: has data for correct year, country, rows as expected etc

## for SICEX2.5:
## enable editing, go to data sheet, replace all ';' with '.'
## save data sheet as csv, with the same name as the xlsx original

## upload both, xlsx and csv files, in an 'ORIGINALS' folder in the appropriate location on S3
## there are various possibilities for interacting with S3: through the site, command line, 'S3 browser' software, ...


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
current_folder <- '0613'
script_folder <- 's3inventory/comtrade_checks'

source('R_aws.s3_credentials.R')					# load AWS S3 credentials



for (yy in 2013:2017){
	
	orig <- get_bucket_df(	bucket = 'trase-storage', prefix = paste0('data/1-TRADE/CD/EXPORT/PANAMA/', yy))	
	keys <- subset(orig, grepl("ORIGINALS/.*.csv$", Key) )
	keys <- as.vector(keys$Key)
	
	J <- list()
	i = 1
	
	for (f in keys){
		
		obj <- get_object(object = f, bucket = 'trase-storage')
		data <- read.csv(text = rawToChar(obj), sep = ';', quote = '', row.names = NULL, stringsAsFactors=FALSE)
	
		# make sure the files look correct, and numbers of columns match, to use same names
		print(f)
		print(data[1:3,])
		print(ncol(data))
		
		# remove all empty rows: get index of all rows that have NAs across all columns and remove
		k <- which( apply(data, 1, function(x) all(is.na(x))) )
		if(length(k)>0) data<- data[-k,]
		
		# use column names of the first files, remove special characters if needed, and assign to all
		if (i==1)  nn <- names(data)
		if (i>1)   names(data) <- nn
		
		# add the data to the list
		J[[i]] <- data
		i <- i + 1
	}

	# append all data stored in list of data frames in J
	D <- do.call(rbind, J)
	
	
	# in all columns check again that ; is replaced with .
	D <- data.frame(lapply(D, function(x) {gsub(";", ".", x)}))
	
	# test that all dates are for the correct year
	print(unique(D$Year))
	# this step revealed an issue common with Bolivia:
	# open in text editor and remove quotes manually in 'INCLUSO G"' etc
	# checking unique levels of the first column is useful for detecting misplaced newline characters
	
	
	# remove commas from numeric columns
	D$TOTAL.Quantity.1 <- as.numeric(as.character(gsub(",", "", D$TOTAL.Quantity.1)))
	D$TOTAL.FOB.Value..US.. <- as.numeric(as.character(gsub(",", "", D$TOTAL.FOB.Value..US..)))
	D$FOB.per.Unit..Quantity1. <- as.numeric(as.character(gsub(",", "", D$FOB.per.Unit..Quantity1.)))
	D$TOTAL.CIF.Value..US.. <- as.numeric(as.character(gsub(",", "", D$TOTAL.CIF.Value..US..)))
	D$TOTAL.Net.Weight..Kg. <- as.numeric(as.character(gsub(",", "", D$TOTAL.Net.Weight..Kg.)))
	D$TOTAL.Gross.Weight..Kg. <- as.numeric(as.character(gsub(",", "", D$TOTAL.Gross.Weight..Kg.)))
	D$Freight <- as.numeric(as.character(gsub(",", "", D$Freight)))
	D$Insurance <- as.numeric(as.character(gsub(",", "", D$Insurance)))
	D$Commercial.Quantity <- as.numeric(as.character(gsub(",", "", D$Commercial.Quantity)))
	D$Calculated.Tax <- as.numeric(as.character(gsub(",", "", D$Calculated.Tax)))
	D$Imp.Sel.Consum <- as.numeric(as.character(gsub(",", "", D$Imp.Sel.Consum)))
	D$Icddp <- as.numeric(as.character(gsub(",", "", D$Icddp)))
	D$Imp.Import <- as.numeric(as.character(gsub(",", "", D$Imp.Import)))
	D$Imp.Total.Payment <- as.numeric(as.character(gsub(",", "", D$Imp.Total.Payment)))
	
	# make sure HS column is even number of digits, here 6
	D$Harmonized.Code.Product.English <- as.numeric(as.character(D$Harmonized.Code.Product.English))
	D$Harmonized.Code.Product.English <- AT.add.leading.zeros(D$Harmonized.Code.Product.English, digits = 6)
	# this should be 10 digits:
	D$Product.Schedule.B.Code <- as.numeric(as.character(D$Product.Schedule.B.Code))
	D$Product.Schedule.B.Code <- AT.add.leading.zeros(D$Product.Schedule.B.Code, digits = 10)
	
	
	# just for testing... save a copy locally
	write.table(	D, 
					paste0(current_folder, '/', 'CD_PANAMA_', yy, '_TEST.csv'), 
					quote = FALSE, 
					row.names = FALSE, 
					dec = '.', 
					sep = ';')

	# write table to S3:
	# write to an in-memory raw connection
	zz <- rawConnection(raw(0), "r+")
	write.table(D, zz, quote = FALSE, row.names = FALSE, dec = '.', sep = ';')
	# upload the object to S3
	put_object(	file = rawConnectionValue(zz), 
				bucket = 'trase-storage', 
				object = paste0('data/1-TRADE/CD/EXPORT/PANAMA/', yy, '/SICEX25/CD_PANAMA_', yy, '.csv') )
	# close the connection
	close(zz)
	
}

# clean up
gc()


## test new files with comtrade_check.R weight_table
## correct folder structure on aws
