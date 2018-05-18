## Preprocessing of Ecuador customs declarations trade data 2013 - 2017 from SICEX2.0
## Laura Del Duca


## download xlsx files
## open each one, check that file has no obvious errors: has data for correct year, country, rows as expected etc

## for SICEX2.5:
## enable editing, go to data sheet, replace all ';' with '.'
## save data sheet as csv, with the same name as the xlsx original

## for SICEX2.0:
## enable editing, replace all ';' with '.'
## save as csv, with the same name as the xlsx original
## need to check which row contains column names and in which row the actual data starts
## as this really is different for each file it is not efficient to automate:
## delete rows above header manually

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
current_folder <- '0518'
script_folder <- 's3inventory/comtrade_checks'

source('R_aws.s3_credentials.R')					# load AWS S3 credentials



for (yy in 2013:2017){
	
	# load csv originals keys for all years, store in vector 'ecuador_originals_YEAR_keys'
	orig <- get_bucket_df(	bucket = 'trase-storage', prefix = paste0('data/1-TRADE/CD/EXPORT/ECUADOR/', yy))	
	keys <- subset(orig, grepl("ORIGINALS/.*.csv$", Key) )
	keys <- as.vector(keys$Key)
	assign(paste0('ecuador_originals_', yy, '_keys'), keys)
	
	# create an empty list to store the data of each file
	J <- list()
	i = 1
	
	for (f in keys){
		
		obj <- get_object(object = f, bucket = 'trase-storage')
		data <- read.csv(text = rawToChar(obj), sep = ';', quote = '', row.names = NULL)
	
		# make sure the files look correct, and numbers of columns match, to use same names
		print(f)
		print(data[1:3,])
		print(ncol(data))
		
		# remove all empty rows: get index of all rows that have NAs across all columns and remove
		k <- which( apply(data, 1, function(x) all(is.na(x))) )
		if(length(k)>0) data<- data[-k,]
		
		# use column names of the first files, remove special characters if needed, and assign to all
		# setting encoding of whole file to utf8: 
		# fread with encoding = 'UTF-8' option is not sufficient so correcting colnames manually
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
	
	# remove commas from numeric columns
	D$TOTAL.Quantity.1 <- as.numeric(gsub(",", "", D$TOTAL.Quantity.1))
	D$TOTAL.FOB.Value.US <- as.numeric(gsub(",", "", D$TOTAL.FOB.Value.US))
	D$FOB.per.Unit.Quantity1 <- as.numeric(gsub(",", "", D$FOB.per.Unit.Quantity1))
	D$TOTAL.CIF.Value.US <- as.numeric(gsub(",", "", D$TOTAL.CIF.Value.US))
	D$TOTAL.Net.Weight.Kg <- as.numeric(gsub(",", "", D$TOTAL.Net.Weight.Kg))
	D$TOTAL.Gross.Weight.Kg <- as.numeric(gsub(",", "", D$TOTAL.Gross.Weight.Kg))
	
	# make sure HS column is even number of digits, here 6
	D$Harmonized.CodeProduct.Spanish <- as.numeric(as.character(D$Harmonized.CodeProduct.Spanish))
	D$Harmonized.CodeProduct.Spanish <- AT.add.leading.zeros(D$Harmonized.CodeProduct.Spanish, digits = 6)
	# this should be 10 digits:
	D$Product.Schedule.B.Code <- as.numeric(as.character(D$Product.Schedule.B.Code))
	D$Product.Schedule.B.Code <- AT.add.leading.zeros(D$Product.Schedule.B.Code, digits = 10)
	
	
	# just for testing... save a copy locally
	write.table(	D, 
					paste0(current_folder, '/', 'CD_ECUADOR_', yy, '_TEST.csv'), 
					quote = FALSE, 
					row.names = FALSE, 
					dec = '.', 
					sep = ';')

	# write table to S3:
	# write to an in-memory raw connection
	zz <- rawConnection(raw(0), "r+")
	# can use write.table? to keep dec and sep...
	write.table(D, zz, quote = FALSE, row.names = FALSE, dec = '.', sep = ';')
	# upload the object to S3
	put_object(	file = rawConnectionValue(zz), 
				bucket = 'trase-storage', 
				prefix = paste0('data/1-TRADE/CD/EXPORT/ECUADOR/', yy, 'SICEX20'),
				object = paste0('CD_ECUADOR_', yy, '_TEST.csv') )
	# close the connection
	close(zz)

}

# clean up
gc()
