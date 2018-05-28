## Preprocessing of Brazil customs declarations trade data from Datamyne, 2015-2017 dashboard, 20 - 20 third party
## check email, there is some more data to add, and third_party_separate

## Brazil has originals for dashboard 2015-2017, with read-only and csvs

## careful to check if dashboard 2015 cotton is added twice
## sum of last three files is 975148.956 tons, comtrade is 951038

## Laura Del Duca


## download xlsx files, set properties to 'read only'
## open each one, check that file has no obvious errors: has data for correct year, country, rows as expected etc
## enable editing, replace all ';' with '.': ctrl-a, ctrl-f, 'save as'
## save data sheet as csv, with the same/recognizable name as the xlsx original

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
current_folder <- '0528'
script_folder <- 's3inventory/comtrade_checks'

source('R_aws.s3_credentials.R')					# load AWS S3 credentials


## 2015 - 2017 dashboard
for (yy in 2015:2017){
	
	# load csv originals keys for all years, store in vector 'brazil_originals_YEAR_keys'
	orig <- get_bucket_df(bucket = 'trase-storage', prefix = paste0('data/1-TRADE/CD/EXPORT/BRAZIL/DATAMYNE/DASHBOARD/', yy))	
	keys <- subset(orig, grepl("ORIGINALS/.*.csv$", Key) )
	keys <- as.vector(keys$Key)
	assign(paste0('brazil_originals_', yy, '_keys'), keys)
	
	keys <- keys[1:28]
	
	# remove all " as they mess with columns
	for (f in keys){
	
		obj <- get_object(object = f, bucket = 'trase-storage')
		data <- read.csv(text = rawToChar(obj), sep = ';', quote = '', row.names = NULL, stringsAsFactors=FALSE)
		
		data <- data.frame(lapply(data, function(x) {gsub('"', '', x)}))
		
		# in all columns check again that ; is replaced with .
		data <- data.frame(lapply(data, function(x) {gsub(";", ".", x)}))
	
		for (i in 1:length(data[is.na(data$FOB.Value..US..),])){
		
			data[is.na(data$FOB.Value..US..),]$HS.Description[i] <- paste0(	data[is.na(data$FOB.Value..US..),]$HS.Description[i], 
																			'. ',
																			data[is.na(data$FOB.Value..US..),]$Country.of.Destination[i])
		
		}
	
		# write table to S3:
		# write to an in-memory raw connection
		zz <- rawConnection(raw(0), "r+")
		write.table(data, zz, quote = FALSE, row.names = FALSE, dec = '.', sep = ';')
		# upload the object to S3
		put_object(	file = rawConnectionValue(zz), 
					bucket = 'trase-storage', 
					object = paste0(f) )
		# close the connection
		close(zz)
	
	}
	
	
	# create an empty list to store the data of each file
	J <- list()
	i = 1
	
	for (f in keys){
		
		obj <- get_object(object = f, bucket = 'trase-storage')
		data <- read.csv(text = rawToChar(obj), sep = ';', quote = '', row.names = NULL, stringsAsFactors=FALSE)
		
		# delete empty columns
		data <- data[,1:12]
	
		# make sure the files look correct, and numbers of columns match, to use same names
		print(f)
		print(data[1:3,])
		print(ncol(data))
		
		# remove all empty rows: get index of all rows that have NAs across all columns and remove
		k <- which( apply(data, 1, function(x) all(is.na(x))) )
		if(length(k)>0) data<- data[-k,]
		
		# some 'trasesei' titled downloads have info at the end, and all of 2017 dashboard originals
		# remove this manually
		print(tail(data))
		
		# is sometimes factor, sometimes integer, so rbind causes NAs
		# should be converted to numeric, for fob and weight
		#data$FOB.Value..US.. <- as.numeric(as.character(data$FOB.Value..US..))
		#data$Net.Weight <- as.numeric(as.character(data$Net.Weight))
		
		print(sapply(data, class))
		## issues:
		# 2015 weights are still all NA
		# 2015, 16 cotton is too much -> go over 'trasesei' integration
		# (not a problem for 2017 as this is all newly downloaded)
		
		
		# files were downloaded separately but names match
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
	D$FOB.Value..US.. <- as.numeric(gsub(",", "", D$FOB.Value..US..))
	D$Net.Weight <- as.numeric(gsub(",", "", D$Net.Weight))	
	
	# make sure HS column is even number of digits, here 8
	D$Product.HS <- as.numeric(as.character(D$Product.HS))
	D$Product.HS <- AT.add.leading.zeros(D$Product.HS, digits = 8)
	# create 6-digit HS column from Product.HS
	D$HS6 <- substr(D$Product.HS, 1, 6)
	
	
	# just for testing... save a copy locally
	write.table(	D, 
					paste0(current_folder, '/', 'CD_BRAZIL_DASHBOARD_', yy, '_TEST_withoutcotton.csv'), 
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
				object = paste0('data/1-TRADE/CD/EXPORT/BRAZIL/DATAMYNE/DASHBOARD/', yy, '/TEST/CD_BRAZIL_', yy, '.csv') )
	# close the connection
	close(zz)
	
}


## 20 third party
for (yy in 2015:2017){
	
	# load csv originals keys for all years, store in vector 'brazil_originals_YEAR_keys'
	orig <- get_bucket_df(bucket = 'trase-storage', prefix = paste0('data/1-TRADE/CD/EXPORT/BRAZIL/DATAMYNE/DASHBOARD/', yy))	
	keys <- subset(orig, grepl("ORIGINALS/.*.csv$", Key) )
	keys <- as.vector(keys$Key)
	assign(paste0('brazil_originals_', yy, '_keys'), keys)
	
	# create an empty list to store the data of each file
	J <- list()
	i = 1
	
	for (f in keys){
		
		obj <- get_object(object = f, bucket = 'trase-storage')
		data <- read.csv(text = rawToChar(obj), sep = ';', quote = '', row.names = NULL)
		
		# delete empty columns
		data <- data[,1:12]
	
		# make sure the files look correct, and numbers of columns match, to use same names
		print(f)
		print(data[1:3,])
		print(ncol(data))
		
		# remove all empty rows: get index of all rows that have NAs across all columns and remove
		k <- which( apply(data, 1, function(x) all(is.na(x))) )
		if(length(k)>0) data<- data[-k,]
		
		# files were downloaded separatedly but names match
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
	D$FOB.Value..US.. <- as.numeric(gsub(",", "", D$FOB.Value..US..))
	D$Net.Weight <- as.numeric(gsub(",", "", D$Net.Weight))	
	
	# make sure HS column is even number of digits, here 8
	D$Product.HS <- as.numeric(as.character(D$Product.HS))
	D$Product.HS <- AT.add.leading.zeros(D$Product.HS, digits = 8)
	# create 6-digit HS column from Product.HS
	D$HS6 <- substr(D$Product.HS, 1, 6)
	
	
	# just for testing... save a copy locally
	write.table(	D, 
					paste0(current_folder, '/', 'CD_BRAZIL_DASHBOARD_', yy, '_TEST.csv'), 
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
				object = paste0('data/1-TRADE/CD/EXPORT/BRAZIL/DATAMYNE/DASHBOARD/', yy, '/TEST/CD_BRAZIL_', yy, '.csv') )
	# close the connection
	close(zz)
	
}


# clean up
gc()


## test new files with comtrade_check.R weight_table
## correct folder structure on aws
