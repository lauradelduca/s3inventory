## Preprocessing of Brazil CDs from Datamyne 2015-2017 dashboard
## Laura Del Duca


# Download ---------------------------------------------------------------------------------------

# download xlsx files, set properties to 'read only'
# open each one, check that file has no obvious errors: has data for correct year, country, rows as expected etc
# enable editing, replace all ';' with '.': ctrl-a, ctrl-f, 'save as'
# save data sheet as csv, with the same/recognizable name as the xlsx original

# need to check which row contains column names and in which row the actual data starts
# as this really is different for each file it is not efficient to automate:
# delete rows above header manually

# issue specific to some Brazil Datamyne Dashboard 2015 files:
# some HS description (940 rows) contain ; that fall through all usual checks and end up splitting the column in two
# after locating the errors it was quicker to correct manually (substituted '.' for ';' in the HS description)
# the files affected are:
# "data/1-TRADE/CD/EXPORT/BRAZIL/DATAMYNE/DASHBOARD/2015/ORIGINALS/MASTER_2015_100001_105000.csv"
# "data/1-TRADE/CD/EXPORT/BRAZIL/DATAMYNE/DASHBOARD/2015/ORIGINALS/MASTER_2015_110001_115000.csv"
# "data/1-TRADE/CD/EXPORT/BRAZIL/DATAMYNE/DASHBOARD/2015/ORIGINALS/MASTER_2015_130001_135000.csv"
# "data/1-TRADE/CD/EXPORT/BRAZIL/DATAMYNE/DASHBOARD/2015/ORIGINALS/MASTER_2015_50001_55000.csv"
# "data/1-TRADE/CD/EXPORT/BRAZIL/DATAMYNE/DASHBOARD/2015/ORIGINALS/MASTER_2015_5001_10000.csv"
# "data/1-TRADE/CD/EXPORT/BRAZIL/DATAMYNE/DASHBOARD/2015/ORIGINALS/MASTER_2015_80001_85000.csv"
# one file has one row with net weight NA, removing it for now:
# "data/1-TRADE/CD/EXPORT/BRAZIL/DATAMYNE/DASHBOARD/2015/ORIGINALS/MASTER_2015_25001_30000.csv"
# 11/1/2015;2062100;TONGUES OF BOVINE ANIMALS, EDIBLE, FROZEN;RUSSIA;SANTOS;75860;MATABOI ALIMENTOS S.A.;MINAS GERAIS-MG;ARAGUARI;MARITIMA;NA;1.68201E+13;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA;NA

# upload both, xlsx and csv files, in an 'ORIGINALS' folder in the appropriate location on S3


# ------------------------------------------------------------------------------------------------

rm(list=ls(all=TRUE))

require(stringr)
require(gsubfn)
require(dplyr)
require(readxl)
require(data.table)
require(libamtrack)
require(aws.s3)
require(aws.signature)

options(scipen=99999999)


# get aws credentials ----------------------------------------------------------------------------
use_credentials()


for (yy in 2015:2017){
	
	orig <- get_bucket_df(bucket = 'trase-storage', prefix = paste0('data/1-TRADE/CD/EXPORT/BRAZIL/DATAMYNE/DASHBOARD/', yy))	
	keys <- subset(orig, grepl("ORIGINALS/.*.csv$", Key) )
	keys <- as.vector(keys$Key)


	# remove all " as they mess with columns, and check for ; again
	for (f in keys){
	
		obj <- get_object(object = f, bucket = 'trase-storage')
		data <- read.csv(text = rawToChar(obj), sep = ';', quote = '', row.names = NULL, stringsAsFactors=FALSE)
		
		data <- data.frame(lapply(data, function(x) {gsub('"', '', x)}))
		
		# in all columns check again that ; is replaced with .
		data <- data.frame(lapply(data, function(x) {gsub(";", ".", x)}))
	
		# write table to S3:
		zz <- rawConnection(raw(0), "r+")
		write.table(data, zz, quote = FALSE, row.names = FALSE, dec = '.', sep = ';')
		put_object(	file = rawConnectionValue(zz), 
					bucket = 'trase-storage', 
					object = paste0(f) )
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
	
	
	# 2015, 2016: keys with ''brazil201.cotton'' contain updated cotton data, D needs to be updated
	# (not 2017 as this is all newly downloaded)
	
	if ((yy == 2015) | (yy == 2016)){
	
		# combine all cotton files into one (keys that have 'brazil201.cotton')
		cottonkeys <- keys[grepl('brazil201.cotton', keys)]
		
		K <- list()									# create an empty list to store the data of each file
		p = 1
		
		for (f in cottonkeys){
		
			obj <- get_object(object = f, bucket = 'trase-storage')
			data <- read.csv(text = rawToChar(obj), sep = ';', quote = '', row.names = NULL, stringsAsFactors=FALSE)
		
			data <- data[,1:12]											# delete empty columns
		
			k <- which( apply(data, 1, function(x) all(is.na(x))) )		# remove all empty rows
			if(length(k)>0) data<- data[-k,]
		
			names(data) <- names(D)										# use column names of D
		
			K[[p]] <- data												# add the data to the list
			p <- p + 1
		}

		cottondata <- do.call(rbind, K)				# append all data stored in list of data frames in K
	
		# delete all rows where Product.HS is in the vector of Product.HS from cotton file
		cottoncodes <- as.vector(unique(as.numeric(cottondata$Product.HS)))
		D <- D[!(as.numeric(D$Product.HS) %in% cottoncodes),]
		
		# rbind D and cotton file
		D <- rbind(D, cottondata)
	
	}
	
	
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
	
	
	
	# chose only pork data for this test run ------------------------------------
	
	obj <- get_object(object = 'data/1-TRADE/commodity_equivalents_final.csv', bucket = 'trase-storage')
	hs <- read.csv(text = rawToChar(obj), sep = ';', quote = '',
	               colClasses = c("character", "character", "character", "character", "character", 
	                              "numeric", "character", "character"))
	
	hs6 <- as.vector(as.numeric(hs$code_value[hs$com_name == 'PORK']))
	hs6 <- AT.add.leading.zeros(hs6, digits = 6)
	        
	D <- D[D$HS6 %in% hs6,]
	
	
	# Format dates and include year column --------------------------------------
	
	D$date <- strptime(as.character(D$'Date..Month.'), "%d/%m/%Y")
	D$date <- format(D$date, "%Y-%m-%d")
	
	D$year <- format(D$date, format="%Y")
	D$year <- format(as.Date(D$date, format="%Y-%m-%d"),"%Y")
	
	
	
	# write table to S3 ---------------------------------------------------------
	zz <- rawConnection(raw(0), "r+")
	write.table(D, zz, quote = FALSE, row.names = FALSE, dec = '.', sep = '|;|')
	put_object(file = rawConnectionValue(zz), 
	           bucket = 'trase-storage', 
	           object = paste0('data/1-TRADE/CD/EXPORT/BRAZIL/DATAMYNE/DASHBOARD/', yy, '/PORK/CD_BRAZIL_PORK_', yy, '.csv'))
	close(zz)
	
}


# clean up ---------------------------------------------------------------------------
gc()

