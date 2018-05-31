## Preprocessing of Argentina customs declarations trade data 2013 - 2017 from SICEX2.5
## maybe add 2010 - 2012

## Laura Del Duca

## Weight is defined by the field "Cantidad Estadstica"
## We have detected mistakes in weight values, given that

## i) units change and are not always kilograms
## ii) sometimes the reporters report tones instead of kilograms. To detect these cases divide FOB values and weight, and see which records have "wrong" prices per kilogram (values about 1000 times larger/smaller than the majority of the records)


## download xlsx files, set properties to 'read only'
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
current_folder <- '0531'
script_folder <- 's3inventory/comtrade_checks'

source('R_aws.s3_credentials.R')					# load AWS S3 credentials


## 2013 - 2017 SICEX2.5
for (yy in 2013:2017){
	
	# load csv originals keys for all years
	orig <- get_bucket_df(bucket = 'trase-storage', prefix = paste0('data/1-TRADE/CD/EXPORT/ARGENTINA/', yy))	
	keys <- subset(orig, grepl("ORIGINALS/.*.csv$", Key) )
	keys <- as.vector(keys$Key)
	
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
		if (i==1){
			setnames(	data,
						old = c('Cantidad.Estadística', 'Unidad.Estadística'), 
						new = c('Cantidad.Estadistica', 'Unidad.Estadistica'))
			nn <- names(data)
		}
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
	D$Cantidad.Estadistica <- as.numeric(gsub(",", "", D$Cantidad.Estadistica))
	D$TOTAL.FOB.Value..US.. <- as.numeric(gsub(",", "", D$TOTAL.FOB.Value..US..))
	D$FOB.per.Unit..Quantity1. <- as.numeric(gsub(",", "", D$FOB.per.Unit..Quantity1.))
	D$TOTAL.CIF.Value..US.. <- as.numeric(gsub(",", "", D$TOTAL.CIF.Value..US..))
	D$Freight <- as.numeric(gsub(",", "", D$Freight))
	
	# make sure HS column is even number of digits, here 6
	D$Harmonized.Code.Product.English <- as.numeric(as.character(D$Harmonized.Code.Product.English))
	D$Harmonized.Code.Product.English <- AT.add.leading.zeros(D$Harmonized.Code.Product.English, digits = 6)
	# this should be 10 digits:
	D$Product.Schedule.B.Code <- as.numeric(as.character(D$Product.Schedule.B.Code))
	D$Product.Schedule.B.Code <- AT.add.leading.zeros(D$Product.Schedule.B.Code, digits = 10)

	
	# just for testing... save a copy locally
	write.table(	D, 
					paste0(current_folder, '/', 'CD_ARGENTINA_', yy, '_TEST.csv'), 
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
				object = paste0('data/1-TRADE/CD/EXPORT/ARGENTINA/', yy, '/SICEX25/TEST/CD_ARGENTINA_', yy, '.csv') )
	# close the connection
	close(zz)
	
}


# clean up
gc()


## test new files with comtrade_check.R weight_table
## correct folder structure on aws



### correcting Argentina files:

## Weight is defined by the field "Cantidad Estadstica"
## We have detected mistakes in weight values, given that

## i) units change and are not always kilograms
## ii) sometimes the reporters report tones instead of kilograms. To detect these cases divide FOB values and weight, and see which records have "wrong" prices per kilogram (values about 1000 times larger/smaller than the majority of the records)


f <- 'data/1-TRADE/CD/EXPORT/ARGENTINA/2013/SICEX25/CD_ARGENTINA_2013.csv'

obj <- get_object(object = f, bucket = 'trase-storage')
data <- read.csv(text = rawToChar(obj), sep = ';', quote = '', row.names = NULL)

head(data)
# HS code columns get imported as numerics without leading zeros

# # check for HS code type and leading zeros
# class(data$Harmonized.Code.Product.English)		# integer
# data[30:50,]
# class(data$Product.Schedule.B.Code)				# numeric

# create new column, 'fob_per_kg' (divide FOB values and weight)
data$fob_per_kg <- data$TOTAL.FOB.Value..US.. / data$Cantidad.Estadistica

head(data)

# sort by HS code, then by fob_per_kg
data <- arrange(data, data$Product.Schedule.B.Code, data$fob_per_kg)

data[1:20,]


# look at data for each commodity separately

# load relevant HS6 codes from commodity dictionary
# this loads codes (as vector) for beef, chicken, corn, cotton, leather, timber, woodpulp, shrimps, soy, sugarcane
# codes are loaded as type numeric without leading zeros, like columns in data
source(paste0(script_folder, '/', 'get_hs_codes.R'))

data_beef <- data[as.numeric(data$Harmonized.Code.Product.English) %in% beef,]
data_chicken <- data[as.numeric(data$Harmonized.Code.Product.English) %in% chicken,]
data_corn <- data[as.numeric(data$Harmonized.Code.Product.English) %in% corn,]
data_cotton <- data[as.numeric(data$Harmonized.Code.Product.English) %in% cotton,]
data_leather <- data[as.numeric(data$Harmonized.Code.Product.English) %in% leather,]
data_timber <- data[as.numeric(data$Harmonized.Code.Product.English) %in% timber,]
data_woodpulp <- data[as.numeric(data$Harmonized.Code.Product.English) %in% woodpulp,]
data_shrimps <- data[as.numeric(data$Harmonized.Code.Product.English) %in% shrimps,]
data_soy <- data[as.numeric(data$Harmonized.Code.Product.English) %in% soy,]
data_sugarcane <- data[as.numeric(data$Harmonized.Code.Product.English) %in% sugarcane,]

dim(data_beef)			# [1] 124998     32
dim(data_chicken)		# [1] 9202   32
dim(data_corn)			# [1] 10687    32
dim(data_cotton)		# [1] 1078   32
dim(data_leather)		# [1] 16725    32
dim(data_timber)		# [1] 13 32
dim(data_woodpulp)		# [1] 1753   32
dim(data_shrimps)		# [1] 7180   32
dim(data_soy)			# [1] 8965   32
dim(data_sugarcane)		# [1] 1132   32



# commodity		/2013/SICEX25/CD_ARGENTINA_2013.csv tons	comtrade_tons_2013	trase_per_comtrade
# BEEF			647287.0704		269894.987		2.398292305
# CHICKEN		398193.4621		367782.465		1.082687458		ok
# CORN			24093021.7		20200819.74		1.192675447		ok
# COTTON		98872.31381		83896.074		1.178509424		ok
# LEATHER		184181.849		112433.086		1.638146346
# TIMBER		0.07201			54.875			0.001312255		ok, to update in processing
# WOOD PULP		185283.577		196532.084		0.942765035		ok
# SHRIMPS		198703.7861		91288.683		2.176653004
# SOYBEANS		39459696.57		34121384.96		1.156450613		ok
# SUGAR CANE	318520.9519		281996.329		1.129521626		ok


# check min/maxes for each hs code
# order to check: timber, shrimps, leather, beef


### data_timber

# sort by HS code, then by fob_per_kg
data_timber <- arrange(data_timber, data_timber$Product.Schedule.B.Code, data_timber$fob_per_kg)
data_timber

# does not seem to be tons misreported as kg
# but except for first row, units are 'metros cubicos'
# conversion of 'metros cubicos' to kg?
# suspect pattern of mostly metros cubicos unit across the years, low comtrade ratios are a pattern

# try conversion factor of 0.7 from cubic meter to ton, or 700 from cubic meter to kg (for 440729)
# 1 cubic meter is 0.7 tons, so 1 cubic meter is 700 kg

data_timber$conversion_700 <- data_timber$Cantidad.Estadistica
data_timber$conversion_700 <- data_timber$conversion_700 * 700

sum(data_timber$conversion_700) - 161 + 0.23		# sums to 50246.23 kg, comtrade reports 54.875 tons, ok


# check result with 2014: no timber codes, 0 for comtrade as well
# compare with 2015

data_2013 <- data
f <- 'data/1-TRADE/CD/EXPORT/ARGENTINA/2015/SICEX25/CD_ARGENTINA_2015.csv'
obj <- get_object(object = f, bucket = 'trase-storage')
data <- read.csv(text = rawToChar(obj), sep = ';', quote = '', row.names = NULL)
data_2015 <- data

data_timber <- data[as.numeric(data$Harmonized.Code.Product.English) %in% timber,]
dim(data_timber)	# [1]  3 31
data_timber <- arrange(data_timber, data_timber$Product.Schedule.B.Code)
data_timber

data_timber$conversion_700 <- data_timber$Cantidad.Estadistica
data_timber$conversion_700 <- data_timber$conversion_700 * 700
sum(data_timber$conversion_700) - 20587 + 29.41		# sums to 5125.41 kg, comtrade reports 4.29 tons, ok


## data_timber result:
## for data, if code is in timber (440729?) and Unidad.Estadistica == 'METROS CUBICOS'
## weight in kg is Cantidad.Estadistica * 700



### data_shrimps (that's for 2015 actually...)

# sort by HS code, then by fob_per_kg
data_shrimps <- arrange(data_shrimps, data_shrimps$Product.Schedule.B.Code, data_shrimps$fob_per_kg)

data_shrimps[,c('Product.Schedule.B.Code', 
				'Measure.Unit.1..Quantity.1.', 
				'Cantidad.Estadistica', 
				'Unidad.Estadistica', 
				'fob_per_kg')][1:60,]

data_shrimps[,c('Product.Schedule.B.Code', 
				'Measure.Unit.1..Quantity.1.', 
				'TOTAL.Quantity.1',  
				'Cantidad.Estadistica', 
				'Unidad.Estadistica', 
				'fob_per_kg')][data_shrimps$'Unidad.Estadistica' != 'KILOGRAMOS',][1:60,]
				
				
unique(data_shrimps$Unidad.Estadistica)		# [1] KILOGRAMOS  DESCONOCIDA

# maybe where weight column is 0 or unidad estadistica is deconocida
# should use total.quantity.1 column instead

# note: where Unidad.Estadistica == 'DESCONOCIDA', Measure.Unit.1..Quantity.1. == 'KILOGRAMOS'

# test: 
sum(data_shrimps[data_shrimps$Unidad.Estadistica == 'KILOGRAMOS',]$Cantidad.Estadistica)	# 198703786 kg
sum(data_shrimps[data_shrimps$Unidad.Estadistica == 'DESCONOCIDA',]$TOTAL.Quantity.1)		# 1445156 kg
# sums to 200148942 kg, 200148.942 tons, comtrade reports 120790.285 tons
# better to take 198703786 kg, 198703.786 tons
## -> this should be the total weight for shrimps, rerun comtrade checks for this weight column
## or not
## check the ton/kg ratio fob/weight then



# next steps:
# in process script, include a 'corrected_net_weight_kg' column
