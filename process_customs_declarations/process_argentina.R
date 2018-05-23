## Preprocessing of Argentina customs declarations trade data 2013 - 2017 from SICEX2.5

## maybe adding 2010 - 2012 
## update to work remotely

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
current_folder <- '0517'
script_folder <- 's3inventory/comtrade_checks'

source('R_aws.s3_credentials.R')						# load AWS S3 credentials



setwd('C:/Users/laura.delduca/Desktop/code/0507')

	
# 2013
din <- 'argentina_2013'
ff <- list.files(din, pattern = 'csv', full = TRUE)
arg13 <- fread(ff[1])


# remove commas from 2013, for numeric columns
arg13$TOTAL.Quantity.1 <- as.numeric(gsub(",", "", arg13$TOTAL.Quantity.1))
arg13$Cantidad.Estadística <- as.numeric(gsub(",", "", arg13$Cantidad.Estadística))
arg13$TOTAL.FOB.Value..US.. <- as.numeric(gsub(",", "", arg13$TOTAL.FOB.Value..US..))
arg13$FOB.per.Unit..Quantity1. <- as.numeric(gsub(",", "", arg13$FOB.per.Unit..Quantity1.))
arg13$TOTAL.CIF.Value..US.. <- as.numeric(gsub(",", "", arg13$TOTAL.CIF.Value..US..))
arg13$Freight <- as.numeric(gsub(",", "", arg13$Freight))

# remove special characters from colnames, correct encoding of whole file to utf8
# fread with encoding = 'UTF-8' option is not sufficient so correcting colnames manually
setnames(	arg13, 
			old = c('Cantidad.Estadística', 'Unidad.Estadística'), 
			new = c('Cantidad.Estadistica', 'Unidad.Estadistica'))

# make sure HS column is even number of digits, here 6
arg13$Harmonized.Code.Product.English <- AT.add.leading.zeros(arg13$Harmonized.Code.Product.English, digits = 6)
# this should be 10 digits:
arg13$Product.Schedule.B.Code <- AT.add.leading.zeros(arg13$Product.Schedule.B.Code, digits = 10)


write.table(arg13, paste0(din, '/', 'CD_ARGENTINA_2013_test.csv'), quote = FALSE, row.names = FALSE, dec = '.', sep = ';')



# 2014 - 2017

din <- c('argentina_2014', 'argentina_2015', 'argentina_2016', 'argentina_2017')

for (yy in din){

	ff <- list.files(yy, pattern = 'csv', full = TRUE)

	J <- list()
	i = 1
	total_rows <- 0
	
	for (f in ff){

		j <- fread(f)
		total_rows <- total_rows + nrow(j)
		
		k <- which( apply(j, 1, function(x) all(is.na(x))) )
		if(length(k)>0) j<- j[-k,]

		J[[i]] <- j
		i <- i + 1
		
	}

	D <- do.call(rbind, J)
	
	
	# check that dimensions are correct for each year	
	print(dim(D))
	print(total_rows)

	# check there is no separator issue
	D <- data.frame(lapply(D, function(x) {gsub(";", ",", x)}))
	
	# assign correct names, use 2013 for 2014-2017
	names(D) <- names(arg13)

	# remove commas from all numeric columns
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
	

	write.table(D, paste0(yy, '/', 'CD_ARGENTINA_', str_sub(yy, start= -4), '_test.csv'), quote = FALSE, row.names = FALSE, dec = '.', sep = ';')

}

# clean up
gc()
