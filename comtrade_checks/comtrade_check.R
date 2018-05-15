## Checking customs declarations trade data against COMTRADE for 2018 level 1 release
## Laura Del Duca

require(stringr)
require(gsubfn)
require(dplyr)
require(readxl)
require(data.table)
require(libamtrack)
require(aws.s3)

options(scipen=9999999999)

setwd('C:/Users/laura.delduca/Desktop/code')

current_folder <- '0515'
aws_credentials_file <- 'R_aws.s3_credentials.R'

countries <- unique(as.vector(CD$country))		# chose which countries to run
parked <- c('VENEZUELA', 'COLOMBIA', 'PANAMA', 'BOLIVIA', 'MEXICO', 'ARGENTINA', 'BRAZIL')
countries <- countries[!countries %in% parked]
countries <- c('ARGENTINA')
cc <- 'ARGENTINA'


source(aws_credentials_file)		# load AWS S3 credentials
source(get_comtrade_files.R)		# load preprocessed COMTRADE files 2005 - 2016
source(get_hs_codes.R)				# load relevant HS codes from commodity dictionary

source(get_aws_content.R)			# load content of AWS into dataframe CD
source(get_columns.R)				# select columns for each country/dataset
write.table(CD, paste0(current_folder, '/', 'CD_AWS.csv'), quote = FALSE, row.names = FALSE, dec = '.', sep = ';')


source(write_weight_check_files.R)	# write weight checks files
source(write_unit_check_files.R)	# write unit checks files

source(comtrade_check_helpers.R) 	# helpers (print header,first lines of all files for 'countries')

source(get_release_codes.R)			# load codes needed for release
write.table(todownload, 'CD_todownload.csv', quote = FALSE, row.names = FALSE, dec = '.', sep = ';')


## helpers for argentina

soy <- as.vector(as.numeric(sort(unique(hs$code_value[hs$com_name == 'SOYBEANS']))))

shrimps <- c( 30616, 30617, 30635, 30636, 30695)

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

sum(as.numeric(gsub(',', '', data_beef$TOTAL.Quantity.1)))/1000
sum(as.numeric(gsub(',', '', data_beef$Cantidad.Estadística)))/1000
sum(as.numeric(gsub(',', '', data_chicken$TOTAL.Quantity.1)))/1000
sum(as.numeric(gsub(',', '', data_chicken$Cantidad.Estadística)))/1000
sum(as.numeric(gsub(',', '', data_corn$TOTAL.Quantity.1)))/1000
sum(as.numeric(gsub(',', '', data_corn$Cantidad.Estadística)))/1000
sum(as.numeric(gsub(',', '', data_cotton$TOTAL.Quantity.1)))/1000
sum(as.numeric(gsub(',', '', data_cotton$Cantidad.Estadística)))/1000
sum(as.numeric(gsub(',', '', data_leather$TOTAL.Quantity.1)))/1000
sum(as.numeric(gsub(',', '', data_leather$Cantidad.Estadística)))/1000
sum(as.numeric(gsub(',', '', data_timber$TOTAL.Quantity.1)))/1000
sum(as.numeric(gsub(',', '', data_timber$Cantidad.Estadística)))/1000
sum(as.numeric(gsub(',', '', data_woodpulp$TOTAL.Quantity.1)))/1000
sum(as.numeric(gsub(',', '', data_woodpulp$Cantidad.Estadística)))/1000
sum(as.numeric(gsub(',', '', data_shrimps$TOTAL.Quantity.1)))/1000
sum(as.numeric(gsub(',', '', data_shrimps$Cantidad.Estadística)))/1000
sum(as.numeric(gsub(',', '', data_soy$TOTAL.Quantity.1)))/1000
sum(as.numeric(gsub(',', '', data_soy$Cantidad.Estadística)))/1000
sum(as.numeric(gsub(',', '', data_sugarcane$TOTAL.Quantity.1)))/1000
sum(as.numeric(gsub(',', '', data_sugarcane$Cantidad.Estadística)))/1000


sort(unique(data_beef$Unidad.Estadística))
sort(unique(data_chicken$Unidad.Estadística))
sort(unique(data_corn$Unidad.Estadística))
sort(unique(data_cotton$Unidad.Estadística))
sort(unique(data_leather$Unidad.Estadística))
sort(unique(data_timber$Unidad.Estadística))
sort(unique(data_woodpulp$Unidad.Estadística))
sort(unique(data_shrimps$Unidad.Estadística))
sort(unique(data_soy$Unidad.Estadística))
sort(unique(data_sugarcane$Unidad.Estadística))
	

sort(unique(data_beef$Measure.Unit.1..Quantity.1.[data_beef$Unidad.Estadística != 'KILOGRAMOS']))
sort(unique(data_chicken$Measure.Unit.1..Quantity.1.[data_chicken$Unidad.Estadística != 'KILOGRAMOS']))
sort(unique(data_corn$Measure.Unit.1..Quantity.1.[data_corn$Unidad.Estadística != 'KILOGRAMOS']))
sort(unique(data_cotton$Measure.Unit.1..Quantity.1.[data_cotton$Unidad.Estadística != 'KILOGRAMOS']))
sort(unique(data_leather$Measure.Unit.1..Quantity.1.[data_leather$Unidad.Estadística != 'KILOGRAMOS']))
sort(unique(data_timber$Measure.Unit.1..Quantity.1.[data_timber$Unidad.Estadística != 'KILOGRAMOS']))
sort(unique(data_woodpulp$Measure.Unit.1..Quantity.1.[data_woodpulp$Unidad.Estadística != 'KILOGRAMOS']))
sort(unique(data_shrimps$Measure.Unit.1..Quantity.1.[data_shrimps$Unidad.Estadística != 'KILOGRAMOS']))
sort(unique(data_soy$Measure.Unit.1..Quantity.1.[data_soy$Unidad.Estadística != 'KILOGRAMOS']))
sort(unique(data_sugarcane$Measure.Unit.1..Quantity.1.[data_sugarcane$Unidad.Estadística != 'KILOGRAMOS']))
