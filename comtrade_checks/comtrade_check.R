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


source(aws_credentials_file)				# load AWS S3 credentials
source(get_comtrade_files.R)				# load preprocessed COMTRADE files 2005 - 2016
source(get_hs_codes.R)						# load relevant HS codes from commodity dictionary

source(get_aws_content.R)					# load content of AWS into dataframe CD
source(get_columns.R)						# select columns for each country/dataset
write.table(CD, paste0(current_folder, '/', 'CD_AWS.csv'), quote = FALSE, row.names = FALSE, dec = '.', sep = ';')


source(write_weight_check_files.R)			# write weight checks files
source(write_unit_check_files.R)			# write unit checks files

source(comtrade_check_helpers.R) 			# helpers (print header,first lines of all files for 'countries')
source(comtrade_check_helpers_argentina.R)	# helpers for Argentina

source(get_release_codes.R)					# load codes needed for release
write.table(todownload, paste0(current_folder, '/', 'CD_todownload.csv'), quote = FALSE, row.names = FALSE, dec = '.', sep = ';')

