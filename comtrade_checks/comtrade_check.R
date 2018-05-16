## Checking customs declarations trade data against COMTRADE for 2018 level 1 release
## Laura Del Duca

rm(list=ls(all=TRUE))

require(stringr)
require(gsubfn)
require(dplyr)
require(readxl)
require(data.table)
require(libamtrack)
require(aws.s3)

options(scipen=9999999999)

setwd('C:/Users/laura.delduca/Desktop/code')

current_folder <- '0516'
script_folder <- 's3inventory/comtrade_checks'

source('R_aws.s3_credentials.R')											# load AWS S3 credentials
source(paste0(script_folder, '/', 'get_comtrade_files.R'))					# load preprocessed COMTRADE files 2005 - 2016
source(paste0(script_folder, '/', 'get_hs_codes.R'))						# load relevant HS codes from commodity dictionary

source(paste0(script_folder, '/', 'get_aws_content.R'))						# load content of AWS into dataframe CD
source(paste0(script_folder, '/', 'get_columns.R'))							# select columns for each country/dataset
write.table(CD, paste0(current_folder, '/', 'CD_AWS.csv'), quote = FALSE, row.names = FALSE, dec = '.', sep = ';')

countries <- unique(as.vector(CD$country))									# chose which countries to run
# parked <- c('VENEZUELA', 'COLOMBIA', 'PANAMA', 'BOLIVIA', 'MEXICO', 'ARGENTINA', 'BRAZIL')
# countries <- countries[!countries %in% parked]
countries <- c('ARGENTINA')
cc <- 'ARGENTINA'

source(paste0(script_folder, '/', 'write_weight_check_files.R'))			# write weight checks files


source(paste0(script_folder, '/', 'write_unit_check_files.R'))				# write unit checks files

source(paste0(script_folder, '/', 'comtrade_check_helpers.R')) 				# helpers (print header,first lines of all files for 'countries')
source(paste0(script_folder, '/', 'comtrade_check_helpers_argentina.R'))	# helpers for Argentina

source(paste0(script_folder, '/', 'get_release_codes.R'))					# load codes needed for release
write.table(todownload, paste0(current_folder, '/', 'CD_todownload.csv'), quote = FALSE, row.names = FALSE, dec = '.', sep = ';')
