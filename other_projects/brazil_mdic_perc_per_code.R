## brazil_mdic_perc_per_code.R
## Brazil MDIC 1997-2004 percentage of total weight per HS code
## Laura Del Duca


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
current_folder <- '0608'
script_folder <- 's3inventory/comtrade_checks'

source('R_aws.s3_credentials.R')					# load AWS S3 credentials


for (yy in 1997:2004){
	
	# load csv originals keys for all years
	keys <- get_bucket_df(bucket = 'trase-storage', prefix = 'data/1-TRADE/CD/EXPORT/BRAZIL/MDIC/NCM8/')	
	keys <- subset(keys, grepl(yy, Key))
	f <- as.vector(keys$Key)
	
	
	
	
	
	
	
}
