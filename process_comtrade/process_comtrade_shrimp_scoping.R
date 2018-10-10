# prepare COMTRADE data for shrimp scoping

library(readxl)
library(data.table)
library(dplyr)


options(scipen=999999999999999999999999999999999999999999)
 

# set location of files and get all file paths
din <- 'C:/Users/laura.delduca/Desktop/code/0928/comtrade'

setwd(din)

ff <- list.files(din)

# only select from 2013 onwards


# filter for our HS codes

hs <- fread('C:/Users/laura.delduca/Desktop/code/0928/commodity_equivalents_final.csv')

hs <- as.vector(as.numeric(hs$code_value))

# only select shrimp codes

for (f in ff){

	j <- fread(f)
	
	j <- j[j$'Commodity Code' %in% hs,]

	j <- data.frame('trade_flow' = j$'Trade Flow',
	                'partner' = j$Partner,
	                'country' = j$Reporter,
			'commodity' = as.integer(j$'Commodity Code'), 
			'netweight_kg' = j$'Netweight (kg)')


	write.csv2(j, paste0(f, '_shrimp', ".csv"), quote = FALSE, row.names = FALSE)

}

