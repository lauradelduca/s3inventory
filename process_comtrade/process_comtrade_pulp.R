# prepare COMTRADE data for pulp

library(readxl)
library(data.table)
library(dplyr)


options(scipen=999999999999999999999999999999999999999999)
 

# set location of files and get all file paths
din <- 'C:/Users/laura.delduca/Desktop/code/0928/comtrade'

setwd(din)

ff <- list.files(din)


# filter for our shrimp HS codes

obj <- get_object(object = 'data/1-TRADE/commodity_equivalents_final.csv', bucket = 'trase-storage')

hs <- read.csv(text = rawToChar(obj), sep = ';', quote = '',
               colClasses = c("character", "character", "character", 
                              "character", "character", "numeric", "character", 
                              "character"))

hs6 <- as.vector(as.numeric(hs$code_value[hs$com_name == 'WOOD PULP']))
hs6 <- AT.add.leading.zeros(hs6, digits = 6)


for (f in ff){

	j <- fread(f)
	
	j <- j[j$'Commodity Code' %in% hs6,]

	j <- data.frame('flow' = j$'Trade Flow',
	                'partner' = j$Partner,
	                'country' = j$Reporter,
			'commodity' = as.integer(j$'Commodity Code'), 
			'netweight_kg' = j$'Netweight (kg)',
			'value_USD' = j$'Trade Value (US$)')


	write.csv2(j, paste0(f, '_pulp', '.csv'), quote = FALSE, row.names = FALSE)

}

