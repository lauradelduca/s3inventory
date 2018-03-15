
library(readxl)
library(data.table)
library(dplyr)


# set location of files
f <- 'C:/Users/laura.delduca/Desktop/CKAN/CD_BRAZIL_2017 (1).csv'

setwd('C:/Users/laura.delduca/Desktop/CKAN')

# read the file
j <- read.csv(f, header = TRUE, sep = ';')
					
					
					
# get index of all rows that have NAs across all columns
k <- which( apply(j, 1, function(x) all(is.na(x))) )
# remove those rows with all NAs
if(length(k)>0) j<- j[-k,]


# create file of SOY HS codes

hs <- read.csv('C:/Users/laura.delduca/Desktop/datamyne combine files/commodity_equivalents_final.csv', 
			header = TRUE, sep = ';',
			colClasses = c("character", "character", "character", "character", "character", 
							"character", "character", "numeric"))

unique(hs$ï..prod_name)
			
hs_soy <- hs[hs$'ï..prod_name' == 'SOY',]
							
soy <- as.vector(hs_soy$code_value)

j <- filter(j, PRODUCT_HS %in% soy)

# filter for exports to France

j <- filter(j, COUNTRY_OF_DESTINATION == 'FRANCA')

# write file
write.csv2(j, 'CD_BRAZIL_SOY_2017_FRANCE.csv', quote = FALSE, row.names = FALSE)


## to clear the R environment
rm(list = ls())