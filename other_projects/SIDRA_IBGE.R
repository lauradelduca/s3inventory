## 2018-06-15 connecting to SIDRA IBGE from R

## sidrar

install.packages("sidrar")

rm(list=ls(all=TRUE))


require(stringr)
require(gsubfn)
require(dplyr)
require(readxl)
require(data.table)
require(libamtrack)

library(sidrar)
require(aws.s3)

options(scipen=99999999)

setwd('C:/Users/laura.delduca/Desktop/code')
current_folder <- '0618'
script_folder <- 's3inventory/comtrade_checks'

source('R_aws.s3_credentials.R')					# load AWS S3 credentials




# Table 1092 Animal kills per trimester along with carcass weights: cattle
cattle <- get_sidra(x = 1092) #,
          #variable = 63,
          #period = c("last" = 12),
          #geo = "City",
          #geo.filter = 5002407,
          #classific = "c315",
          #category = list(7169),
          #header = FALSE,
          #format = 3)

		  
# Table 1093 Animal kills per trimester along with carcass weights: pigs
pigs <- get_sidra(x = 1093) #,
          #variable = 63,
          #period = c("last" = 12),
          #geo = "City",
          #geo.filter = 5002407,
          #classific = "c315",
          #category = list(7169),
          #header = FALSE,
          #format = 3)

		  
# Table 1094 Animal kills per trimester along with carcass weights: chicken
chicken <- get_sidra(x = 1094) #,
          #variable = 63,
          #period = c("last" = 12),
          #geo = "City",
          #geo.filter = 5002407,
          #classific = "c315",
          #category = list(7169),
          #header = FALSE,
          #format = 3)

		  
# Table 6669 Slaughtered animals, by herd type
herd <- get_sidra(x = 6669) #,
          #variable = 63,
          #period = c("last" = 12),
          #geo = "City",
          #geo.filter = 5002407,
          #classific = "c315",
          #category = list(7169),
          #header = FALSE,
          #format = 3)  


# save cattle	  
write.table(cattle, paste0(current_folder, '/', 'IBGE_1092_cattle_year.csv'), quote = FALSE, 
			row.names = FALSE, dec = '.', sep = ';')

zz <- rawConnection(raw(0), "r+")
write.table(cattle, zz, quote = FALSE, row.names = FALSE, dec = '.', sep = ';')
put_object(	file = rawConnectionValue(zz), bucket = 'trase-storage', 
			object = paste0('data/2-PRODUCTION/STATISTICS/BRAZIL/IBGE/beef/beef_ _mun.csv') )
close(zz)


# save pigs
write.table(pigs, paste0(current_folder, '/', 'IBGE_1092_pigs_year.csv'), quote = FALSE, 
			row.names = FALSE, dec = '.', sep = ';')

zz <- rawConnection(raw(0), "r+")
write.table(pigs, zz, quote = FALSE, row.names = FALSE, dec = '.', sep = ';')
put_object(	file = rawConnectionValue(zz), bucket = 'trase-storage', 
			object = paste0('data/2-PRODUCTION/STATISTICS/BRAZIL/IBGE/pigs/pigs_ _ .csv') )
close(zz)


# save chicken
write.table(chicken, paste0(current_folder, '/', 'IBGE_1092_chicken_year_mun.csv'), quote = FALSE, 
			row.names = FALSE, dec = '.', sep = ';')

zz <- rawConnection(raw(0), "r+")
write.table(chicken, zz, quote = FALSE, row.names = FALSE, dec = '.', sep = ';')
put_object(	file = rawConnectionValue(zz), bucket = 'trase-storage', 
			object = paste0('data/2-PRODUCTION/STATISTICS/BRAZIL/IBGE/chicken/chicken_ _ .csv') )
close(zz)


# save herd
write.table(herd, paste0(current_folder, '/', 'IBGE_1092_herd', yy, '.csv'), quote = FALSE, 
			row.names = FALSE, dec = '.', sep = ';')

# zz <- rawConnection(raw(0), "r+")
# write.table(herd, zz, quote = FALSE, row.names = FALSE, dec = '.', sep = ';')
# put_object(	file = rawConnectionValue(zz), bucket = 'trase-storage', 
			# object = paste0('data/2-PRODUCTION/STATISTICS/BRAZIL/IBGE/herd/herd_ _ .csv') )
# close(zz)


# clean up
gc()	