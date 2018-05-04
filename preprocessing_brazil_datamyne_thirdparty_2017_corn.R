## Brazil datamyne third party 2017 corn
## preprocessing

library(readxl)
library(data.table)
library(dplyr)

options(scipen=99999999)



din <- 'C:/Users/laura.delduca/Desktop/code/0502/brazil_corn/2017'
setwd(din)
ff <- list.files(din, pattern = 'csv', full = TRUE)

for (f in ff){
	data <- fread(f)
	print(f)
	print(dim(data))
	print(data[1:3,])
	print(names(data))
}

r <- 0
for (f in ff){
	r <- r + nrow(fread(f))
}

J <- list()
i = 1

for (f in ff){
	
	j <- fread(f)
	
	# remove those rows with all NAs
	k <- which( apply(j, 1, function(x) all(is.na(x))) )
	if(length(k)>0) j<- j[-k,]
						
	# add the data to the list
	J[[i]] <- j
	i <- i + 1

}


# append all data, earlier stored in a list of dataframes in J
D <- do.call(rbind, J)

# in all columns replace ; with ,
D <- data.frame(lapply(D, function(x) {gsub(";", ",", x)}))

# add that hs column needs to have leading zeroes
D$'COD.SUBITEM.NCM' <- as.character(D$'COD.SUBITEM.NCM')
D$'COD.SUBITEM.NCM' <- sprintf("%08s", D$'COD.SUBITEM.NCM')

# be sure that numbers are correctly formatted, no commas, only dot as decimal
grepl(',', D$QTDE.EST.MERC.BAL.EXP)
grepl(',', D$VMLE.DOLAR.Unid.BAL.EXP)
grepl(',', D$VMLE.DOLAR.BAL.EXP)
grepl(',', D$PESO.LIQ.MERC.BAL.EXP)

# check dates unique(D$'DIA REGIS'); unique(D$'DIA DESEMB')
!!

# write file
write.table(D, 'CD_BRAZIL_2017.csv', quote = FALSE, row.names = FALSE, dec = '.', sep = ';')



## to clear the R environment
#rm(list = ls())


#nm <- c('DATE_YYYY_MM', 'DATE_YYYY_MM_DD', 'COMPANY_ID_NUMBER', 'EXPORTER', 
#'COUNTRY_OF_DESTINY', 'PRODUCT_SCHEDULE_B_CODE', 'PRODUCT_DESCRIPTION_BY_SCHEDULE_B_CODE', 'PRODUCT_DESCRIPTION_BY_SCHEDULE_B_CODE_IN_URUGUAY', 
#'PRODUCT_DESCRIPTION', 'HARMONIZED_CODEPRODUCT_ENGLISH', 'HARMONIZED_CODE_DESCRIPTION_ENGLISH', 
#'TOTAL_QUANTITY_1', 'MEASURE_UNIT_1_QUANTITY_1', 'TOTAL_FOB_VALUE_US', "FOB_PER_UNIT_QUANTITY_1", 'TOTAL_CIF_VALUE_US', 
#'TOTAL_NET_WEIGHT_KG', 'TOTAL_GROSS_WEIGHT_KG', 'TYPE_OF_TRANSPORT', 'CUSTOM', 'FREIGHT', 'INSURANCE',
#"TRANSPORT_COMPANY_TRANSPORT_USED", 'INCOTERM')



## replacing codes with new download

data <- fread(ff[1])
cotton <- fread(ff[2])
cotton <- cotton[, 1:12]

data$PRODUCT_HS <- formatC(data$PRODUCT_HS, width = 8, format = "d", flag = "0") 


data <- data[!(  (substr(data$PRODUCT_HS, 1, 6) == 120720) | 
				(substr(data$PRODUCT_HS, 1, 6) == 120721) | 
				(substr(data$PRODUCT_HS, 1, 4) == 5203) |
				(substr(data$PRODUCT_HS, 1, 6) == 120729) |
				(substr(data$PRODUCT_HS, 1, 6) == 140420) |
				(substr(data$PRODUCT_HS, 1, 6) == 151210) |
				(substr(data$PRODUCT_HS, 1, 6) == 151220) |
				(substr(data$PRODUCT_HS, 1, 6) == 151221) |
				(substr(data$PRODUCT_HS, 1, 6) == 151229) |
				(substr(data$PRODUCT_HS, 1, 6) == 230610) |
				(substr(data$PRODUCT_HS, 1, 6) == 470610) |
				(substr(data$PRODUCT_HS, 1, 2) == 52)  )]

names(cotton) <- names(data)

test <- rbind(data, cotton)

write.table(test, 'CD_BRAZIL_2016_NEW_COTTON.csv', quote = FALSE, row.names = FALSE, dec = '.', sep = ';')
