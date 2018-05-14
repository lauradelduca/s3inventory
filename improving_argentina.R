## improving argentina

library(readxl)
library(data.table)
library(dplyr)

options(scipen=99999999)


# should setwd once here, and then have it relational to this one
# correct all in comtrade check if working
	
# 2013
din <- 'C:/Users/laura.delduca/Desktop/code/0507/argentina_2013'
setwd(din)
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
arg13$Harmonized.Code.Product.English <- formatC(	arg13$Harmonized.Code.Product.English, 
													width = 6, format = "d", flag = "0") 

write.table(arg13, 'CD_ARGENTINA_2013_test.csv', quote = FALSE, row.names = FALSE, dec = '.', sep = ';')


# 2014 - 2017
# should loop through all years

din <- c(	'C:/Users/laura.delduca/Desktop/code/0507/argentina_2014',
			'C:/Users/laura.delduca/Desktop/code/0507/argentina_2015',
			'C:/Users/laura.delduca/Desktop/code/0507/argentina_2016',
			'C:/Users/laura.delduca/Desktop/code/0507/argentina_2017')

for (yy in din){

	setwd(yy)
	ff <- list.files(yy, pattern = 'csv', full = TRUE)


	J <- list()
	i = 1

	for (f in ff){

		j <- fread(f)

		k <- which( apply(j, 1, function(x) all(is.na(x))) )
		if(length(k)>0) j<- j[-k,]

		J[[i]] <- j
		i <- i + 1
		
	}

	D <- do.call(rbind, J)

	# assign correct names, use 2013 for 2014-2017
	names(D) <- names(arg13)

	# check there is no separator issue
	D <- data.frame(lapply(D, function(x) {gsub(";", ",", x)}))

	# remove commas from all files, for both weight columns
	D$TOTAL.Quantity.1 <- as.numeric(gsub(",", "", D$TOTAL.Quantity.1))
	D$Cantidad.Estadística <- as.numeric(gsub(",", "", D$Cantidad.Estadística))
	D$TOTAL.FOB.Value..US.. <- as.numeric(gsub(",", "", D$TOTAL.FOB.Value..US..))
	D$FOB.per.Unit..Quantity1. <- as.numeric(gsub(",", "", D$FOB.per.Unit..Quantity1.))
	D$TOTAL.CIF.Value..US.. <- as.numeric(gsub(",", "", D$TOTAL.CIF.Value..US..))
	D$Freight <- as.numeric(gsub(",", "", D$Freight))


	# make sure HS column is even number of digits
	# data$PRODUCT_HS <- formatC(data$PRODUCT_HS, width = 8, format = "d", flag = "0") 

	write.table(D, 'CD_ARGENTINA_2014.csv', quote = FALSE, row.names = FALSE, dec = '.', sep = ';')

}