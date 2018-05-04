## Brazil datamyne third party corn
## preprocessing

library(readxl)
library(data.table)
library(dplyr)
library(stringr)

options(scipen=99999999)



#din <- 'C:/Users/laura.delduca/Desktop/code/0502/brazil_corn/2017'
din <- 'C:/Users/laura.delduca/Desktop/code/0502/brazil_corn/2016'
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
D$'COD.SUBITEM.NCM' <- as.numeric(D$'COD.SUBITEM.NCM')
D$'COD.SUBITEM.NCM' <- formatC(D$'COD.SUBITEM.NCM', width = 8, format = "d", flag = "0") 

# be sure that numbers are correctly formatted, no commas, only dot as decimal
grep(',', D$QTDE.EST.MERC.BAL.EXP)
grep(',', D$VMLE.DOLAR.Unid.BAL.EXP)
grep(',', D$VMLE.DOLAR.BAL.EXP)
grep(',', D$PESO.LIQ.MERC.BAL.EXP)

# check dates 
unique(str_sub(as.character(D$'DIA.REGIS'), start = -4))
unique(str_sub(as.character(D$'DIA.DESEMB'), start = -4))


# write file
write.table(D, 'CD_BRAZIL_2017.csv', quote = FALSE, row.names = FALSE, dec = '.', sep = ';')



## to clear the R environment
#rm(list = ls())


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
