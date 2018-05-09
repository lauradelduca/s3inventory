## improving argentina

library(readxl)
library(data.table)
library(dplyr)

options(scipen=99999999)
 

# 2013
din <- 'C:/Users/laura.delduca/Desktop/code/0507/argentina_2013'
setwd(din)
ff <- list.files(din, pattern = 'csv', full = TRUE)
arg13 <- fread(ff[1])

# 2014
din <- 'C:/Users/laura.delduca/Desktop/code/0507/argentina_2014'
setwd(din)
ff <- list.files(din, pattern = 'csv', full = TRUE)


J <- list()
i = 1

for (f in ff){

	j <- fread(f)
	
	## remove first 13 and last 0 rows if first file
	##j <- slice(j, 16:(nrow(j)-6))
	#j <- slice(j, 11:nrow(j))

	k <- which( apply(j, 1, function(x) all(is.na(x))) )
	if(length(k)>0) j<- j[-k,]

	J[[i]] <- j
	i <- i + 1
	
}

D <- do.call(rbind, J)

names(D) <- names(arg13)
D <- data.frame(lapply(D, function(x) {gsub(";", ",", x)}))


# remove commas from both 2013 and 2014 file, for both weight columns

# make sure HS column is even number of digit in 2013 and 2014
# data$PRODUCT_HS <- formatC(data$PRODUCT_HS, width = 8, format = "d", flag = "0") 


write.table(D, 'CD_ARGENTINA_2014.csv', quote = FALSE, row.names = FALSE, dec = '.', sep = ';')
