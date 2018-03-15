# prepare COMTRADE data for checks

library(readxl)
library(data.table)
library(dplyr)


options(scipen=99999999)
 

# set location of files and get all file paths
din <- 'C:/Users/laura.delduca/Desktop/COMTRADE'

setwd(din)

ff <- list.files(din, pattern = '2005.csv$', full = TRUE)


# read the file

j <- fread(ff[1])
					
					
# get index of all rows that have NAs across all columns
k <- which( apply(j, 1, function(x) all(is.na(x))) )
# remove those rows with all NAs
if(length(k)>0) j<- j[-k,]


# zoom in

countries <- c("Argentina", "Bolivia", "Brazil", "Chile", "China", "Colombia", "Ecuador", "Indonesia",
				"Mexico", "Panama", "Paraguay", "Peru", "Russia", "Uruguay", "Venezuela")

j <- j[(j$'Trade Flow' == 'Export') & (j$Partner %in% countries)]

j <- data.frame('country' = j$Partner,
				'commodity' = as.integer(j$'Commodity Code'), 
				'comtrade_weight' = j$'Netweight (kg)')


# filter for our HS codes

hs <- fread('C:/Users/laura.delduca/Desktop/datamyne combine files/commodity_equivalents_final.csv')

#hs <- read.csv('C:/Users/laura.delduca/Desktop/datamyne combine files/commodity_equivalents_final.csv', 
#			header = TRUE, sep = ';',
#			colClasses = c("character", "character", "character", "character", "character", 
#							"character", "character", "numeric"))

ncm8 <- as.vector(as.numeric(hs$code_value[hs$code_type == 'NCM_8']))
hs6 <- as.vector(as.numeric(hs$code_value[hs$code_type == 'HS_6']))
hs4 <- as.vector(as.numeric(hs$code_value[hs$code_type == 'HS_4']))
hs <- c(ncm8, hs6, hs4)


j <- j[j$commodity %in% hs,]


# aggregate by country, hs code, sum weight

data <- aggregate(j$comtrade_weight, by = list(country = j$country, hs = j$commodity), FUN = sum, na.rm = TRUE)

names(data) <- c('country', 'hs', 'net_weight')



# write file
write.csv2(j, 'COMTRADE_ZOOMIN_2005.csv', quote = FALSE, row.names = FALSE)
