## create file of CORN HS codes


library(readxl)
library(data.table)
library(dplyr)


# set location of files
f <- 'C:/Users/laura.delduca/Desktop/CD_BRAZIL_2017.csv'

setwd('C:/Users/laura.delduca/Desktop')

# read the file
#j <- read.csv(f, header = TRUE, sep = ';',
#			colClasses = c("character", "character", "character", "character", "character", 
#					"character", "character", "character", "character", "character", "character", "character", "character",
#					"character", "numeric", "character", "numeric", "numeric", "numeric",
#					"numeric", "numeric", "character", "character", "character", "character", "character", 
#					"character", "character", "character", "character"))
#j <- read_excel(f)
#j <- read.csv(f, header = TRUE, sep = ';', quote = '',
#				colClasses = c("character", "character", "character", "character", "character", 
#								"numeric", "character", "character", "character", "character",	
#								"numeric", "character"))
j <- fread(f)
					
					
# get index of all rows that have NAs across all columns
k <- which( apply(j, 1, function(x) all(is.na(x))) )
# remove those rows with all NAs
if(length(k)>0) j<- j[-k,]


# create file of CORN HS codes

hs <- read.csv('C:/Users/laura.delduca/Desktop/datamyne combine files/commodity_equivalents_final.csv', 
			header = TRUE, sep = ';',
			colClasses = c("character", "character", "character", "character", "character", 
							"character", "character", "numeric"))

unique(hs$ï..prod_name)
			
hs_corn <- hs[hs$'ï..prod_name' == 'CORN',]
							
corn <- as.vector(hs_corn$code_value)

j <- j[j$PRODUCT_HS %in% corn,]

#j <- filter(j, 'PRODUCT_HS'   %in% corn)


# aggregate by hs code, sum weight

data <- data.frame(hs = as.character(j$PRODUCT_HS), net_weight = as.integer(j$NET_WEIGHT))

data <- aggregate(data$net_weight, by = list(hs = data$hs), FUN = sum, na.rm = TRUE)

#> sum(j$NET_WEIGHT[j$PRODUCT_HS == 10059010])
#[1] 29242388430

data$x[data$hs == 10059010] <- 29242388430

names(data) <- c('hs', 'net_weight')


# include percentage column

total_weight <- sum(data$net_weight)

data$perc_weight <- data$net_weight/total_weight


# write file
write.csv2(j, 'CD_BRAZIL_2017_CORN_PERC.csv', quote = FALSE, row.names = FALSE)


## to clear the R environment
rm(list = ls())