
## cleaning files containing Chinese characters


## to clear the R environment
rm(list = ls())


library(readxl)
library(data.table)
library(dplyr)


# set location of files
din <- 'C:/Users/laura.delduca/Desktop/china'

setwd(din)

f1 <- '2hs_CCD42 201501-201612 import.csv'
f2 <- '6hs code.csv'

# read the files

j1 <- read.csv(f1, sep = '\t', encoding="UTF-8", stringsAsFactors=FALSE)
j2 <- read.csv(f2, sep = '\t', encoding="UTF-8", stringsAsFactors=FALSE)


# get index of all rows that have NAs across all columns, for j1
k1 <- which( apply(j1, 1, function(x) all(is.na(x))) )
# remove those rows with all NAs
if(length(k1)>0) j1<- j1[-k1,]

# get index of all rows that have NAs across all columns, for j2
k2 <- which( apply(j2, 1, function(x) all(is.na(x))) )
# remove those rows with all NAs
if(length(k2)>0) j2<- j2[-k2,]

# the first 2681 rows of j2 are empty
# the same number of NA j2$Month
# delete all rows with is.na(Month)
k3 <- which( is.na(j2$Month) )
# remove those rows with Month NA
if(length(k3)>0) j2<- j2[-k3,]

# basic info
dim(j1)
names(j1)
dim(j2)
names(j2)

names(j1)==names(j2)


# test if all rows of j1 are in j2
do.call(paste0, j1) %in% do.call(paste0, j2)
# no, not this and not vice-versa

j <- rbind(j1, j2)

# delete duplicates
#j <- j[!duplicated(j), ]


dim(j)


# data is for all months of years 2015 and 2016
# create one file per year

j$year <- substr(j$Month,1,4)

j_15 <- filter(j, year == '2015')
j_16 <- filter(j, year == '2016')

j_15$year <- j_16$year <- NULL


# write files
write.csv2(j_15, 'CD_CHINA_2015.csv', quote = FALSE, row.names = FALSE, fileEncoding = 'UTF-8')
write.csv2(j_16, 'CD_CHINA_2016.csv', quote = FALSE, row.names = FALSE)
