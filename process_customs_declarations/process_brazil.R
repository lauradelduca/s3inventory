## improving Brazil
## remove scientific notation

library(data.table)
library(dplyr)

options(scipen=99999999)

# CD_BRAZIL_2017.csv

din <- 'C:/Users/laura.delduca/Desktop/code/0504/brazil'
setwd(din)
ff <- list.files(din, pattern = 'csv', full = TRUE)



write.table(b17sn, 'CD_BRAZIL_2017_test1.csv', quote = FALSE, row.names = FALSE, dec = '.', sep = ';')

