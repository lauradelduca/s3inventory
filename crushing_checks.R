## crushing OCR check

library(data.table)

options(scipen=999999999)

din <- 'C:/Users/laura.delduca/Desktop/arg_scans/Hinrichsen crushing/Hinrichsen crushing'
setwd(din)
ff <- list.files(din, pattern = 'csv', full = TRUE)

j1 <- fread(ff[1])
j2 <- fread(ff[2])
j3 <- fread(ff[3])
j4 <- fread(ff[4])

dim(j1); dim(j2); dim(j3); dim(j4)

names(j1); names(j2); names(j3); names(j4)