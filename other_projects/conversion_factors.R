## conversion_factors.R



library(data.table)
library(readr)
library(readxl)


hs <- read.delim('C:/Users/laura.delduca/Desktop/commodity_equivalents.csv', sep = ';', header = TRUE)

conv <- read_excel("C:/Users/laura.delduca/Desktop/CONVERSION FACTORS_RAW.xlsx", sheet = 'Sheet1')


## COCOA

hs$eq_factor_calories[hs$code_value == '180100'] <- 1
hs$eq_factor_calories[hs$code_value == '180200'] <- NA
hs$eq_factor_calories[hs$code_value == '180310'] <- as.numeric(conv$calories[conv$COMMODITY == 'COCOA PASTE'])/as.numeric(conv$calories[conv$COMMODITY == 'COCOA BEANS'])
hs$eq_factor_calories[hs$code_value == '180320'] <- conv$calories[conv$commodity == 'COCOA PASTE']/conv$calories[conv$commodity == 'COCOA BEANS']
hs$eq_factor_calories[hs$code_value == '180400'] <- conv$calories[conv$commodity == 'COCOA BUTTER']/conv$calories[conv$commodity == 'COCOA BEANS']
hs$eq_factor_calories[hs$code_value == '180500'] <- conv$calories[conv$commodity == 'COCOA POWDER']/conv$calories[conv$commodity == 'COCOA BEANS']
hs$eq_factor_calories[hs$code_value == '180610'] <- conv$calories[conv$commodity == 'COCOA POWDER']/conv$calories[conv$commodity == 'COCOA BEANS']

hs$eq_factor_protein[hs$code_value == '180100'] <- 1
hs$eq_factor_protein[hs$code_value == '180200'] <- NA
hs$eq_factor_protein[hs$code_value == '180310'] <- conv$protein[conv$commodity == 'COCOA PASTE']/conv$protein[conv$commodity == 'COCOA BEANS']
hs$eq_factor_protein[hs$code_value == '180320'] <- conv$protein[conv$commodity == 'COCOA PASTE']/conv$protein[conv$commodity == 'COCOA BEANS']
hs$eq_factor_protein[hs$code_value == '180400'] <- conv$protein[conv$commodity == 'COCOA BUTTER']/conv$protein[conv$commodity == 'COCOA BEANS']
hs$eq_factor_protein[hs$code_value == '180500'] <- conv$protein[conv$commodity == 'COCOA POWDER']/conv$protein[conv$commodity == 'COCOA BEANS']
hs$eq_factor_protein[hs$code_value == '180610'] <- conv$protein[conv$commodity == 'COCOA POWDER']/conv$protein[conv$commodity == 'COCOA BEANS']

hs$eq_factor_fat[hs$code_value == '180100'] <- 1
hs$eq_factor_fat[hs$code_value == '180200'] <- NA
hs$eq_factor_fat[hs$code_value == '180310'] <- conv$fat[conv$commodity == 'COCOA PASTE']/conv$fat[conv$commodity == 'COCOA BEANS']
hs$eq_factor_fat[hs$code_value == '180320'] <- conv$fat[conv$commodity == 'COCOA PASTE']/conv$fat[conv$commodity == 'COCOA BEANS']
hs$eq_factor_fat[hs$code_value == '180400'] <- conv$fat[conv$commodity == 'COCOA BUTTER']/conv$fat[conv$commodity == 'COCOA BEANS']
hs$eq_factor_fat[hs$code_value == '180500'] <- conv$fat[conv$commodity == 'COCOA POWDER']/conv$fat[conv$commodity == 'COCOA BEANS']
hs$eq_factor_fat[hs$code_value == '180610'] <- conv$fat[conv$commodity == 'COCOA POWDER']/conv$fat[conv$commodity == 'COCOA BEANS']