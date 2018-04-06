# prepare COMTRADE data for checks

library(readxl)
library(data.table)
library(dplyr)


options(scipen=999999999999999999999999999999999999999999)
 

# set location of files and get all file paths
din <- 'C:/Users/laura.delduca/Desktop/0404'

setwd(din)

ff <- list.files(din)

# filter for our HS codes

hs <- fread('C:/Users/laura.delduca/Desktop/commodity_equivalents_final_0404.csv')

ncm8 <- as.vector(as.numeric(hs$code_value[hs$code_type == 'NCM_8']))
hs6 <- as.vector(as.numeric(hs$code_value[hs$code_type == 'HS_6']))
hs4 <- as.vector(as.numeric(hs$code_value[hs$code_type == 'HS_4']))
hs <- c(ncm8, hs6, hs4)

for (f in ff){

	j <- fread(f)

	countries <- c("Argentina", "Bolivia (Plurinational State of)", "Brazil", "Colombia", 
				"Costa Rica", "Ecuador", "Mexico", "Panama", "Paraguay", 
				"Peru", "Uruguay", "Venezuela")

	j <- j[(j$'Trade Flow' == 'Export') & (j$Reporter %in% countries) & (j$Partner == 'World')]

	j <- data.frame('country' = j$Reporter,
				'commodity' = as.integer(j$'Commodity Code'), 
				'comtrade_weight' = j$'Netweight (kg)')

	j <- j[j$commodity %in% hs,]

	# aggregate by country, hs code, sum weight
	data <- aggregate(j$comtrade_weight, by = list(country = j$country, hs = j$commodity), FUN = sum, na.rm = TRUE)

	names(data) <- c('country', 'hs', 'net_weight')

	write.csv2(j, paste0(f, '_zoom', ".csv"), quote = FALSE, row.names = FALSE)

}

