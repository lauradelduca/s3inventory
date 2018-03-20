# AWS check

 
library(aws.s3)
library(stringr)
library(gsubfn)
library(dplyr)

#options(scipen=999999999999999999999999999999999999999999)


		   
BOL_content <- get_bucket_df(bucket = 'trase-storage', prefix = 'data/1-TRADE/BoL/')

argentina_content <- get_bucket_df(bucket = 'trase-storage', prefix = 'data/1-TRADE/CD/EXPORT/ARGENTINA/')
argentina_content <- subset(argentina_content, grepl(".*/CD_[A-Z]+_[1-9][0-9]{3}.csv$", Key) )

bolivia_content <- get_bucket_df(bucket = 'trase-storage', prefix = 'data/1-TRADE/CD/EXPORT/BOLIVIA/')
bolivia_content <- subset(bolivia_content, grepl(".*/CD_[A-Z]+_[1-9][0-9]{3}.csv$", Key) )

brazil_content <- get_bucket_df(bucket = 'trase-storage', prefix = 'data/1-TRADE/CD/EXPORT/BRAZIL/')
brazil_content <- subset(brazil_content, grepl(".*/CD_[A-Z]+_[1-9][0-9]{3}.csv$", Key) )

chile_content <- get_bucket_df(bucket = 'trase-storage', prefix = 'data/1-TRADE/CD/EXPORT/CHILE/')
chile_content <- subset(chile_content, grepl(".*/CD_[A-Z]+_[1-9][0-9]{3}.csv$", Key) )

colombia_content <- get_bucket_df(bucket = 'trase-storage', prefix = 'data/1-TRADE/CD/EXPORT/COLOMBIA/')
colombia_content <- subset(colombia_content, grepl(".*/CD_[A-Z]+_[1-9][0-9]{3}.csv$", Key) )

costarica_content <- get_bucket_df(bucket = 'trase-storage', prefix = 'data/1-TRADE/CD/EXPORT/COSTA RICA/')
costarica_content <- subset(costarica_content, grepl(".*/CD_[A-Z]+_[1-9][0-9]{3}.csv$", Key) )

ecuador_content <- get_bucket_df(bucket = 'trase-storage', prefix = 'data/1-TRADE/CD/EXPORT/ECUADOR/')
ecuador_content <- subset(ecuador_content, grepl(".*/CD_[A-Z]+_[1-9][0-9]{3}.csv$", Key) )

mexico_content <- get_bucket_df(bucket = 'trase-storage', prefix = 'data/1-TRADE/CD/EXPORT/MEXICO/')
mexico_content <- subset(mexico_content, grepl(".*/CD_[A-Z]+_[1-9][0-9]{3}.csv$", Key) )

panama_content <- get_bucket_df(bucket = 'trase-storage', prefix = 'data/1-TRADE/CD/EXPORT/PANAMA/')
panama_content <- subset(panama_content, grepl(".*/CD_[A-Z]+_[1-9][0-9]{3}.csv$", Key) )

paraguay_content <- get_bucket_df(bucket = 'trase-storage', prefix = 'data/1-TRADE/CD/EXPORT/PARAGUAY/')
paraguay_content <- subset(paraguay_content, grepl(".*/CD_[A-Z]+_[1-9][0-9]{3}.csv$", Key) )

peru_content <- get_bucket_df(bucket = 'trase-storage', prefix = 'data/1-TRADE/CD/EXPORT/PERU/')
peru_content <- subset(peru_content, grepl(".*/CD_[A-Z]+_[1-9][0-9]{3}.csv$", Key) )

russia_content <- get_bucket_df(bucket = 'trase-storage', prefix = 'data/1-TRADE/CD/EXPORT/RUSSIA/')
russia_content <- subset(russia_content, grepl(".*/CD_[A-Z]+_[1-9][0-9]{3}.csv$", Key) )

uruguay_content <- get_bucket_df(bucket = 'trase-storage', prefix = 'data/1-TRADE/CD/EXPORT/URUGUAY/')
uruguay_content <- subset(uruguay_content, grepl(".*/CD_[A-Z]+_[1-9][0-9]{3}.csv$", Key) )

venezuela_content <- get_bucket_df(bucket = 'trase-storage', prefix = 'data/1-TRADE/CD/EXPORT/VENEZUELA/')
venezuela_content <- subset(venezuela_content, grepl(".*/CD_[A-Z]+_[1-9][0-9]{3}.csv$", Key) )


BOL_content <- subset(BOL_content, grepl(".*/BOL_[A-Z]+_[1-9][0-9]{3}.csv$", Key) )

CD_content <- do.call(rbind, list(	argentina_content,
									bolivia_content,
									brazil_content,
									chile_content,
									colombia_content,
									costarica_content,
									ecuador_content,
									mexico_content,
									panama_content,
									paraguay_content,
									peru_content,
									russia_content,
									uruguay_content,
									venezuela_content	))


BOL <- data.frame(	file = BOL_content$Key,
					type = strapplyc(BOL_content$Key, ".*/(BOL)_[A-Z]+_[1-9][0-9]{3}.csv$", simplify = TRUE),
					country = strapplyc(BOL_content$Key, ".*/BOL_([A-Z]+)_[1-9][0-9]{3}.csv$", simplify = TRUE),
					year = strapplyc(BOL_content$Key, ".*/BOL_[A-Z]+_([1-9][0-9]{3}).csv$", simplify = TRUE)  )

BOL <- BOL[order(BOL$country, BOL$year),]

					
CD <- data.frame(	file = CD_content$Key,
					type = strapplyc(CD_content$Key, ".*/(CD)_[A-Z]+_[1-9][0-9]{3}.csv$", simplify = TRUE),
					country = strapplyc(CD_content$Key, ".*/CD_([A-Z]+)_[1-9][0-9]{3}.csv$", simplify = TRUE),
					year = strapplyc(CD_content$Key, ".*/CD_[A-Z]+_([1-9][0-9]{3}).csv$", simplify = TRUE)  )

CD <- CD[order(CD$country, CD$year),]	


## check codes ----------------------------------------------------------------------------------------------------------------------------

obj <- get_object(object = 'data/1-TRADE/commodity_equivalents_final.csv', bucket = 'trase-storage')

hs <- read.csv(text = rawToChar(obj), sep = ';', quote = '',
				colClasses = c("character", "character", "character", 
				"character", "character", "character", "numeric", 
				"numeric"))

ncm8 <- as.vector(as.numeric(hs$code_value[hs$code_type == 'NCM_8']))
hs6 <- as.vector(as.numeric(hs$code_value[hs$code_type == 'HS_6']))


## ----------------------------------------------------------------------------------------------------------------------------------------

countries <- c()


for (f in as.vector(CD$file)){
	
	obj <- get_object(object = f, bucket = 'trase-storage')
	
	
	###########################  dictionary: HS, price and weight columns  ################################################################
	
	if (CD$country[CD$file == f] == 'ARGENTINA'){
	
		CD$hs_column[CD$file == f] <- hs_column <- 'HARMONIZED_CODE_PRODUCT_ENGLISH'
		CD$price_column[CD$file == f] <- price_column <- 'TOTAL_FOB_VALUE_US'
		CD$weight_column[CD$file == f] <- weight_column <- 'TOTAL_NET_WEIGHT_KG'
		
		release <- c('BEEF', 'CHICKEN', 'CORN', 'COTTON', 'LEATHER (CATTLE)', 
					'NATURAL TIMBER', 'PAPER AND PULP', 'SHRIMPS', 'SOY', 'SUGARCANE')
		
		countries <- unique(c(countries, as.character(CD$country[CD$file == f]))) }
	
	if (CD$country[CD$file == f] == 'BOLIVIA'){
	
		CD$hs_column[CD$file == f] <- hs_column <- 'COD_ARMONIZADOPRODUCTO_INGLES'
		CD$price_column[CD$file == f] <- price_column <- 'TOTAL_VALOR_FOB_US'
		CD$weight_column[CD$file == f] <- weight_column <- 'TOTAL_PESO_NETO_KG'
		
		release <- c('CHICKEN', 'COFFEE', 'CORN', 'LEATHER (CATTLE)', 
					'NATURAL TIMBER', 'SOY')
		
		countries <- unique(c(countries, as.character(CD$country[CD$file == f]))) }
		
	if (grepl("data/1-TRADE/CD/EXPORT/BRAZIL/DATAMYNE/DASHBOARD/", f)){
	
		CD$hs_column[CD$file == f] <- hs_column <- 'PRODUCT_HS'
		CD$price_column[CD$file == f] <- price_column <- 'FOB_VALUE_USD'
		CD$weight_column[CD$file == f] <- weight_column <- 'NET_WEIGHT'
		
		release <- c('BEEF', 'CHICKEN', 'COCOA', 'COFFEE', 'CORN', 'COTTON', 'LEATHER (CATTLE)', 
					'NATURAL TIMBER', 'OIL PALM', 'PAPER AND PULP', 'SOY', 'SUGARCANE')
		
		countries <- unique(c(countries, as.character(CD$country[CD$file == f]))) }
		
	if (grepl("data/1-TRADE/CD/EXPORT/BRAZIL/DATAMYNE/THIRD_PARTY/", f)){
	
		CD$hs_column[CD$file == f] <- hs_column <- 'COD_SUBITEM_NCM'
		CD$price_column[CD$file == f] <- price_column <- 'VMLE_DOLAR_BAL_EXP'
		CD$weight_column[CD$file == f] <- weight_column <- 'PESO_LIQ_MERC_BAL_EXP'
		
		release <- c('BEEF', 'CHICKEN', 'COCOA', 'COFFEE', 'CORN', 'COTTON', 'LEATHER (CATTLE)', 
					'NATURAL TIMBER', 'OIL PALM', 'PAPER AND PULP', 'SOY', 'SUGARCANE')
		
		countries <- unique(c(countries, as.character(CD$country[CD$file == f]))) }
		
	if (CD$country[CD$file == f] == 'CHILE'){
	
		CD$hs_column[CD$file == f] <- hs_column <- 'HARMONIZED_CODEPRODUCT_ENGLISH'
		CD$price_column[CD$file == f] <- price_column <- 'TOTAL_FOB_VALUE_US'
		CD$weight_column[CD$file == f] <- weight_column <- 'TOTAL_NET_WEIGHT_KG'
		
		release <- c()
		
		countries <- unique(c(countries, as.character(CD$country[CD$file == f]))) }
		
	if (CD$country[CD$file == f] == 'COLOMBIA'){
	
		CD$hs_column[CD$file == f] <- hs_column <- 'HARMONIZED_CODE_PRODUCT_ENGLISH'
		CD$price_column[CD$file == f] <- price_column <- 'TOTAL_FOB_VALUE_US'
		CD$weight_column[CD$file == f] <- weight_column <- 'TOTAL_NET_WEIGHT_KG'
		
		release <- c('BEEF', 'CHICKEN', 'COCOA', 'COFFEE', 'CORN', 'LEATHER (CATTLE)', 
					'NATURAL TIMBER', 'OIL PALM', 'PAPER AND PULP', 'SHRIMPS', 'SUGARCANE')
		
		countries <- unique(c(countries, as.character(CD$country[CD$file == f]))) }
		
	if (CD$country[CD$file == f] == 'COSTARICA'){
	
		CD$hs_column[CD$file == f] <- hs_column <- 'HARMONIZED_CODE_PRODUCT_ENGLISH'
		CD$price_column[CD$file == f] <- price_column <- 'TOTAL_CIF_VALUE_US'
		CD$weight_column[CD$file == f] <- weight_column <- 'TOTAL_NET_WEIGHT_KG'
		
		release <- c('BEEF', 'COFFEE', 'LEATHER (CATTLE)', 
					'NATURAL TIMBER', 'OIL PALM', 'SHRIMPS', 'SOY', 'SUGARCANE')
		
		countries <- unique(c(countries, as.character(CD$country[CD$file == f]))) }	
		
	if (CD$country[CD$file == f] == 'ECUADOR'){
	
		CD$hs_column[CD$file == f] <- hs_column <- 'HARMONIZED_CODEPRODUCT_SPANISH'
		CD$price_column[CD$file == f] <- price_column <- 'TOTAL_FOB_VALUE_US'
		CD$weight_column[CD$file == f] <- weight_column <- 'TOTAL_NET_WEIGHT_KG'
		
		release <- c('COCOA', 'COFFEE', 'LEATHER (CATTLE)', 
					'OIL PALM', 'PAPER AND PULP', 'SHRIMPS')
		
		countries <- unique(c(countries, as.character(CD$country[CD$file == f]))) }
		
	if (CD$country[CD$file == f] == 'MEXICO'){
	
		CD$hs_column[CD$file == f] <- hs_column <- 'HARMONIZED_CODE_PRODUCT_ENGLISH'
		CD$price_column[CD$file == f] <- price_column <- 'TOTAL_FOB_VALUE_US'
		CD$weight_column[CD$file == f] <- weight_column <- 'TOTAL_QUANTITY_1'
		
		release <- c('BEEF', 'CHICKEN', 'COCOA', 'COFFEE', 'CORN', 'COTTON', 'LEATHER (CATTLE)', 
					'NATURAL TIMBER', 'PAPER AND PULP', 'SHRIMPS', 'SOY', 'SUGARCANE')
		
		countries <- unique(c(countries, as.character(CD$country[CD$file == f]))) }		

	if (CD$country[CD$file == f] == 'PANAMA'){
	
		CD$hs_column[CD$file == f] <- hs_column <- 'HARMONIZED_CODE_PRODUCT_ENGLISH'
		CD$price_column[CD$file == f] <- price_column <- 'TOTAL_FOB_VALUE_US'
		CD$weight_column[CD$file == f] <- weight_column <- 'TOTAL_NET_WEIGHT_KG'
		
		release <- c('COFFEE', 'LEATHER (CATTLE)', 
					'NATURAL TIMBER', 'OIL PALM', 'SHRIMPS', 'SUGARCANE')		
		
		countries <- unique(c(countries, as.character(CD$country[CD$file == f]))) }
	
	if (grepl("data/1-TRADE/CD/EXPORT/PARAGUAY/SICEX/", f)){
	
		CD$hs_column[CD$file == f] <- hs_column <- 'Harmonized CodeProduct English'
		CD$price_column[CD$file == f] <- price_column <- 'TOTAL FOB Value US'
		CD$weight_column[CD$file == f] <- weight_column <- 'TOTAL Net Weight Kg'
		
		release <- c()
		
		countries <- unique(c(countries, as.character(CD$country[CD$file == f]))) }
		
	if (grepl("data/1-TRADE/CD/EXPORT/PARAGUAY/MINTRADE/", f)){
	
		CD$hs_column[CD$file == f] <- hs_column <- 'hs8'
		CD$price_column[CD$file == f] <- price_column <- 'Valor Fob Dolar'
		CD$weight_column[CD$file == f] <- weight_column <- 'Kilo Neto'
		
		release <- c('BEEF', 'CORN', 'LEATHER (CATTLE)', 
					'NATURAL TIMBER', 'SOY', 'SUGARCANE')
		
		countries <- unique(c(countries, as.character(CD$country[CD$file == f]))) }
		
	if (CD$country[CD$file == f] == 'PERU'){
	
		CD$hs_column[CD$file == f] <- hs_column <- 'COD..ARMONIZADOPRODUCTO.INGLES'
		CD$price_column[CD$file == f] <- price_column <- 'TOTAL.VALOR.FOB.US'
		CD$weight_column[CD$file == f] <- weight_column <- 'TOTAL.PESO.NETO.KG'
		
		release <- c('CHICKEN', 'COCOA', 'COFFEE', 'CORN', 'LEATHER (CATTLE)', 
					'NATURAL TIMBER', 'OIL PALM', 'SHRIMPS', 'SUGARCANE')
		
		countries <- unique(c(countries, as.character(CD$country[CD$file == f]))) }
	
	if (CD$country[CD$file == f] == 'URUGUAY'){
	
		CD$hs_column[CD$file == f] <- hs_column <- 'HARMONIZED_CODEPRODUCT_ENGLISH'
		CD$price_column[CD$file == f] <- price_column <- 'TOTAL_FOB_VALUE_US'
		CD$weight_column[CD$file == f] <- weight_column <- 'TOTAL_NET_WEIGHT_KG'
		
		release <- c('BEEF', 'CHICKEN', 'COCOA', 'CORN', 'LEATHER (CATTLE)', 
					'PAPER AND PULP', 'SOY')
		
		countries <- unique(c(countries, as.character(CD$country[CD$file == f]))) }
	
	if (CD$country[CD$file == f] == 'VENEZUELA'){
	
		CD$hs_column[CD$file == f] <- hs_column <- 'HARMONIZED_CODE_PRODUCT_SPANISH'
		CD$price_column[CD$file == f] <- price_column <- 'TOTAL_FOB_VALUE_US'
		CD$weight_column[CD$file == f] <- weight_column <- 'TOTAL_NET_WEIGHT_KG'
		
		release <- c('COCOA', 'LEATHER (CATTLE)', 
					'PAPER AND PULP', 'SHRIMPS')
		
		countries <- unique(c(countries, as.character(CD$country[CD$file == f]))) }
		
	
	CD$release[CD$file == f] <- paste(release, collapse=", ")
	
	## filter for codes relevant for next release -----------------------------------------------------------------------------------------
	
	hs6_release <- as.vector(as.numeric(hs$code_value[ (hs$code_type == 'HS_6') & (hs$prod_name %in% release) ]))
	
	
	
	#####  ARGENTINA, BOLIVIA, CHILE, COLOMBIA, COSTARICA, ECUADOR, MEXICO, PANAMA, PARAGUAY, PERU, URUGUAY, VENEZUELA  ###################

	if (CD$country[CD$file == f] %in% countries){
	
		data <- read.csv(text = rawToChar(obj), sep = ';', quote = '')
		
		## commodities for release --------------------------------------------------------------------------------------------------------
		
		CD$release[CD$file == f] <- paste(release, collapse=", ")
		
		## check codes --------------------------------------------------------------------------------------------------------------------
	
		if (grepl("data/1-TRADE/CD/EXPORT/PARAGUAY/MINTRADE/", CD$file)){
			data$hs8 <- as.integer(substr(gsub('\\.', '', data$NCM, perl=TRUE), 0, 8))
		}
	
		codes <- as.vector(as.numeric(substr(data[, hs_column], 1, 6)))
		CD$missing[CD$file == f] <- paste(hs6_release[!(hs6_release %in% codes)], collapse=", ")
		
		## check if weight == 0 -----------------------------------------------------------------------------------------------------------
		
		zero_codes <- as.vector(as.numeric(data[, hs_column][data[, weight_column] == 0]))
		CD$weight_zero[CD$file == f] <- paste(unique(zero_codes), collapse=", ")
		
		##  unit checks: deviation from mean ratio  ---------------------------------------------------------------------------------------
		
		ratios <- data.frame(	commodity = as.numeric(data[, hs_column]), 
								ratio = as.numeric(data[, price_column]) / as.numeric(data[, weight_column]))
		ratios <- ratios[!is.na(ratios$ratio),]
		ratios$mean_ratio <- ave(ratios$ratio, ratios$commodity, FUN = function(x) mean(x, na.rm = TRUE))
		
		units_codes <- c()
		for (i in 1:nrow(ratios)){
			if ( (ratios$ratio[i] >= (12 * ratios$mean_ratio[i])) || (ratios$ratio[i] <= (1/12 * ratios$mean_ratio[i])) ){
				units_codes <- c(units_codes, ratios$commodity[i])
			}
		}
		CD$units[CD$file == f] <- paste(unique(units_codes), collapse=", ")
					
		
		## check weights with COMTRADE ------------------------------------------------------------------
		
		weight <- data.frame(commodity = as.integer(substr(data[, hs_column], 1, 6)), weight = as.numeric(data[, weight_column]))
		weight <- aggregate(weight$weight, by = list(weight$commodity), FUN = sum, na.rm = TRUE)
		colnames(weight) <- c('commodity', 'total_weight')
		
		## get comtrade data from s3
		# COMTRADE 2017 not yet available; compare 2017/2018 to COMTRADE 2016
		
		if (CD$year[CD$file == f] == 2017 | CD$year[CD$file == f] == 2018){
			y <- paste('data/1-TRADE/STATISTICAL_DATA/GLOBAL/COMTRADE/COMTRADE_ZOOMIN/COMTRADE_ZOOMIN_', '2016', '.csv', sep = '')
		} else {
			y <- paste('data/1-TRADE/STATISTICAL_DATA/GLOBAL/COMTRADE/COMTRADE_ZOOMIN/COMTRADE_ZOOMIN_', 
			as.character(CD$year[CD$file == f]), '.csv', sep = '')
		}
		
		obj <- get_object(object = y, bucket = 'trase-storage')
		comtrade <- read.csv(text = rawToChar(obj), quote = '', sep = ';')
		comtrade$country <- toupper(comtrade$country)
		comtrade <- comtrade[comtrade$country == CD$country[CD$file == f],]
		comtrade <- subset(comtrade, select = -c(country))
		comtrade <- aggregate(comtrade$comtrade_weight, by = list(comtrade$commodity), FUN = sum, na.rm = TRUE)
		names(comtrade) <- c('commodity', 'comtrade_weight')

		weight <- left_join(weight, comtrade, by = 'commodity')
		weight_codes <- c()
		
		for (i in 1:nrow(weight)){
			
			if ( (!is.na(weight$total_weight[i])) & (!is.na(weight$comtrade_weight[i])) & 
			( (weight$comtrade_weight[i] >= (1.5 * weight$total_weight)) || 
			(weight$comtrade_weight[i] <= (0.5 * weight$total_weight)) )) {
			
				weight_codes <- c(weight_codes, weight$commodity[i])
			} 
		}
		
		CD$comtrade_weight[CD$file == f] <- paste(unique(weight_codes), collapse=", ")
		
		
		## check for commas in price and weight ------------------------------------------------------------------------------
		
		price_comma_codes <- c()
		for (i in 1:nrow(data)){
			if ( grepl(',', data[, price_column][i]) ) { price_comma_codes <- c(price_comma_codes, data[, hs_column][i]) }}
		CD$price_comma[CD$file == f] <- paste(unique(price_comma_codes), collapse=", ")
		
		weight_comma_codes <- c()
		for (i in 1:nrow(data)){
			if ( grepl(',', data[, weight_column][i])) { weight_comma_codes <- c(weight_comma_codes, data[, hs_column][i]) }}		
		CD$weight_comma[CD$file == f] <- paste(unique(weight_comma_codes), collapse=", ")
		
	}
	
}


#CD_commodities <- CD[CD$year != 2018]

#CD_commodities$hs_column <- NULL
#CD_commodities$price_column <- NULL
#CD_commodities$weight_column <- NULL

#write.csv2(CD_commodities, 'CD_release.csv', quote = FALSE, row.names = FALSE)

# write file
write.csv2(BOL, 'BOL_check.csv', quote = FALSE, row.names = FALSE)
write.csv2(CD, 'CD_check.csv', quote = FALSE, row.names = FALSE)

