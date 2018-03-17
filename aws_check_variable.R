# AWS check

 
library(aws.s3)
library(stringr)
library(gsubfn)
library(dplyr)

options(scipen=999999999999999999999999999999999999999999)

# include check with row== weight 0 # done for argentina


Sys.setenv("AWS_ACCESS_KEY_ID" = "X",
           "AWS_SECRET_ACCESS_KEY" = "X",
           "AWS_DEFAULT_REGION" = "X")
		   
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


## check codes ---------------------------------------------------------------------------------------------------

obj <- get_object(object = 'data/1-TRADE/commodity_equivalents_final.csv', bucket = 'trase-storage')

hs <- read.csv(text = rawToChar(obj), sep = ';', quote = '',
				colClasses = c("character", "character", "character", 
				"character", "character", "character", "numeric", 
				"numeric"))

ncm8 <- as.vector(as.numeric(hs$code_value[hs$code_type == 'NCM_8']))
hs6 <- as.vector(as.numeric(hs$code_value[hs$code_type == 'HS_6']))



## check units (weight/price ratios) ------------------------------------------------------------------------------

units <- data.frame( commodity = c(ncm8, hs6))



for (f in as.vector(CD$file)){
	
	obj <- get_object(object = f, bucket = 'trase-storage')
	
	
	###########################  create dictionary: HS, price and weight columns  #########################################################
	
	if (CD$country[CD$file == f] == 'ARGENTINA'){
	
		CD$hs_column[CD$file == f] <- hs_column <- 'HARMONIZED_CODE_PRODUCT_ENGLISH'
		CD$price_column[CD$file == f] <- price_column <- 'TOTAL_FOB_VALUE_US'
		CD$weight_column[CD$file == f] <- weight_column <- 'TOTAL_NET_WEIGHT_KG'
	
	}
	
	
	################################  ARGENTINA  ##########################################################################################

	if (CD$country[CD$file == f] == 'ARGENTINA'){
	
		data <- read.csv(text = rawToChar(obj), sep = ';', quote = '')
				#colClasses = c("character", "character", "character", 
				#"character", "character", "numeric", "character", 
				#"character", "character", "character", "numeric", 
				#"character"))
		
		## check codes ----------------------------------------------------------------------------
	
		codes <- as.vector(as.numeric(data$hs_column))
	
		CD$missing[CD$file == f] <- paste(hs6[!(hs6 %in% codes)], collapse=", ")
		
		## check if weight == 0 -------------------------------------------------------------------
		
		zero_codes <- as.vector(as.numeric(data$hs_column[data$TOTAL_NET_WEIGHT_KG == 0]))
		
		CD$weight_zero[CD$file == f] <- paste(zero_codes, collapse=", ")
		
		## check if for release it's all there
		
		## check units ----------------------------------------------------------------------------
		
		####### change test
		
		agg_help <- data.frame(	commodity = as.numeric(data$hs_column), 
								f = as.numeric(data$TOTAL_FOB_VALUE_US) / as.numeric(data$TOTAL_NET_WEIGHT_KG ))
		colnames(agg_help) <- c('commodity', f)
		
		agg <- aggregate(agg_help, by = list(agg_help$commodity), FUN = mean, na.rm = TRUE)[,2:3]
		
		units <- left_join(units, agg, by = 'commodity')
		units[units==Inf]<-NA
		
		units_codes <- c()
		
		if (ncol(units) > 2){
		
			j <- ncol(units)
		
			units$tmp1 <- units[, j] / units[, j-1]
			if (j > 3) { units$tmp2 <- units[, j] / units[, j-2] }
			
			for (i in 1:nrow(units)){
			
				if ( (!is.na(units$tmp1[i])) & ( (units$tmp1[i] >=3) || (units$tmp1[i] <= 1/3) )) {
					units_codes <- c(units_codes, units$commodity[i])
				}
				
				if ('tmp2' %in% names(units)){
					if ( (!is.na(units$tmp1[i])) & ( (units$tmp1[i] >=3) || (units$tmp1[i] <= 1/3) )) {
						units_codes <- c(units_codes, units$commodity[i])
					}
				}

			}				
			
			CD$units[CD$file == f] <- paste(unique(units_codes), collapse=", ")
			
			units = subset(units, select = -c(tmp1) )
			if ('tmp2' %in% names(units)){ units = subset(units, select = -c(tmp2)) }
		
		}
		
		
		## check weights with COMTRADE ------------------------------------------------------------------
		
		weight <- data.frame(commodity = as.integer(substr(data$hs_column, 1, 6)), weight = as.numeric(data$TOTAL_NET_WEIGHT_KG))
		#weight <- data.frame(commodity = as.integer(data$HARMONIZED_CODE_PRODUCT_ENGLISH), weight = data$TOTAL_NET_WEIGHT_KG)
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
		
		#### relax a bit # done for argentina
		
		for (i in 1:nrow(weight)){
			
			if ( (!is.na(weight$total_weight[i])) & (!is.na(weight$comtrade_weight[i])) & 
			( (weight$comtrade_weight[i] >= (1.3 * weight$total_weight)) || 
			(weight$comtrade_weight[i] <= (0.7 * weight$total_weight)) )) {
			
				weight_codes <- c(weight_codes, weight$commodity[i]) 
			} 
		}
		
		CD$comtrade_weight[CD$file == f] <- paste(unique(weight_codes), collapse=", ")
		
		
		## check for commas in price and weight ------------------------------------------------------------------------------
		
		price_comma_codes <- c()
		for (i in 1:nrow(data)){
			if ( grepl(',', data$TOTAL_FOB_VALUE_US[i]) ) { price_comma_codes <- c(price_comma_codes, data$hs_column[i]) }}
		CD$price_comma[CD$file == f] <- paste(unique(price_comma_codes), collapse=", ")
		
		weight_comma_codes <- c()
		for (i in 1:nrow(data)){
			if ( grepl(',', data$TOTAL_NET_WEIGHT_KG[i])) { weight_comma_codes <- c(weight_comma_codes, data$hs_column[i]) }}		
		CD$weight_comma[CD$file == f] <- paste(unique(weight_comma_codes), collapse=", ")
		
	}
	
	
	################################  BOLIVIA  ##########################################################################################

	if (CD$country[CD$file == f] == 'BOLIVIA'){
	
		data <- read.csv(text = rawToChar(obj), sep = ';', quote = '')
				#colClasses = c("character", "character", "character", 
				#"character", "character", "numeric", "character", 
				#"character", "character", "character", "numeric", 
				#"character"))
		
		## check codes ----------------------------------------------------------------------------
	
		codes <- as.vector(as.numeric(data$COD_ARMONIZADOPRODUCTO_INGLES))
	
		CD$missing[CD$file == f] <- paste(hs6[!(hs6 %in% codes)], collapse=", ")
		
		## check units ----------------------------------------------------------------------------
		
		agg_help <- data.frame(	commodity = as.numeric(data$COD_ARMONIZADOPRODUCTO_INGLES), 
								f = data$TOTAL_VALOR_FOB_US / data$TOTAL_PESO_NETO_KG )
		colnames(agg_help) <- c('commodity', f)
		
		agg <- aggregate(agg_help, by = list(agg_help$commodity), FUN = mean, na.rm = TRUE)[,2:3]
		
		units <- left_join(units, agg, by = 'commodity')
		units[units==Inf]<-NA
		
		units_codes <- c()
		
		if (ncol(units) > 2){
		
			j <- ncol(units)
		
			units$tmp1 <- units[, j] / units[, j-1]
			if (j > 3) { units$tmp2 <- units[, j] / units[, j-2] }
			
			for (i in 1:nrow(units)){
			
				if ( (!is.na(units$tmp1[i])) & ( (units$tmp1[i] >=3) || (units$tmp1[i] <= 1/3) )) {
					units_codes <- c(units_codes, units$commodity[i])
				}
				
				if ('tmp2' %in% names(units)){
					if ( (!is.na(units$tmp1[i])) & ( (units$tmp1[i] >=3) || (units$tmp1[i] <= 1/3) )) {
						units_codes <- c(units_codes, units$commodity[i])
					}
				}

			}				
			
			CD$units[CD$file == f] <- paste(unique(units_codes), collapse=", ")
			
			units = subset(units, select = -c(tmp1) )
			if ('tmp2' %in% names(units)){ units = subset(units, select = -c(tmp2)) }
		
		}
		
		
		## check weights with COMTRADE ------------------------------------------------------------------
		
		weight <- data.frame(commodity = as.integer(substr(data$COD_ARMONIZADOPRODUCTO_INGLES, 1, 6)), weight = data$TOTAL_PESO_NETO_KG)
		#weight <- data.frame(commodity = as.integer(data$COD_ARMONIZADOPRODUCTO_INGLES), weight = data$TOTAL_PESO_NETO_KG)
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
		if (nrow(comtrade) != 0){comtrade <- aggregate(comtrade$comtrade_weight, by = list(comtrade$commodity), FUN = sum, na.rm = TRUE)}
		names(comtrade) <- c('commodity', 'comtrade_weight')

		
		weight <- left_join(weight, comtrade, by = 'commodity')
		weight_codes <- c()
		
		for (i in 1:nrow(weight)){
			
			if ( (!is.na(weight$total_weight[i])) & (!is.na(weight$comtrade_weight[i])) & 
			( (weight$comtrade_weight[i] >= (1.2 * weight$total_weight)) || 
			(weight$comtrade_weight[i] <= (0.8 * weight$total_weight)) )) {
			
				weight_codes <- c(weight_codes, weight$commodity[i]) 
			} 
		}
		
		CD$comtrade_weight[CD$file == f] <- paste(unique(weight_codes), collapse=", ")
		
		
		## check for commas in price and weight ------------------------------------------------------------------------------
		
		price_comma_codes <- c()
		for (i in 1:nrow(data)){
			if ( grepl(',', data$TOTAL_VALOR_FOB_US[i]) ) { price_comma_codes <- c(price_comma_codes, data$COD_ARMONIZADOPRODUCTO_INGLES[i]) }}
		CD$price_comma[CD$file == f] <- paste(unique(price_comma_codes), collapse=", ")
		
		weight_comma_codes <- c()
		for (i in 1:nrow(data)){
			if ( grepl(',', data$TOTAL_PESO_NETO_KG[i])) { weight_comma_codes <- c(weight_comma_codes, data$COD_ARMONIZADOPRODUCTO_INGLES[i]) }}		
		CD$weight_comma[CD$file == f] <- paste(unique(weight_comma_codes), collapse=", ")
		
	}
	
	
	################################  BRAZIL  ##########################################################################################

	if (CD$country[CD$file == f] == 'BRAZIL'){
	
		data <- read.csv(text = rawToChar(obj), sep = ';', quote = '')
				#colClasses = c("character", "character", "character", 
				#"character", "character", "numeric", "character", 
				#"character", "character", "character", "numeric", 
				#"character"))
				
		## check codes ----------------------------------------------------------------------------
	
		if (grepl("data/1-TRADE/CD/EXPORT/BRAZIL/DATAMYNE/DASHBOARD/", f)){
			codes <- as.vector(as.numeric(data$PRODUCT_HS))
		}
		if (grepl("data/1-TRADE/CD/EXPORT/BRAZIL/DATAMYNE/THIRD_PARTY/", CD$file)){
			codes <- as.vector(as.numeric(data$COD_SUBITEM_NCM))
		}
	
		CD$missing[CD$file == f] <- paste(ncm8[!(ncm8 %in% codes)], collapse=", ")
		
		## check units ----------------------------------------------------------------------------
		
		if (grepl("data/1-TRADE/CD/EXPORT/BRAZIL/DATAMYNE/DASHBOARD/", CD$file)){
			agg_help <- data.frame(	commodity = as.numeric(data$PRODUCT_HS), 
									f = data$FOB_VALUE_USD / data$NET_WEIGHT)
		}
		if (grepl("data/1-TRADE/CD/EXPORT/BRAZIL/DATAMYNE/THIRD_PARTY/", CD$file)){
			agg_help <- data.frame(	commodity = as.numeric(data$COD_SUBITEM_NCM), 
									f = data$VMLE_DOLAR_BAL_EXP / data$PESO_LIQ_MERC_BAL_EXP)
		}
		
		colnames(agg_help) <- c('commodity', f)
		
		agg <- aggregate(agg_help, by = list(agg_help$commodity), FUN = mean, na.rm = TRUE)[,2:3]
		
		units <- left_join(units, agg, by = 'commodity')
		units[units==Inf]<-NA
		
		units_codes <- c()
		
		if (ncol(units) > 2){
		
			j <- ncol(units)
		
			units$tmp1 <- units[, j] / units[, j-1]
			if (j > 3) { units$tmp2 <- units[, j] / units[, j-2] }
			
			for (i in 1:nrow(units)){
			
				if ( (!is.na(units$tmp1[i])) & ( (units$tmp1[i] >=3) || (units$tmp1[i] <= 1/3) )) {
					units_codes <- c(units_codes, units$commodity[i])
				}
				
				if ('tmp2' %in% names(units)){
					if ( (!is.na(units$tmp1[i])) & ( (units$tmp1[i] >=3) || (units$tmp1[i] <= 1/3) )) {
						units_codes <- c(units_codes, units$commodity[i])
					}
				}

			}				
			
			CD$units[CD$file == f] <- paste(unique(units_codes), collapse=", ")
			
			units = subset(units, select = -c(tmp1) )
			if ('tmp2' %in% names(units)){ units = subset(units, select = -c(tmp2)) }
		
		}
		
		
		
		## check weights with COMTRADE ------------------------------------------------------------------
		
		if (grepl("data/1-TRADE/CD/EXPORT/BRAZIL/DATAMYNE/DASHBOARD/", CD$file)){
			weight <- data.frame(commodity = as.integer(substr(data$PRODUCT_HS, 1, 6)), weight = data$NET_WEIGHT)
		}
		if (grepl("data/1-TRADE/CD/EXPORT/BRAZIL/DATAMYNE/THIRD_PARTY/", CD$file)){
			weight <- data.frame(commodity = as.integer(substr(data$COD_SUBITEM_NCM, 1, 6)), weight = data$PESO_LIQ_MERC_BAL_EXP)
		}
		
		
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
			( (weight$comtrade_weight[i] >= (1.2 * weight$total_weight)) || 
			(weight$comtrade_weight[i] <= (0.8 * weight$total_weight)) )) {
			
				weight_codes <- c(weight_codes, weight$commodity[i]) 
			} 
		}
		
		CD$comtrade_weight[CD$file == f] <- paste(unique(weight_codes), collapse=", ")
		
		
		## check for commas in price and weight ------------------------------------------------------------------------------
		
		price_comma_codes <- c()
		if (grepl("data/1-TRADE/CD/EXPORT/BRAZIL/DATAMYNE/DASHBOARD/", CD$file)){
			for (i in 1:nrow(data)){
				if ( grepl(',', data$FOB_VALUE_USD[i]) ) { price_comma_codes <- c(price_comma_codes, data$PRODUCT_HS[i]) }}
		}
		if (grepl("data/1-TRADE/CD/EXPORT/BRAZIL/DATAMYNE/THIRD_PARTY/", CD$file)){
			for (i in 1:nrow(data)){
				if ( grepl(',', data$VMLE_DOLAR_BAL_EXP[i]) ) { price_comma_codes <- c(price_comma_codes, data$PCOD_SUBITEM_NCM[i]) }}
		}
		CD$price_comma[CD$file == f] <- paste(unique(price_comma_codes), collapse=", ")
		
		weight_comma_codes <- c()
		if (grepl("data/1-TRADE/CD/EXPORT/BRAZIL/DATAMYNE/DASHBOARD/", CD$file)){
			for (i in 1:nrow(data)){
				if ( grepl(',', data$NET_WEIGHT[i])) { weight_comma_codes <- c(weight_comma_codes, data$PRODUCT_HS[i]) }}
		}
		if (grepl("data/1-TRADE/CD/EXPORT/BRAZIL/DATAMYNE/THIRD_PARTY/", CD$file)){
			for (i in 1:nrow(data)){
				if ( grepl(',', data$PESO_LIQ_MERC_BAL_EXP[i])) { weight_comma_codes <- c(weight_comma_codes, data$PCOD_SUBITEM_NCM[i]) }}
		}		
		CD$weight_comma[CD$file == f] <- paste(unique(weight_comma_codes), collapse=", ")
		
		
	}
	
	
	################################  CHILE  ##########################################################################################

	if (CD$country[CD$file == f] == 'CHILE'){
	
		data <- read.csv(text = rawToChar(obj), sep = ';', quote = '')
				#colClasses = c("character", "character", "character", 
				#"character", "character", "numeric", "character", 
				#"character", "character", "character", "numeric", 
				#"character"))
		
		## check codes ----------------------------------------------------------------------------
	
		codes <- as.vector(as.numeric(data$HARMONIZED_CODEPRODUCT_ENGLISH))
	
		CD$missing[CD$file == f] <- paste(hs6[!(hs6 %in% codes)], collapse=", ")
		
		## check units ----------------------------------------------------------------------------
		
		agg_help <- data.frame(	commodity = as.numeric(data$HARMONIZED_CODEPRODUCT_ENGLISH), 
								f = data$TOTAL_FOB_VALUE_US / data$TOTAL_NET_WEIGHT_KG )
		colnames(agg_help) <- c('commodity', f)
		
		agg <- aggregate(agg_help, by = list(agg_help$commodity), FUN = mean, na.rm = TRUE)[,2:3]
		
		units <- left_join(units, agg, by = 'commodity')
		units[units==Inf]<-NA
		
		units_codes <- c()
		
		if (ncol(units) > 2){
		
			j <- ncol(units)
		
			units$tmp1 <- units[, j] / units[, j-1]
			if (j > 3) { units$tmp2 <- units[, j] / units[, j-2] }
			
			for (i in 1:nrow(units)){
			
				if ( (!is.na(units$tmp1[i])) & ( (units$tmp1[i] >=3) || (units$tmp1[i] <= 1/3) )) {
					units_codes <- c(units_codes, units$commodity[i])
				}
				
				if ('tmp2' %in% names(units)){
					if ( (!is.na(units$tmp1[i])) & ( (units$tmp1[i] >=3) || (units$tmp1[i] <= 1/3) )) {
						units_codes <- c(units_codes, units$commodity[i])
					}
				}

			}				
			
			CD$units[CD$file == f] <- paste(unique(units_codes), collapse=", ")
			
			units = subset(units, select = -c(tmp1) )
			if ('tmp2' %in% names(units)){ units = subset(units, select = -c(tmp2)) }
		
		}
		
	
		## check weights with COMTRADE ------------------------------------------------------------------
		
		weight <- data.frame(commodity = as.integer(substr(data$HARMONIZED_CODEPRODUCT_ENGLISH, 1, 6)), weight = data$TOTAL_NET_WEIGHT_KG)
		#weight <- data.frame(commodity = as.integer(data$HARMONIZED_CODEPRODUCT_ENGLISH), weight = data$TOTAL_NET_WEIGHT_KG)
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
			( (weight$comtrade_weight[i] >= (1.2 * weight$total_weight)) || 
			(weight$comtrade_weight[i] <= (0.8 * weight$total_weight)) )) {
			
				weight_codes <- c(weight_codes, weight$commodity[i]) 
			} 
		}
		
		CD$comtrade_weight[CD$file == f] <- paste(unique(weight_codes), collapse=", ")
		
		
		## check for commas in price and weight ------------------------------------------------------------------------------
		
		price_comma_codes <- c()
		for (i in 1:nrow(data)){
			if ( grepl(',', data$TOTAL_FOB_VALUE_US[i]) ) { price_comma_codes <- c(price_comma_codes, data$HARMONIZED_CODEPRODUCT_ENGLISH[i]) }}
		CD$price_comma[CD$file == f] <- paste(unique(price_comma_codes), collapse=", ")
		
		weight_comma_codes <- c()
		for (i in 1:nrow(data)){
			if ( grepl(',', data$TOTAL_NET_WEIGHT_KG[i])) { weight_comma_codes <- c(weight_comma_codes, data$HARMONIZED_CODEPRODUCT_ENGLISH[i]) }}		
		CD$weight_comma[CD$file == f] <- paste(unique(weight_comma_codes), collapse=", ")
		
			
	}
	

	################################  COLOMBIA  ##########################################################################################

	if (CD$country[CD$file == f] == 'COLOMBIA'){
	
		data <- read.csv(text = rawToChar(obj), sep = ';', quote = '')
				#colClasses = c("character", "character", "character", 
				#"character", "character", "numeric", "character", 
				#"character", "character", "character", "numeric", 
				#"character"))
		
		## check codes ----------------------------------------------------------------------------
	
		codes <- as.vector(as.numeric(data$HARMONIZED_CODE_PRODUCT_ENGLISH))
	
		CD$missing[CD$file == f] <- paste(hs6[!(hs6 %in% codes)], collapse=", ")
		
		## check units ----------------------------------------------------------------------------
		
		agg_help <- data.frame(	commodity = as.numeric(data$HARMONIZED_CODE_PRODUCT_ENGLISH), 
								f = data$TOTAL_FOB_VALUE_US / data$TOTAL_NET_WEIGHT_KG )
		colnames(agg_help) <- c('commodity', f)
		
		agg <- aggregate(agg_help, by = list(agg_help$commodity), FUN = mean, na.rm = TRUE)[,2:3]
		
		units <- left_join(units, agg, by = 'commodity')
		units[units==Inf]<-NA
		
		units_codes <- c()
		
		if (ncol(units) > 2){
		
			j <- ncol(units)
		
			units$tmp1 <- units[, j] / units[, j-1]
			if (j > 3) { units$tmp2 <- units[, j] / units[, j-2] }
			
			for (i in 1:nrow(units)){
			
				if ( (!is.na(units$tmp1[i])) & ( (units$tmp1[i] >=3) || (units$tmp1[i] <= 1/3) )) {
					units_codes <- c(units_codes, units$commodity[i])
				}
				
				if ('tmp2' %in% names(units)){
					if ( (!is.na(units$tmp1[i])) & ( (units$tmp1[i] >=3) || (units$tmp1[i] <= 1/3) )) {
						units_codes <- c(units_codes, units$commodity[i])
					}
				}

			}				
			
			CD$units[CD$file == f] <- paste(unique(units_codes), collapse=", ")
			
			units = subset(units, select = -c(tmp1) )
			if ('tmp2' %in% names(units)){ units = subset(units, select = -c(tmp2)) }
		
		}
		
	
		## check weights with COMTRADE ------------------------------------------------------------------
		
		weight <- data.frame(commodity = as.integer(substr(data$HARMONIZED_CODE_PRODUCT_ENGLISH, 1, 6)), weight = data$TOTAL_NET_WEIGHT_KG)
		#weight <- data.frame(commodity = as.integer(data$HARMONIZED_CODE_PRODUCT_ENGLISH), weight = data$TOTAL_NET_WEIGHT_KG)
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
			( (weight$comtrade_weight[i] >= (1.2 * weight$total_weight)) || 
			(weight$comtrade_weight[i] <= (0.8 * weight$total_weight)) )) {
			
				weight_codes <- c(weight_codes, weight$commodity[i]) 
			} 
		}
		
		CD$comtrade_weight[CD$file == f] <- paste(unique(weight_codes), collapse=", ")
		
		
		## check for commas in price and weight ------------------------------------------------------------------------------
		
		price_comma_codes <- c()
		for (i in 1:nrow(data)){
			if ( grepl(',', data$TOTAL_FOB_VALUE_US[i]) ) { price_comma_codes <- c(price_comma_codes, data$HARMONIZED_CODE_PRODUCT_ENGLISH[i]) }}
		CD$price_comma[CD$file == f] <- paste(unique(price_comma_codes), collapse=", ")
		
		weight_comma_codes <- c()
		for (i in 1:nrow(data)){
			if ( grepl(',', data$TOTAL_NET_WEIGHT_KG[i])) { weight_comma_codes <- c(weight_comma_codes, data$HARMONIZED_CODE_PRODUCT_ENGLISH[i]) }}		
		CD$weight_comma[CD$file == f] <- paste(unique(weight_comma_codes), collapse=", ")
	
		
	}
	
	
	################################  COSTARICA  ##########################################################################################

	if (CD$country[CD$file == f] == 'COSTARICA'){
	
		data <- read.csv(text = rawToChar(obj), sep = ';', quote = '')
				#colClasses = c("character", "character", "character", 
				#"character", "character", "numeric", "character", 
				#"character", "character", "character", "numeric", 
				#"character"))
		
		## check codes ----------------------------------------------------------------------------
	
		codes <- as.vector(as.numeric(data$HARMONIZED_CODE_PRODUCT_ENGLISH))
	
		CD$missing[CD$file == f] <- paste(hs6[!(hs6 %in% codes)], collapse=", ")
		
		## check units ----------------------------------------------------------------------------
		
		agg_help <- data.frame(	commodity = as.numeric(data$HARMONIZED_CODE_PRODUCT_ENGLISH), 
								f = data$TOTAL_CIF_VALUE_US / data$TOTAL_NET_WEIGHT_KG )
		colnames(agg_help) <- c('commodity', f)
		
		agg <- aggregate(agg_help, by = list(agg_help$commodity), FUN = mean, na.rm = TRUE)[,2:3]
		
		units <- left_join(units, agg, by = 'commodity')
		units[units==Inf]<-NA
		
		units_codes <- c()
		
		if (ncol(units) > 2){
		
			j <- ncol(units)
		
			units$tmp1 <- units[, j] / units[, j-1]
			if (j > 3) { units$tmp2 <- units[, j] / units[, j-2] }
			
			for (i in 1:nrow(units)){
			
				if ( (!is.na(units$tmp1[i])) & ( (units$tmp1[i] >=3) || (units$tmp1[i] <= 1/3) )) {
					units_codes <- c(units_codes, units$commodity[i])
				}
				
				if ('tmp2' %in% names(units)){
					if ( (!is.na(units$tmp1[i])) & ( (units$tmp1[i] >=3) || (units$tmp1[i] <= 1/3) )) {
						units_codes <- c(units_codes, units$commodity[i])
					}
				}

			}				
			
			CD$units[CD$file == f] <- paste(unique(units_codes), collapse=", ")
			
			units = subset(units, select = -c(tmp1) )
			if ('tmp2' %in% names(units)){ units = subset(units, select = -c(tmp2)) }
		
		}
		
		
		## check weights with COMTRADE ------------------------------------------------------------------
		
		weight <- data.frame(commodity = as.integer(substr(data$HARMONIZED_CODE_PRODUCT_ENGLISH, 1, 6)), weight = data$TOTAL_NET_WEIGHT_KG)
		#weight <- data.frame(commodity = as.integer(data$HARMONIZED_CODE_PRODUCT_ENGLISH), weight = data$TOTAL_NET_WEIGHT_KG)
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
			( (weight$comtrade_weight[i] >= (1.2 * weight$total_weight)) || 
			(weight$comtrade_weight[i] <= (0.8 * weight$total_weight)) )) {
			
				weight_codes <- c(weight_codes, weight$commodity[i]) 
			} 
		}
		
		CD$comtrade_weight[CD$file == f] <- paste(unique(weight_codes), collapse=", ")
		
		
		## check for commas in price and weight ------------------------------------------------------------------------------
		
		price_comma_codes <- c()
		for (i in 1:nrow(data)){
			if ( grepl(',', data$TOTAL_CIF_VALUE_US[i]) ) { price_comma_codes <- c(price_comma_codes, data$HARMONIZED_CODE_PRODUCT_ENGLISH[i]) }}
		CD$price_comma[CD$file == f] <- paste(unique(price_comma_codes), collapse=", ")
		
		weight_comma_codes <- c()
		for (i in 1:nrow(data)){
			if ( grepl(',', data$TOTAL_NET_WEIGHT_KG[i])) { weight_comma_codes <- c(weight_comma_codes, data$HARMONIZED_CODE_PRODUCT_ENGLISH[i]) }}		
		CD$weight_comma[CD$file == f] <- paste(unique(weight_comma_codes), collapse=", ")
		
		
	}
	
	
	
	################################  ECUADOR  #########################################################################################
	
	if (CD$country[CD$file == f] == 'ECUADOR'){
	
		data <- read.csv(text = rawToChar(obj), sep = ';', quote = '',
				colClasses = c("character", "character", "character", 
				"character", "character", "character", "character", 
				"character", "character", "character", "character", 
				"character", "character", "character", "numeric", 
				"character", "numeric", "numeric", "numeric", "numeric", 
				"numeric", "character", "character", "character", 
				"character", "character", "character", "character", 
				"character", "character"))
				
		## check codes ----------------------------------------------------------------------------
		
		codes <- as.vector(as.numeric(data$HARMONIZED_CODEPRODUCT_SPANISH))
	
		CD$missing[CD$file == f] <- paste(hs6[!(hs6 %in% codes)], collapse=", ")
		
		## check units ----------------------------------------------------------------------------
		
		agg_help <- data.frame(	commodity = as.numeric(data$HARMONIZED_CODEPRODUCT_SPANISH), 
								f = data$TOTAL_FOB_VALUE_US / data$TOTAL_NET_WEIGHT_KG)
		colnames(agg_help) <- c('commodity', f)
		
		agg <- aggregate(agg_help, by = list(agg_help$commodity), FUN = mean, na.rm = TRUE)[,2:3]
		
		units <- left_join(units, agg, by = 'commodity')
		units[units==Inf]<-NA
		
		units_codes <- c()
		
		if (ncol(units) > 2){
		
			j <- ncol(units)
		
			units$tmp1 <- units[, j] / units[, j-1]
			if (j > 3) { units$tmp2 <- units[, j] / units[, j-2] }
			
			for (i in 1:nrow(units)){
			
				if ( (!is.na(units$tmp1[i])) & ( (units$tmp1[i] >=3) || (units$tmp1[i] <= 1/3) )) {
					units_codes <- c(units_codes, units$commodity[i])
				}
				
				if ('tmp2' %in% names(units)){
					if ( (!is.na(units$tmp1[i])) & ( (units$tmp1[i] >=3) || (units$tmp1[i] <= 1/3) )) {
						units_codes <- c(units_codes, units$commodity[i])
					}
				}

			}				
			
			CD$units[CD$file == f] <- paste(unique(units_codes), collapse=", ")
			
			units = subset(units, select = -c(tmp1) )
			if ('tmp2' %in% names(units)){ units = subset(units, select = -c(tmp2)) }
		
		}
		
	
		## check weights with COMTRADE ------------------------------------------------------------------
		
		weight <- data.frame(commodity = as.integer(substr(data$HARMONIZED_CODEPRODUCT_SPANISH, 1, 6)), weight = data$TOTAL_NET_WEIGHT_KG)
		#weight <- data.frame(commodity = as.integer(data$HARMONIZED_CODEPRODUCT_SPANISH), weight = data$TOTAL_NET_WEIGHT_KG)
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
			( (weight$comtrade_weight[i] >= (1.2 * weight$total_weight)) || 
			(weight$comtrade_weight[i] <= (0.8 * weight$total_weight)) )) {
			
				weight_codes <- c(weight_codes, weight$commodity[i]) 
			} 
		}
		
		CD$comtrade_weight[CD$file == f] <- paste(unique(weight_codes), collapse=", ")
		
		
		## check for commas in price and weight ------------------------------------------------------------------------------
		
		price_comma_codes <- c()
		for (i in 1:nrow(data)){
			if ( grepl(',', data$TOTAL_FOB_VALUE_US[i]) ) { price_comma_codes <- c(price_comma_codes, data$HARMONIZED_CODEPRODUCT_SPANISH[i]) }}
		CD$price_comma[CD$file == f] <- paste(unique(price_comma_codes), collapse=", ")
		
		weight_comma_codes <- c()
		for (i in 1:nrow(data)){
			if ( grepl(',', data$TOTAL_NET_WEIGHT_KG[i])) { weight_comma_codes <- c(weight_comma_codes, data$HARMONIZED_CODEPRODUCT_SPANISH[i]) }}		
		CD$weight_comma[CD$file == f] <- paste(unique(weight_comma_codes), collapse=", ")
		
		
	}
	
		
	################################  MEXICO  ##########################################################################################

	if (CD$country[CD$file == f] == 'MEXICO'){
	
		data <- read.csv(text = rawToChar(obj), sep = ';', quote = '')
				#colClasses = c("character", "character", "character", 
				#"character", "character", "numeric", "character", 
				#"character", "character", "character", "numeric", 
				#"character"))
		
		## check codes ----------------------------------------------------------------------------
	
		codes <- as.vector(as.numeric(data$HARMONIZED_CODE_PRODUCT_ENGLISH))
	
		CD$missing[CD$file == f] <- paste(hs6[!(hs6 %in% codes)], collapse=", ")
		
		## check units ----------------------------------------------------------------------------
		
		## check if kilos are for all interesting codes
		
		
		agg_help <- data.frame(	commodity = as.numeric(data$HARMONIZED_CODE_PRODUCT_ENGLISH), 
								f = data$TOTAL_FOB_VALUE_US / data$TOTAL_QUANTITY_1 )
		colnames(agg_help) <- c('commodity', f)
		
		agg <- aggregate(agg_help, by = list(agg_help$commodity), FUN = mean, na.rm = TRUE)[,2:3]
		
		units <- left_join(units, agg, by = 'commodity')
		units[units==Inf]<-NA
		
		units_codes <- c()
		
		if (ncol(units) > 2){
		
			j <- ncol(units)
		
			units$tmp1 <- units[, j] / units[, j-1]
			if (j > 3) { units$tmp2 <- units[, j] / units[, j-2] }
			
			for (i in 1:nrow(units)){
			
				if ( (!is.na(units$tmp1[i])) & ( (units$tmp1[i] >=3) || (units$tmp1[i] <= 1/3) )) {
					units_codes <- c(units_codes, units$commodity[i])
				}
				
				if ('tmp2' %in% names(units)){
					if ( (!is.na(units$tmp1[i])) & ( (units$tmp1[i] >=3) || (units$tmp1[i] <= 1/3) )) {
						units_codes <- c(units_codes, units$commodity[i])
					}
				}

			}				
			
			CD$units[CD$file == f] <- paste(unique(units_codes), collapse=", ")
			
			units = subset(units, select = -c(tmp1) )
			if ('tmp2' %in% names(units)){ units = subset(units, select = -c(tmp2)) }
		
		}
		
	
		## check weights with COMTRADE ------------------------------------------------------------------
		
		weight <- data.frame(commodity = as.integer(substr(data$HARMONIZED_CODE_PRODUCT_ENGLISH, 1, 6)), weight = data$TOTAL_QUANTITY_1)
		#weight <- data.frame(commodity = as.integer(data$HARMONIZED_CODE_PRODUCT_ENGLISH), weight = data$TOTAL_QUANTITY_1)
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
			( (weight$comtrade_weight[i] >= (1.2 * weight$total_weight)) || 
			(weight$comtrade_weight[i] <= (0.8 * weight$total_weight)) )) {
			
				weight_codes <- c(weight_codes, weight$commodity[i]) 
			} 
		}
		
		CD$comtrade_weight[CD$file == f] <- paste(unique(weight_codes), collapse=", ")
		
		
		## check for commas in price and weight ------------------------------------------------------------------------------
		
		price_comma_codes <- c()
		for (i in 1:nrow(data)){
			if ( grepl(',', data$TOTAL_FOB_VALUE_US[i]) ) { price_comma_codes <- c(price_comma_codes, data$HARMONIZED_CODE_PRODUCT_ENGLISH[i]) }}
		CD$price_comma[CD$file == f] <- paste(unique(price_comma_codes), collapse=", ")
		
		weight_comma_codes <- c()
		for (i in 1:nrow(data)){
			if ( grepl(',', data$TOTAL_QUANTITY_1[i])) { weight_comma_codes <- c(weight_comma_codes, data$HARMONIZED_CODE_PRODUCT_ENGLISH[i]) }}		
		CD$weight_comma[CD$file == f] <- paste(unique(weight_comma_codes), collapse=", ")
		
		
	}
	

	################################  PANAMA  ##########################################################################################

	if (CD$country[CD$file == f] == 'PANAMA'){
	
		data <- read.csv(text = rawToChar(obj), sep = ';', quote = '')
				#colClasses = c("character", "character", "character", 
				#"character", "character", "numeric", "character", 
				#"character", "character", "character", "numeric", 
				#"character"))
		
		## check codes ----------------------------------------------------------------------------
	
		codes <- as.vector(as.numeric(data$HARMONIZED_CODE_PRODUCT_ENGLISH))
	
		CD$missing[CD$file == f] <- paste(hs6[!(hs6 %in% codes)], collapse=", ")
		
		## check units ----------------------------------------------------------------------------
		
		agg_help <- data.frame(	commodity = as.numeric(data$HARMONIZED_CODE_PRODUCT_ENGLISH), 
								f = data$TOTAL_FOB_VALUE_US / data$TOTAL_NET_WEIGHT_KG )
		colnames(agg_help) <- c('commodity', f)
		
		agg <- aggregate(agg_help, by = list(agg_help$commodity), FUN = mean, na.rm = TRUE)[,2:3]
		
		units <- left_join(units, agg, by = 'commodity')
		units[units==Inf]<-NA
		
		units_codes <- c()
		
		if (ncol(units) > 2){
		
			j <- ncol(units)
		
			units$tmp1 <- units[, j] / units[, j-1]
			if (j > 3) { units$tmp2 <- units[, j] / units[, j-2] }
			
			for (i in 1:nrow(units)){
			
				if ( (!is.na(units$tmp1[i])) & ( (units$tmp1[i] >=3) || (units$tmp1[i] <= 1/3) )) {
					units_codes <- c(units_codes, units$commodity[i])
				}
				
				if ('tmp2' %in% names(units)){
					if ( (!is.na(units$tmp1[i])) & ( (units$tmp1[i] >=3) || (units$tmp1[i] <= 1/3) )) {
						units_codes <- c(units_codes, units$commodity[i])
					}
				}

			}				
			
			CD$units[CD$file == f] <- paste(unique(units_codes), collapse=", ")
			
			units = subset(units, select = -c(tmp1) )
			if ('tmp2' %in% names(units)){ units = subset(units, select = -c(tmp2)) }
		
		}
		
		
		## check weights with COMTRADE ------------------------------------------------------------------
		
		weight <- data.frame(commodity = as.integer(substr(data$HARMONIZED_CODE_PRODUCT_ENGLISH, 1, 6)), weight = data$TOTAL_NET_WEIGHT_KG)
		#weight <- data.frame(commodity = as.integer(data$HARMONIZED_CODE_PRODUCT_ENGLISH), weight = data$TOTAL_NET_WEIGHT_KG)
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
			( (weight$comtrade_weight[i] >= (1.2 * weight$total_weight)) || 
			(weight$comtrade_weight[i] <= (0.8 * weight$total_weight)) )) {
			
				weight_codes <- c(weight_codes, weight$commodity[i]) 
			} 
		}
		
		CD$comtrade_weight[CD$file == f] <- paste(unique(weight_codes), collapse=", ")
		
		
		## check for commas in price and weight ------------------------------------------------------------------------------
		
		price_comma_codes <- c()
		for (i in 1:nrow(data)){
			if ( grepl(',', data$TOTAL_FOB_VALUE_US[i]) ) { price_comma_codes <- c(price_comma_codes, data$HARMONIZED_CODE_PRODUCT_ENGLISH[i]) }}
		CD$price_comma[CD$file == f] <- paste(unique(price_comma_codes), collapse=", ")
		
		weight_comma_codes <- c()
		for (i in 1:nrow(data)){
			if ( grepl(',', data$TOTAL_NET_WEIGHT_KG[i])) { weight_comma_codes <- c(weight_comma_codes, data$HARMONIZED_CODE_PRODUCT_ENGLISH[i]) }}		
		CD$weight_comma[CD$file == f] <- paste(unique(weight_comma_codes), collapse=", ")
		
		
	}
	

	################################  PARAGUAY  ##########################################################################################

	if (CD$country[CD$file == f] == 'PARAGUAY'){
	
		data <- read.csv(text = rawToChar(obj), sep = ';', quote = '')
				#colClasses = c("character", "character", "character", 
				#"character", "character", "numeric", "character", 
				#"character", "character", "character", "numeric", 
				#"character"))
		
		## check codes ----------------------------------------------------------------------------
	
		
		if (grepl("data/1-TRADE/CD/EXPORT/PARAGUAY/SICEX/", CD$file)){
			codes <- as.vector(as.numeric(data$'Harmonized CodeProduct English'))
		}
		if (grepl("data/1-TRADE/CD/EXPORT/PARAGUAY/MINTRADE/", CD$file)){
			data$hs8 <- as.integer(substr(gsub('\\.', '', data$NCM, perl=TRUE), 0, 8))
			codes <- as.vector(as.numeric(data$hs8))
		}

	
		CD$missing[CD$file == f] <- paste(hs6[!(hs6 %in% codes)], collapse=", ")
		
		## check units ----------------------------------------------------------------------------
		
		if (grepl("data/1-TRADE/CD/EXPORT/PARAGUAY/SICEX/", CD$file)){
			agg_help <- data.frame(	commodity = as.numeric(data$'Harmonized CodeProduct English'), 
									f = data$'TOTAL FOB Value US' / data$'TOTAL Net Weight Kg' )
		}
		if (grepl("data/1-TRADE/CD/EXPORT/PARAGUAY/MINTRADE/", CD$file)){
			agg_help <- data.frame(	commodity = as.numeric(data$hs8), 
									f = data$'Valor Fob Dolar' / data$'Kilo Neto' )
		}
		
		colnames(agg_help) <- c('commodity', f)
		
		agg <- aggregate(agg_help, by = list(agg_help$commodity), FUN = mean, na.rm = TRUE)[,2:3]
		
		units <- left_join(units, agg, by = 'commodity')
		units[units==Inf]<-NA
		
		units_codes <- c()
		
		if (ncol(units) > 2){
		
			j <- ncol(units)
		
			units$tmp1 <- units[, j] / units[, j-1]
			if (j > 3) { units$tmp2 <- units[, j] / units[, j-2] }
			
			for (i in 1:nrow(units)){
			
				if ( (!is.na(units$tmp1[i])) & ( (units$tmp1[i] >=3) || (units$tmp1[i] <= 1/3) )) {
					units_codes <- c(units_codes, units$commodity[i])
				}
				
				if ('tmp2' %in% names(units)){
					if ( (!is.na(units$tmp1[i])) & ( (units$tmp1[i] >=3) || (units$tmp1[i] <= 1/3) )) {
						units_codes <- c(units_codes, units$commodity[i])
					}
				}

			}				
			
			CD$units[CD$file == f] <- paste(unique(units_codes), collapse=", ")
			
			units = subset(units, select = -c(tmp1) )
			if ('tmp2' %in% names(units)){ units = subset(units, select = -c(tmp2)) }
		
		}
		
		
		## check weights with COMTRADE ------------------------------------------------------------------
	
		if (grepl("data/1-TRADE/CD/EXPORT/PARAGUAY/SICEX/", CD$file)){
			weight <- data.frame(commodity = as.integer(substr(as.character(data$'Harmonized CodeProduct English'), 1, 6)), weight = data$'TOTAL Net Weight Kg')
		}
		if (grepl("data/1-TRADE/CD/EXPORT/PARAGUAY/MINTRADE/", CD$file)){
			weight <- data.frame(commodity = as.integer(substr(as.character(data$hs8), 1, 6)), weight = data$'Kilo Neto')
		}
	
		#weight <- data.frame(commodity = as.integer(data$HARMONIZED_CODE_PRODUCT_ENGLISH), weight = data$TOTAL_NET_WEIGHT_KG)
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
			( (weight$comtrade_weight[i] >= (1.2 * weight$total_weight)) || 
			(weight$comtrade_weight[i] <= (0.8 * weight$total_weight)) )) {
			
				weight_codes <- c(weight_codes, weight$commodity[i]) 
			} 
		}
		
		CD$comtrade_weight[CD$file == f] <- paste(unique(weight_codes), collapse=", ")
		
		
		## check for commas in price and weight ------------------------------------------------------------------------------
		
		price_comma_codes <- c()
		if (grepl("data/1-TRADE/CD/EXPORT/PARAGUAY/SICEX/", CD$file)){
			for (i in 1:nrow(data)){
				if ( grepl(',', data$'TOTAL FOB Value US'[i]) ) { price_comma_codes <- c(price_comma_codes, data$'Harmonized CodeProduct English'[i]) }
			}
		}
		if (grepl("data/1-TRADE/CD/EXPORT/PARAGUAY/MINTRADE/", CD$file)){
			for (i in 1:nrow(data)){
				if ( grepl(',', data$'Valor Fob Dolar'[i]) ) { price_comma_codes <- c(price_comma_codes, data$hs8[i]) }
			}
		}
		CD$price_comma[CD$file == f] <- paste(unique(price_comma_codes), collapse=", ")
		
		
		weight_comma_codes <- c()
		if (grepl("data/1-TRADE/CD/EXPORT/PARAGUAY/SICEX/", CD$file)){
			for (i in 1:nrow(data)){
				if ( grepl(',', data$'TOTAL Net Weight Kg'[i])) { weight_comma_codes <- c(weight_comma_codes, data$'Harmonized CodeProduct English'[i]) }
			}
		}
		if (grepl("data/1-TRADE/CD/EXPORT/PARAGUAY/MINTRADE/", CD$file)){
			for (i in 1:nrow(data)){
				if ( grepl(',', data$'Kilo Neto'[i])) { weight_comma_codes <- c(weight_comma_codes, data$hs8[i]) }
			}
		}	
		CD$weight_comma[CD$file == f] <- paste(unique(weight_comma_codes), collapse=", ")
		
		
	}
	
	
	################################  PERU  ##########################################################################################

	if (CD$country[CD$file == f] == 'PERU'){
	
		data <- read.csv(text = rawToChar(obj), sep = ';', quote = '')
				#colClasses = c("character", "character", "character", 
				#"character", "character", "numeric", "character", 
				#"character", "character", "character", "numeric", 
				#"character"))
		
		## check codes ----------------------------------------------------------------------------
	
		codes <- as.vector(as.numeric(data$'COD..ARMONIZADOPRODUCTO.INGLES'))
	
		CD$missing[CD$file == f] <- paste(hs6[!(hs6 %in% codes)], collapse=", ")
		
		## check units ----------------------------------------------------------------------------
		
		agg_help <- data.frame(	commodity = as.numeric(data$'COD..ARMONIZADOPRODUCTO.INGLES'), 
								f = data$'TOTAL.VALOR.FOB.US' / data$'TOTAL.PESO.NETO.KG' )
		colnames(agg_help) <- c('commodity', f)
		
		agg <- aggregate(agg_help, by = list(agg_help$commodity), FUN = mean, na.rm = TRUE)[,2:3]
		
		units <- left_join(units, agg, by = 'commodity')
		units[units==Inf]<-NA
		
		units_codes <- c()
		
		if (ncol(units) > 2){
		
			j <- ncol(units)
		
			units$tmp1 <- units[, j] / units[, j-1]
			if (j > 3) { units$tmp2 <- units[, j] / units[, j-2] }
			
			for (i in 1:nrow(units)){
			
				if ( (!is.na(units$tmp1[i])) & ( (units$tmp1[i] >=3) || (units$tmp1[i] <= 1/3) )) {
					units_codes <- c(units_codes, units$commodity[i])
				}
				
				if ('tmp2' %in% names(units)){
					if ( (!is.na(units$tmp1[i])) & ( (units$tmp1[i] >=3) || (units$tmp1[i] <= 1/3) )) {
						units_codes <- c(units_codes, units$commodity[i])
					}
				}

			}				
			
			CD$units[CD$file == f] <- paste(unique(units_codes), collapse=", ")
			
			units = subset(units, select = -c(tmp1) )
			if ('tmp2' %in% names(units)){ units = subset(units, select = -c(tmp2)) }
		
		}
		
		
		## check weights with COMTRADE ------------------------------------------------------------------
		
		weight <- data.frame(commodity = as.integer(substr(data$'COD..ARMONIZADOPRODUCTO.INGLES', 1, 6)), weight = data$'TOTAL.PESO.NETO.KG')
		#weight <- data.frame(commodity = as.integer(data$'COD..ARMONIZADOPRODUCTO.INGLES'), weight = data$'TOTAL.PESO.NETO.KG')
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
			( (weight$comtrade_weight[i] >= (1.2 * weight$total_weight)) || 
			(weight$comtrade_weight[i] <= (0.8 * weight$total_weight)) )) {
			
				weight_codes <- c(weight_codes, weight$commodity[i]) 
			} 
		}
		
		CD$comtrade_weight[CD$file == f] <- paste(unique(weight_codes), collapse=", ")
		
		
		## check for commas in price and weight ------------------------------------------------------------------------------
		
		price_comma_codes <- c()
		for (i in 1:nrow(data)){
			if ( grepl(',', data$'TOTAL.VALOR.FOB.US'[i]) ) { price_comma_codes <- c(price_comma_codes, data$'COD..ARMONIZADOPRODUCTO.INGLES'[i]) }}
		CD$price_comma[CD$file == f] <- paste(unique(price_comma_codes), collapse=", ")
		
		weight_comma_codes <- c()
		for (i in 1:nrow(data)){
			if ( grepl(',', data$'TOTAL.PESO.NETO.KG'[i])) { weight_comma_codes <- c(weight_comma_codes, data$'COD..ARMONIZADOPRODUCTO.INGLES'[i]) }}		
		CD$weight_comma[CD$file == f] <- paste(unique(weight_comma_codes), collapse=", ")
		
		
	}
	
	
	################################  URUGUAY  ##########################################################################################

	if (CD$country[CD$file == f] == 'URUGUAY'){
	
		data <- read.csv(text = rawToChar(obj), sep = ';', quote = '')
				#colClasses = c("character", "character", "character", 
				#"character", "character", "numeric", "character", 
				#"character", "character", "character", "numeric", 
				#"character"))
		
		## check codes ----------------------------------------------------------------------------
	
		codes <- as.vector(as.numeric(data$HARMONIZED_CODEPRODUCT_ENGLISH))
	
		CD$missing[CD$file == f] <- paste(hs6[!(hs6 %in% codes)], collapse=", ")
		
		## check units ----------------------------------------------------------------------------
		
		agg_help <- data.frame(	commodity = as.numeric(data$HARMONIZED_CODEPRODUCT_ENGLISH), 
								f = data$TOTAL_FOB_VALUE_US  / data$TOTAL_NET_WEIGHT_KG )
		colnames(agg_help) <- c('commodity', f)
		
		agg <- aggregate(agg_help, by = list(agg_help$commodity), FUN = mean, na.rm = TRUE)[,2:3]
		
		units <- left_join(units, agg, by = 'commodity')
		units[units==Inf]<-NA
		
		units_codes <- c()
		
		if (ncol(units) > 2){
		
			j <- ncol(units)
		
			units$tmp1 <- units[, j] / units[, j-1]
			if (j > 3) { units$tmp2 <- units[, j] / units[, j-2] }
			
			for (i in 1:nrow(units)){
			
				if ( (!is.na(units$tmp1[i])) & ( (units$tmp1[i] >=3) || (units$tmp1[i] <= 1/3) )) {
					units_codes <- c(units_codes, units$commodity[i])
				}
				
				if ('tmp2' %in% names(units)){
					if ( (!is.na(units$tmp1[i])) & ( (units$tmp1[i] >=3) || (units$tmp1[i] <= 1/3) )) {
						units_codes <- c(units_codes, units$commodity[i])
					}
				}

			}				
			
			CD$units[CD$file == f] <- paste(unique(units_codes), collapse=", ")
			
			units = subset(units, select = -c(tmp1) )
			if ('tmp2' %in% names(units)){ units = subset(units, select = -c(tmp2)) }
		
		}
		
		
		## check weights with COMTRADE ------------------------------------------------------------------
		
		weight <- data.frame(commodity = as.integer(substr(data$HARMONIZED_CODEPRODUCT_ENGLISH, 1, 6)), weight = data$TOTAL_NET_WEIGHT_KG)
		#weight <- data.frame(commodity = as.integer(data$HARMONIZED_CODEPRODUCT_ENGLISH), weight = data$TOTAL_NET_WEIGHT_KG)
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
			( (weight$comtrade_weight[i] >= (1.2 * weight$total_weight)) || 
			(weight$comtrade_weight[i] <= (0.8 * weight$total_weight)) )) {
			
				weight_codes <- c(weight_codes, weight$commodity[i]) 
			} 
		}
		
		CD$comtrade_weight[CD$file == f] <- paste(unique(weight_codes), collapse=", ")
		
		
		## check for commas in price and weight ------------------------------------------------------------------------------
		
		price_comma_codes <- c()
		for (i in 1:nrow(data)){
			if ( grepl(',', data$TOTAL_FOB_VALUE_US[i]) ) { price_comma_codes <- c(price_comma_codes, data$HARMONIZED_CODEPRODUCT_ENGLISH[i]) }}
		CD$price_comma[CD$file == f] <- paste(unique(price_comma_codes), collapse=", ")
		
		weight_comma_codes <- c()
		for (i in 1:nrow(data)){
			if ( grepl(',', data$TOTAL_NET_WEIGHT_KG[i])) { weight_comma_codes <- c(weight_comma_codes, data$HARMONIZED_CODEPRODUCT_ENGLISH[i]) }}		
		CD$weight_comma[CD$file == f] <- paste(unique(weight_comma_codes), collapse=", ")
		
		
	}
	
	
	################################  VENEZUELA  ##########################################################################################

	if (CD$country[CD$file == f] == 'VENEZUELA'){
	
		data <- read.csv(text = rawToChar(obj), sep = ';', quote = '')
				#colClasses = c("character", "character", "character", 
				#"character", "character", "numeric", "character", 
				#"character", "character", "character", "numeric", 
				#"character"))
		
		## check codes ----------------------------------------------------------------------------
	
		codes <- as.vector(as.numeric(data$HARMONIZED_CODE_PRODUCT_SPANISH))
	
		CD$missing[CD$file == f] <- paste(hs6[!(hs6 %in% codes)], collapse=", ")
		
		## check units ----------------------------------------------------------------------------
		
		agg_help <- data.frame(	commodity = as.numeric(data$HARMONIZED_CODE_PRODUCT_SPANISH), 
								f = data$TOTAL_FOB_VALUE_US  / data$TOTAL_NET_WEIGHT_KG )
		colnames(agg_help) <- c('commodity', f)
		
		agg <- aggregate(agg_help, by = list(agg_help$commodity), FUN = mean, na.rm = TRUE)[,2:3]
		
		units <- left_join(units, agg, by = 'commodity')
		units[units==Inf]<-NA
		
		units_codes <- c()
		
		if (ncol(units) > 2){
		
			j <- ncol(units)
		
			units$tmp1 <- units[, j] / units[, j-1]
			if (j > 3) { units$tmp2 <- units[, j] / units[, j-2] }
			
			for (i in 1:nrow(units)){
			
				if ( (!is.na(units$tmp1[i])) & ( (units$tmp1[i] >=3) || (units$tmp1[i] <= 1/3) )) {
					units_codes <- c(units_codes, units$commodity[i])
				}
				
				if ('tmp2' %in% names(units)){
					if ( (!is.na(units$tmp1[i])) & ( (units$tmp1[i] >=3) || (units$tmp1[i] <= 1/3) )) {
						units_codes <- c(units_codes, units$commodity[i])
					}
				}

			}				
			
			CD$units[CD$file == f] <- paste(unique(units_codes), collapse=", ")
			
			units = subset(units, select = -c(tmp1) )
			if ('tmp2' %in% names(units)){ units = subset(units, select = -c(tmp2)) }
		
		}
		
		
		## check weights with COMTRADE ------------------------------------------------------------------
		
		weight <- data.frame(commodity = as.integer(substr(data$HARMONIZED_CODE_PRODUCT_SPANISH, 1, 6)), weight = data$TOTAL_NET_WEIGHT_KG)
		#weight <- data.frame(commodity = as.integer(data$HARMONIZED_CODE_PRODUCT_SPANISH), weight = data$TOTAL_NET_WEIGHT_KG)
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
			( (weight$comtrade_weight[i] >= (1.2 * weight$total_weight)) || 
			(weight$comtrade_weight[i] <= (0.8 * weight$total_weight)) )) {
			
				weight_codes <- c(weight_codes, weight$commodity[i]) 
			} 
		}
		
		CD$comtrade_weight[CD$file == f] <- paste(unique(weight_codes), collapse=", ")
		
		
		## check for commas in price and weight ------------------------------------------------------------------------------
		
		price_comma_codes <- c()
		for (i in 1:nrow(data)){
			if ( grepl(',', data$TOTAL_FOB_VALUE_US[i]) ) { price_comma_codes <- c(price_comma_codes, data$HARMONIZED_CODE_PRODUCT_SPANISH[i]) }}
		CD$price_comma[CD$file == f] <- paste(unique(price_comma_codes), collapse=", ")
		
		weight_comma_codes <- c()
		for (i in 1:nrow(data)){
			if ( grepl(',', data$TOTAL_NET_WEIGHT_KG[i])) { weight_comma_codes <- c(weight_comma_codes, data$HARMONIZED_CODE_PRODUCT_SPANISH[i]) }}		
		CD$weight_comma[CD$file == f] <- paste(unique(weight_comma_codes), collapse=", ")
		
		
	}
	
}

#only keep release codes


# write file
write.csv2(BOL, 'BOL_check.csv', quote = FALSE, row.names = FALSE)
write.csv2(CD, 'CD_check.csv', quote = FALSE, row.names = FALSE)


