## final comtrade check

library(aws.s3)
library(stringr)
library(gsubfn)
library(dplyr)

#options(scipen=999999999999999999999999999999999999999999)


argentina_content <- get_bucket_df(bucket = 'trase-storage', prefix = 'data/1-TRADE/CD/EXPORT/ARGENTINA/')
argentina_content <- subset(argentina_content, grepl(".*/CD_[A-Z]+_[1-9][0-9]{3}.csv$", Key) )

bolivia_content <- get_bucket_df(bucket = 'trase-storage', prefix = 'data/1-TRADE/CD/EXPORT/BOLIVIA/')
bolivia_content <- subset(bolivia_content, grepl(".*/CD_[A-Z]+_[1-9][0-9]{3}.csv$", Key) )

brazil_content <- get_bucket_df(bucket = 'trase-storage', prefix = 'data/1-TRADE/CD/EXPORT/BRAZIL/')
brazil_content <- subset(brazil_content, grepl(".*/CD_[A-Z]+_[1-9][0-9]{3}.csv$", Key) )

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

uruguay_content <- get_bucket_df(bucket = 'trase-storage', prefix = 'data/1-TRADE/CD/EXPORT/URUGUAY/')
uruguay_content <- subset(uruguay_content, grepl(".*/CD_[A-Z]+_[1-9][0-9]{3}.csv$", Key) )

venezuela_content <- get_bucket_df(bucket = 'trase-storage', prefix = 'data/1-TRADE/CD/EXPORT/VENEZUELA/')
venezuela_content <- subset(venezuela_content, grepl(".*/CD_[A-Z]+_[1-9][0-9]{3}.csv$", Key) )


CD_content <- do.call(rbind, list(	argentina_content,
									bolivia_content,
									brazil_content,
									colombia_content,
									costarica_content,
									ecuador_content,
									mexico_content,
									panama_content,
									paraguay_content,
									peru_content,
									uruguay_content,
									venezuela_content	))

					
CD <- data.frame(	file = CD_content$Key,
					type = strapplyc(CD_content$Key, ".*/(CD)_[A-Z]+_[1-9][0-9]{3}.csv$", simplify = TRUE),
					country = strapplyc(CD_content$Key, ".*/CD_([A-Z]+)_[1-9][0-9]{3}.csv$", simplify = TRUE),
					year = strapplyc(CD_content$Key, ".*/CD_[A-Z]+_([1-9][0-9]{3}).csv$", simplify = TRUE)  )
					
CD <- CD[CD$year != 2018,]

CD <- CD[order(CD$country, CD$year),]	


## load codes ----------------------------------------------------------------------------------------------------------------------------

obj <- get_object(object = 'data/1-TRADE/commodity_equivalents_final.csv', bucket = 'trase-storage')

hs <- read.csv(text = rawToChar(obj), sep = ';', quote = '',
				colClasses = c("character", "character", "character", 
				"character", "character", "character", "numeric", 
				"numeric"))

hs6 <- as.vector(as.numeric(hs$code_value[hs$code_type == 'HS_6']))



obj <- get_object(object = 'data/1-TRADE/STATISTICAL_DATA/GLOBAL/COMTRADE/COMTRADE_ZOOM/COMTRADE_2005_zoom.csv', bucket = 'trase-storage')
comtrade05 <- read.csv(text = rawToChar(obj), quote = '', sep = ';')
obj <- get_object(object = 'data/1-TRADE/STATISTICAL_DATA/GLOBAL/COMTRADE/COMTRADE_ZOOM/COMTRADE_2006_zoom.csv', bucket = 'trase-storage')
comtrade06 <- read.csv(text = rawToChar(obj), quote = '', sep = ';')
obj <- get_object(object = 'data/1-TRADE/STATISTICAL_DATA/GLOBAL/COMTRADE/COMTRADE_ZOOM/COMTRADE_2007_zoom.csv', bucket = 'trase-storage')
comtrade07 <- read.csv(text = rawToChar(obj), quote = '', sep = ';')
obj <- get_object(object = 'data/1-TRADE/STATISTICAL_DATA/GLOBAL/COMTRADE/COMTRADE_ZOOM/COMTRADE_2008_zoom.csv', bucket = 'trase-storage')
comtrade08 <- read.csv(text = rawToChar(obj), quote = '', sep = ';')
obj <- get_object(object = 'data/1-TRADE/STATISTICAL_DATA/GLOBAL/COMTRADE/COMTRADE_ZOOM/COMTRADE_2009_zoom.csv', bucket = 'trase-storage')
comtrade09 <- read.csv(text = rawToChar(obj), quote = '', sep = ';')
obj <- get_object(object = 'data/1-TRADE/STATISTICAL_DATA/GLOBAL/COMTRADE/COMTRADE_ZOOM/COMTRADE_2010_zoom.csv', bucket = 'trase-storage')
comtrade10 <- read.csv(text = rawToChar(obj), quote = '', sep = ';')
obj <- get_object(object = 'data/1-TRADE/STATISTICAL_DATA/GLOBAL/COMTRADE/COMTRADE_ZOOM/COMTRADE_2011_zoom.csv', bucket = 'trase-storage')
comtrade11 <- read.csv(text = rawToChar(obj), quote = '', sep = ';')
obj <- get_object(object = 'data/1-TRADE/STATISTICAL_DATA/GLOBAL/COMTRADE/COMTRADE_ZOOM/COMTRADE_2012_zoom.csv', bucket = 'trase-storage')
comtrade12 <- read.csv(text = rawToChar(obj), quote = '', sep = ';')
obj <- get_object(object = 'data/1-TRADE/STATISTICAL_DATA/GLOBAL/COMTRADE/COMTRADE_ZOOM/COMTRADE_2013_zoom.csv', bucket = 'trase-storage')
comtrade13 <- read.csv(text = rawToChar(obj), quote = '', sep = ';')
obj <- get_object(object = 'data/1-TRADE/STATISTICAL_DATA/GLOBAL/COMTRADE/COMTRADE_ZOOM/COMTRADE_2014_zoom.csv', bucket = 'trase-storage')
comtrade14 <- read.csv(text = rawToChar(obj), quote = '', sep = ';')
obj <- get_object(object = 'data/1-TRADE/STATISTICAL_DATA/GLOBAL/COMTRADE/COMTRADE_ZOOM/COMTRADE_2015_zoom.csv', bucket = 'trase-storage')
comtrade15 <- read.csv(text = rawToChar(obj), quote = '', sep = ';')
obj <- get_object(object = 'data/1-TRADE/STATISTICAL_DATA/GLOBAL/COMTRADE/COMTRADE_ZOOM/COMTRADE_2016_zoom.csv', bucket = 'trase-storage')
comtrade16 <- read.csv(text = rawToChar(obj), quote = '', sep = ';')



for (f in as.vector(CD$file)){
	
	if (CD$country[CD$file == f] == 'ARGENTINA'){
	
		CD$hs_column[CD$file == f] <- hs_column <- 'HARMONIZED_CODE_PRODUCT_ENGLISH'
		CD$price_column[CD$file == f] <- price_column <- 'TOTAL_FOB_VALUE_US'
		CD$weight_column[CD$file == f] <- weight_column <- 'TOTAL_NET_WEIGHT_KG'
		CD$units_column[CD$file == f] <- units_column <- 'UNIDAD_ESTADSTICA'
			
		if (grepl("SICEX25", f)){
	
			CD$hs_column[CD$file == f] <- hs_column <- 'Harmonized.Code.Product.English'
			CD$price_column[CD$file == f] <- price_column <- 'TOTAL.FOB.Value..US..'
			CD$weight_column[CD$file == f] <- weight_column <- 'TOTAL.Quantity.1'
			CD$units_column[CD$file == f] <- units_column <- 'Unidad.Estadística'
			
		}
		
		if (grepl('SOURCE/CD_ARGENTINA_2010.csv', f)){
		
			CD$hs_column[CD$file == f] <- hs_column <- 'Harmonized.CodeProduct.English'
			CD$price_column[CD$file == f] <- price_column <- 'TOTAL.FOB.Value.US'
			CD$weight_column[CD$file == f] <- weight_column <- 'Cantidad.Estadstica'
			CD$units_column[CD$file == f] <- units_column <- 'Unidad.Estadstica'
		
		}
		
		release <- c('BEEF', 'CHICKEN', 'CORN', 'COTTON', 'LEATHER', 
					'TIMBER', 'WOOD PULP', 'SHRIMPS', 'SOYBEANS', 'SUGAR CANE')
		CD$comtrade_country[CD$file == f] <- 'Argentina'
		
	}
	
	if (CD$country[CD$file == f] == 'BOLIVIA'){
	
		if (grepl("SICEX25", f)){
	
			CD$hs_column[CD$file == f] <- hs_column <- 'Harmonized.Code.Product.English'
			CD$price_column[CD$file == f] <- price_column <- 'TOTAL.FOB.Value..US..'
			CD$weight_column[CD$file == f] <- weight_column <- 'TOTAL.Net.Weight..Kg.'
			
		} else if (grepl("CD_BOLIVIA_2010", f)){
			
			CD$hs_column[CD$file == f] <- hs_column <- 'COD_ARMONIZADOPRODUCTO_INGLES'
			CD$price_column[CD$file == f] <- price_column <- 'TOTAL_VALOR_FOB_US'
			CD$weight_column[CD$file == f] <- weight_column <- 'TOTAL_QUANTITY_1'
		
		} else{
	
			CD$hs_column[CD$file == f] <- hs_column <- 'COD_ARMONIZADOPRODUCTO_INGLES'
			CD$price_column[CD$file == f] <- price_column <- 'TOTAL_VALOR_FOB_US'
			CD$weight_column[CD$file == f] <- weight_column <- 'TOTAL_PESO_NETO_KG'
		
		}
			
		release <- c('CHICKEN', 'COFFEE', 'CORN', 'LEATHER', 
					'TIMBER', 'SOYBEANS')
		CD$comtrade_country[CD$file == f] <- 'Bolivia (Plurinational State of)'
		
	}
		
	if (grepl("data/1-TRADE/CD/EXPORT/BRAZIL/DATAMYNE/DASHBOARD/", f)){
	
		CD$hs_column[CD$file == f] <- hs_column <- 'PRODUCT_HS'
		CD$price_column[CD$file == f] <- price_column <- 'FOB_VALUE_USD'
		CD$weight_column[CD$file == f] <- weight_column <- 'NET_WEIGHT'
		
		release <- c('BEEF', 'CHICKEN', 'COCOA', 'COFFEE', 'CORN', 'COTTON', 'LEATHER', 
					'TIMBER', 'PALM OIL', 'WOOD PULP', 'SOYBEANS', 'SUGAR CANE')
		CD$comtrade_country[CD$file == f] <- 'Brazil'
		
	}
		
	if (grepl("data/1-TRADE/CD/EXPORT/BRAZIL/DATAMYNE/THIRD_PARTY/", f)){
	
		CD$hs_column[CD$file == f] <- hs_column <- 'COD_SUBITEM_NCM'
		CD$price_column[CD$file == f] <- price_column <- 'VMLE_DOLAR_BAL_EXP'
		CD$weight_column[CD$file == f] <- weight_column <- 'PESO_LIQ_MERC_BAL_EXP'
		
		release <- c('BEEF', 'CHICKEN', 'COCOA', 'COFFEE', 'CORN', 'COTTON', 'LEATHER', 
					'TIMBER', 'PALM OIL', 'WOOD PULP', 'SOYBEANS', 'SUGAR CANE')
		CD$comtrade_country[CD$file == f] <- 'Brazil'
		
	}
		
	if (CD$country[CD$file == f] == 'COLOMBIA'){
	
		if (grepl("SICEX25", f)){
	
			CD$hs_column[CD$file == f] <- hs_column <- 'Harmonized.Code.Product.English'
			CD$price_column[CD$file == f] <- price_column <- 'TOTAL.FOB.Value..US..'
			CD$weight_column[CD$file == f] <- weight_column <- 'TOTAL.Net.Weight..Kg.'
			
		} else{
			
			CD$hs_column[CD$file == f] <- hs_column <- 'HARMONIZED_CODE_PRODUCT_ENGLISH'
			CD$price_column[CD$file == f] <- price_column <- 'TOTAL_FOB_VALUE_US'
			CD$weight_column[CD$file == f] <- weight_column <- 'TOTAL_NET_WEIGHT_KG'
		
		}

		release <- c('BEEF', 'CHICKEN', 'COCOA', 'COFFEE', 'CORN', 'LEATHER', 
					'TIMBER', 'PALM OIL', 'WOOD PULP', 'SHRIMPS', 'SUGAR CANE')
		CD$comtrade_country[CD$file == f] <- c('Colombia')
		
	}
		
	if (CD$country[CD$file == f] == 'COSTARICA'){
	
		CD$hs_column[CD$file == f] <- hs_column <- 'HARMONIZED_CODE_PRODUCT_ENGLISH'
		CD$price_column[CD$file == f] <- price_column <- 'TOTAL_CIF_VALUE_US'
		CD$weight_column[CD$file == f] <- weight_column <- 'TOTAL_NET_WEIGHT_KG'
		
		release <- c('BEEF', 'COFFEE', 'LEATHER', 
					'TIMBER', 'PALM OIL', 'SHRIMPS', 'SOYBEANS', 'SUGAR CANE')
		CD$comtrade_country[CD$file == f] <- c('Costa Rica')
		
	}	
		
	if (CD$country[CD$file == f] == 'ECUADOR'){
	
		CD$hs_column[CD$file == f] <- hs_column <- 'HARMONIZED_CODEPRODUCT_SPANISH'
		CD$price_column[CD$file == f] <- price_column <- 'TOTAL_FOB_VALUE_US'
		CD$weight_column[CD$file == f] <- weight_column <- 'TOTAL_NET_WEIGHT_KG'
		
		release <- c('COCOA', 'COFFEE', 'LEATHER', 
					'PALM OIL', 'WOOD PULP', 'SHRIMPS')
		CD$comtrade_country[CD$file == f] <- c('Ecuador')
		
	}
		
	if (CD$country[CD$file == f] == 'MEXICO'){
		
		if (grepl("SICEX25", f)){
	
			CD$hs_column[CD$file == f] <- hs_column <- 'Harmonized.Code.Product.English'
			CD$price_column[CD$file == f] <- price_column <- 'TOTAL.FOB.Value..US..'
			CD$weight_column[CD$file == f] <- weight_column <- 'TOTAL.Quantity.1'
			
		} else{
		
			CD$hs_column[CD$file == f] <- hs_column <- 'HARMONIZED_CODE_PRODUCT_ENGLISH'
			CD$price_column[CD$file == f] <- price_column <- 'TOTAL_FOB_VALUE_US'
			CD$weight_column[CD$file == f] <- weight_column <- 'TOTAL_QUANTITY_1'
		
		}
	
		release <- c('BEEF', 'CHICKEN', 'COCOA', 'COFFEE', 'CORN', 'COTTON', 'LEATHER', 
					'TIMBER', 'WOOD PULP', 'SHRIMPS', 'SOYBEANS', 'SUGAR CANE')
		CD$comtrade_country[CD$file == f] <- c('Mexico')
		
	}		

	if (CD$country[CD$file == f] == 'PANAMA'){
		
		if (grepl("SICEX25", f)){
	
			CD$hs_column[CD$file == f] <- hs_column <- 'Harmonized.Code.Product.English'
			CD$price_column[CD$file == f] <- price_column <- 'TOTAL.FOB.Value..US..'
			CD$weight_column[CD$file == f] <- weight_column <- 'TOTAL.Net.Weight..Kg.'
			
		} else{
		
			CD$hs_column[CD$file == f] <- hs_column <- 'HARMONIZED_CODE_PRODUCT_ENGLISH'
			CD$price_column[CD$file == f] <- price_column <- 'TOTAL_FOB_VALUE_US'
			CD$weight_column[CD$file == f] <- weight_column <- 'TOTAL_NET_WEIGHT_KG'
		
		}
		
		release <- c('COFFEE', 'LEATHER', 
					'TIMBER', 'PALM OIL', 'SHRIMPS', 'SUGAR CANE')	
		CD$comtrade_country[CD$file == f] <- c('Panama')
		
	}
	
	if (grepl("data/1-TRADE/CD/EXPORT/PARAGUAY/SICEX/", f)){
	
		CD$hs_column[CD$file == f] <- hs_column <- 'Harmonized.CodeProduct.English'
		CD$price_column[CD$file == f] <- price_column <- 'TOTAL.FOB.Value.US'
		CD$weight_column[CD$file == f] <- weight_column <- 'TOTAL.Net.Weight.Kg'
		
		release <- c()
		CD$comtrade_country[CD$file == f] <- c('Paraguay')
		
	}
		
	if (grepl("data/1-TRADE/CD/EXPORT/PARAGUAY/MINTRADE/", f)){
	
		CD$hs_column[CD$file == f] <- hs_column <- 'hs8'
		CD$price_column[CD$file == f] <- price_column <- 'Valor.Fob.Dolar'
		CD$weight_column[CD$file == f] <- weight_column <- 'Kilo.Neto'
		
		release <- c('BEEF', 'CORN', 'LEATHER', 
					'TIMBER', 'SOYBEANS', 'SUGAR CANE')
		CD$comtrade_country[CD$file == f] <- c('Paraguay')
		
	}
		
	if (CD$country[CD$file == f] == 'PERU'){
		
		if (grepl("PERU/2016/CD_PERU_2016.csv", f) | grepl("PERU/2017/CD_PERU_2017.csv", f)){
		
			CD$hs_column[CD$file == f] <- hs_column <- 'COD..ARMONIZADOPRODUCTO.INGLES'
			CD$price_column[CD$file == f] <- price_column <- 'TOTAL.VALOR.FOB.US'
			CD$weight_column[CD$file == f] <- weight_column <- 'TOTAL.PESO.NETO.KG'
		
		} else{
		
			CD$hs_column[CD$file == f] <- hs_column <- 'X.Harmonized.CodeProduct.English.'
			CD$price_column[CD$file == f] <- price_column <- 'X.TOTAL.FOB.Value.US.'
			CD$weight_column[CD$file == f] <- weight_column <- 'X.TOTAL.Net.Weight.Kg.'
			
		}
		
		release <- c('CHICKEN', 'COCOA', 'COFFEE', 'CORN', 'LEATHER', 
					'TIMBER', 'PALM OIL', 'SHRIMPS', 'SUGAR CANE')
		CD$comtrade_country[CD$file == f] <- c('Peru')
		
	}
	
	if (CD$country[CD$file == f] == 'URUGUAY'){
		
		if (grepl("SICEX25", f)){
	
			CD$hs_column[CD$file == f] <- hs_column <- 'Harmonized.Code.Product.English'
			CD$price_column[CD$file == f] <- price_column <- 'TOTAL.FOB.Value..US..'
			CD$weight_column[CD$file == f] <- weight_column <- 'TOTAL.Net.Weight..Kg.'
		
		} else{
		
			CD$hs_column[CD$file == f] <- hs_column <- 'HARMONIZED_CODEPRODUCT_ENGLISH'
			CD$price_column[CD$file == f] <- price_column <- 'TOTAL_FOB_VALUE_US'
			CD$weight_column[CD$file == f] <- weight_column <- 'TOTAL_NET_WEIGHT_KG'
		
		}
		
		release <- c('BEEF', 'CHICKEN', 'COCOA', 'CORN', 'LEATHER', 
					'WOOD PULP', 'SOYBEANS')
		CD$comtrade_country[CD$file == f] <- c('Uruguay')
		
	}
	
	if (CD$country[CD$file == f] == 'VENEZUELA'){
		
		if (grepl("SICEX25", f)){
	
			CD$hs_column[CD$file == f] <- hs_column <- 'Harmonized.Code.Product.Spanish'
			CD$price_column[CD$file == f] <- price_column <- 'TOTAL.FOB.Value..US..'
			CD$weight_column[CD$file == f] <- weight_column <- 'TOTAL.Net.Weight..Kg.'
		
		} else{
		
			CD$hs_column[CD$file == f] <- hs_column <- 'HARMONIZED_CODE_PRODUCT_SPANISH'
			CD$price_column[CD$file == f] <- price_column <- 'TOTAL_FOB_VALUE_US'
			CD$weight_column[CD$file == f] <- weight_column <- 'TOTAL_NET_WEIGHT_KG'		
		
		}
		
		release <- c('COCOA', 'LEATHER', 
					'WOOD PULP', 'SHRIMPS')
		CD$comtrade_country[CD$file == f] <- c('Venezuela')
		
	}
		
	CD$release[CD$file == f] <- paste(release, collapse=", ")
	
}

write.table(CD, 'CD_AWS.csv', quote = FALSE, row.names = FALSE, dec = '.', sep = ';')


countries <- unique(as.vector(CD$country))

parked <- c('VENEZUELA', 'COLOMBIA', 'PANAMA', 'BOLIVIA', 'MEXICO')

countries <- countries[!countries %in% parked]


for (cc in countries){
	
	weights_table <- data.frame(commodity = as.vector( strsplit(as.character(CD$release[CD$country == cc][1]), ', ') ))
	names(weights_table) = c('commodity')
	
	
	for (f in CD$file[CD$country == cc]){
	
		obj <- get_object(object = f, bucket = 'trase-storage')
		data <- read.csv(text = rawToChar(obj), sep = ';', quote = '', row.names = NULL)
			
		if (grepl("data/1-TRADE/CD/EXPORT/PARAGUAY/MINTRADE/", f)){ data$hs8 <- as.integer(substr(gsub('\\.', '', data$NCM, perl=TRUE), 0, 8)) }
			
		for (i in 1:nrow(weights_table)){
			
			hs6_commodity <- as.vector(as.numeric(hs$code_value[ (hs$code_type == 'HS_6') & (hs$com_name == weights_table$commodity[i]) ]))

			data_commodity <- data[ as.numeric( substr(data[, CD$hs_column[CD$file == f] ] , 1, 6)) %in% hs6_commodity, ]
			
			# get weight whenever there is more than unit kg
			# countries: argentina 2010 and all others, ecuador
			# unit_column: UNIDAD_ESTADISTICA argentina
			# levels: 
			# conversions:  1 head of live animal is 475 kg
			#				1 liter of soy oil is 0.92 kilograms of soy
			# total weight for a year/countries: sum of all weight_column(where unit_column	== x and hs_column %in% soy_codes) * factor 
			#									+ sum of all weight_column(where unit_column == y and hs_column == b) * factor
			#									...
			# include column with, if column exists, levels of unit_column, with ': number of rows in this commodity, '
			
			
			
			##### correct weight_column format #############
			
			# goal: correct any formatting errors in data_commodity[, CD$weight_column[CD$file == f] ]
			
			data_commodity[, CD$weight_column[CD$file == f] ] <- as.character(data_commodity[, CD$weight_column[CD$file == f] ])
			
			# to correct, for each value, as character:
			
			for(i in 1:length(data_commodity[, CD$weight_column[CD$file == f] ])){
			
				# if clean then do nothing
				# clean means max one non-digit that's a dot
				
				if (grepl('[0-9]*[\\.]?[0-9]*' , data_commodity[, CD$weight_column[CD$file == f] ][i]){ }
				
				# if there is more than one dot in the string, remove all dots
				
				if (grepl('.*[\\.]?[0-9]*[\\.]?.*' , data_commodity[, CD$weight_column[CD$file == f] ][i]){
					data_commodity[, CD$weight_column[CD$file == f] ][i] <- gsub('.', '', data_commodity[, CD$weight_column[CD$file == f] ][i])
				}
				
				# if there is more than one comma in the string, remove all commas
			
				if (grepl('.*[,]?[0-9]*[,]?.*' , data_commodity[, CD$weight_column[CD$file == f] ][i]){
					data_commodity[, CD$weight_column[CD$file == f] ][i] <- gsub(',', '', data_commodity[, CD$weight_column[CD$file == f] ][i])
				}
			
				# if there's a comma in the second or third position, convert to dot
				# remove any other non-digit in the string
				
				# second position
				if (grepl('.*[,][0-9]' , data_commodity[, CD$weight_column[CD$file == f] ][i]){
					data_commodity[, CD$weight_column[CD$file == f] ][i] <- gsub(',', '.', data_commodity[, CD$weight_column[CD$file == f] ][i])
					data_commodity[, CD$weight_column[CD$file == f] ][i] <- gsub('[^0-9]', '', data_commodity[, CD$weight_column[CD$file == f] ][i])
				}
				# third position
				if (grepl('.*[,][0-9]{2}' , data_commodity[, CD$weight_column[CD$file == f] ][i]){
					data_commodity[, CD$weight_column[CD$file == f] ][i] <- gsub(',', '.', data_commodity[, CD$weight_column[CD$file == f] ][i])
					data_commodity[, CD$weight_column[CD$file == f] ][i] <- gsub('[^0-9]', '', data_commodity[, CD$weight_column[CD$file == f] ][i])
				}
			
				# if there's a comma in any position left of a dot,
				# remove the comma
			
				# wait to write: if there's a comma in the fourth position and no comma in any
				# second or third, then remove all commas from the string
		
				#else: add row to dataframe 
				# output dataframe as 'f_WEIGHT_FORMAT.csv'
				
			
				dat$y[i] <- dat$x[i]^2
			}
			
			
			
			
			
				
			weights_table$new_column[i] <- sum( as.numeric(data_commodity[, CD$weight_column[CD$file == f] ]) ) / 1000
						
			if (CD$year[CD$file == f] == 2005){ comtrade <- comtrade05 }
			if (CD$year[CD$file == f] == 2006){ comtrade <- comtrade06 }
			if (CD$year[CD$file == f] == 2007){ comtrade <- comtrade07 }
			if (CD$year[CD$file == f] == 2008){ comtrade <- comtrade08 }
			if (CD$year[CD$file == f] == 2009){ comtrade <- comtrade09 }
			if (CD$year[CD$file == f] == 2010){ comtrade <- comtrade10 }
			if (CD$year[CD$file == f] == 2011){ comtrade <- comtrade11 }
			if (CD$year[CD$file == f] == 2012){ comtrade <- comtrade12 }
			if (CD$year[CD$file == f] == 2013){ comtrade <- comtrade13 }
			if (CD$year[CD$file == f] == 2014){ comtrade <- comtrade14 }
			if (CD$year[CD$file == f] == 2015){ comtrade <- comtrade15 }
			if (CD$year[CD$file == f] == 2016){ comtrade <- comtrade16 }
			if (CD$year[CD$file == f] == 2017){ comtrade <- comtrade16 }
			if (CD$year[CD$file == f] == 2018){ comtrade <- comtrade16 }
				
			comtrade <- comtrade[(comtrade$country == CD$comtrade_country[CD$file == f]) & (comtrade$commodity %in% hs6_commodity),  ]
				
			weights_table$comtrade[i] <- sum(as.numeric( comtrade$comtrade_weight )) / 1000
			
			weights_table$deviation[i] <- weights_table$new_column[i] / weights_table$comtrade[i]
				
		}
		
		if (CD$year[CD$file == f] == 2005){ year <- 2005 }
		if (CD$year[CD$file == f] == 2006){ year <- 2006 }
		if (CD$year[CD$file == f] == 2007){ year <- 2007 }
		if (CD$year[CD$file == f] == 2008){ year <- 2008 }
		if (CD$year[CD$file == f] == 2009){ year <- 2009 }
		if (CD$year[CD$file == f] == 2010){ year <- 2010 }
		if (CD$year[CD$file == f] == 2011){ year <- 2011 }
		if (CD$year[CD$file == f] == 2012){ year <- 2012 }
		if (CD$year[CD$file == f] == 2013){ year <- 2013 }
		if (CD$year[CD$file == f] == 2014){ year <- 2014 }
		if (CD$year[CD$file == f] == 2015){ year <- 2015 }
		if (CD$year[CD$file == f] == 2016){ year <- 2016 }
		if (CD$year[CD$file == f] == 2017){ year <- 2016 }
		if (CD$year[CD$file == f] == 2018){ year <- 2016 }
			
		
		if (cc == 'COSTARICA'){names(weights_table)[names(weights_table) == 'new_column'] <- paste0( f , ' tons')}
		names(weights_table)[names(weights_table) == 'new_column'] <- paste0( strsplit(f, paste0('/', cc))[[1]][2] , ' tons')
		names(weights_table)[names(weights_table) == 'comtrade'] <- paste0('comtrade_tons_', year)
		names(weights_table)[names(weights_table) == 'deviation'] <- paste0('trase_per_comtrade')
	}
	
	write.table(weights_table, paste0('CD_weights_', cc, '.csv'), quote = FALSE, row.names = FALSE, dec = '.', sep = ';')

}


for (cc in countries){
	
	units_table <- data.frame(commodity = as.vector( strsplit(as.character(CD$release[CD$country == cc][1]), ', ') ))
	names(units_table) = c('commodity')
	
	
	for (f in CD$file[CD$country == cc]){
	
		if (!is.na(units_column)){
	
			obj <- get_object(object = f, bucket = 'trase-storage')
			data <- read.csv(text = rawToChar(obj), sep = ';', quote = '', row.names = NULL)
				
			if (grepl("data/1-TRADE/CD/EXPORT/PARAGUAY/MINTRADE/", f)){ data$hs8 <- as.integer(substr(gsub('\\.', '', data$NCM, perl=TRUE), 0, 8)) }
				
			for (i in 1:nrow(units_table)){
				
				hs6_commodity <- as.vector(as.numeric(hs$code_value[ (hs$code_type == 'HS_6') & (hs$com_name == units_table$commodity[i]) ]))

				data_commodity <- data[ as.numeric( substr(data[, CD$hs_column[CD$file == f] ] , 1, 6)) %in% hs6_commodity, ]
				
				# for each commodity, state which levels and how many of each
				
				levels_found <- as.vector(sort(unique(data_commodity[, CD$units_column[CD$file == f] ])))
				
				levels <- ''
				if (length(levels_found) != 0){
					for (k in 1:length(levels_found)){
						levels <- paste0(levels, levels_found[k], ': ', nrow(data_commodity[data_commodity[, CD$units_column[CD$file == f] ] == levels_found[k],]), ', ')					
					}
				}
				units_table$units[i] <- levels
				
			}
			
			if (cc == 'COSTARICA'){names(units_table)[names(units_table) == 'units'] <- paste0( f , ' units')}
			names(units_table)[names(units_table) == 'units'] <- paste0( strsplit(f, paste0('/', cc))[[1]][2] , ' units')
			
		}
	}
	
	write.table(units_table, paste0('CD_units_', cc, '.csv'), quote = FALSE, row.names = FALSE, dec = '.', sep = ';')

}



for (cc in countries){	
	
	for (f in CD$file[CD$country == cc]){

		obj <- get_object(object = f, bucket = 'trase-storage')
		data <- read.csv(text = rawToChar(obj), sep = ';', quote = '', row.names = NULL)
		
		print(f)
		print('')
		print(data[1:3,])
		print('')
		print('')
		print('')
		print('')

	}

}
