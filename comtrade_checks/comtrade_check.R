## Checking customs declarations trade data against COMTRADE for 2018 level 1 release
## Laura Del Duca

require(stringr)
require(gsubfn)
require(dplyr)
require(readxl)
require(data.table)
require(libamtrack)
require(aws.s3)

options(scipen=9999999999)

setwd('C:/Users/laura.delduca/Desktop/code')

current_folder <- '0515'
aws_credentials_file <- 'R_aws.s3_credentials.R'


source(aws_credentials_file)		# load AWS S3 credentials
source(get_comtrade_files.R)		# load preprocessed COMTRADE files 2005 - 2016
source(get_hs_codes.R)				# load relevant HS codes from commodity dictionary

source(get_aws_content.R)			# load content of AWS into dataframe CD



## select columns for each country/dataset

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
			#CD$weight_column[CD$file == f] <- weight_column <- 'Cantidad.Estadistica'
			CD$weight_column_2[CD$file == f] <- weight_column_2 <- 'Cantidad.Estadistica'
			CD$units_column[CD$file == f] <- units_column <- 'Unidad.Estadistica'
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
		
		if (grepl('2017', f)){
			CD$hs_column[CD$file == f] <- hs_column <- 'COD.SUBITEM.NCM'
			CD$price_column[CD$file == f] <- price_column <- 'VMLE.DOLAR.BAL.EXP'
			CD$weight_column[CD$file == f] <- weight_column <- 'PESO.LIQ.MERC.BAL.EXP'
		}
		
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
	
		CD$hs_column[CD$file == f] <- hs_column <- 'hs6'
		CD$price_column[CD$file == f] <- price_column <- 'Valor.Fob.Dolar'
		CD$weight_column[CD$file == f] <- weight_column <- 'Kilo.Neto'
		
		release <- c('BEEF', 'CORN', 'LEATHER', 
					'TIMBER', 'SOYBEANS', 'SUGAR CANE')
		CD$comtrade_country[CD$file == f] <- c('Paraguay')
		
	}
		
	if (CD$country[CD$file == f] == 'PERU'){
	
		#2012
		CD$hs_column[CD$file == f] <- hs_column <- 'Harmonized.CodeProduct.English'
		CD$price_column[CD$file == f] <- price_column <- 'TOTAL.FOB.Value.US'
		CD$weight_column[CD$file == f] <- weight_column <- 'TOTAL.Net.Weight.Kg'
		
		if (grepl("SICEX25", f)){
			CD$hs_column[CD$file == f] <- hs_column <- 'Harmonized.Code.Product.English'
			CD$price_column[CD$file == f] <- price_column <- 'TOTAL.FOB.Value..US..'
			CD$weight_column[CD$file == f] <- weight_column <- 'TOTAL.Net.Weight..Kg.'  
		}
		if (grepl("CD_PERU_2016.csv", f) | grepl("CD_PERU_2017.csv", f)){
			CD$hs_column[CD$file == f] <- hs_column <- 'COD..ARMONIZADOPRODUCTO.INGLES'
			CD$price_column[CD$file == f] <- price_column <- 'TOTAL.VALOR.FOB.US'
			CD$weight_column[CD$file == f] <- weight_column <- 'TOTAL.PESO.NETO.KG'
		}
		
		release <- c('CHICKEN', 'COCOA', 'COFFEE', 'CORN', 'LEATHER', 
					'TIMBER', 'PALM OIL', 'SHRIMPS', 'SUGAR CANE')
		CD$comtrade_country[CD$file == f] <- c('Peru')
		
	}
	
	if (CD$country[CD$file == f] == 'URUGUAY'){

		if ((grepl('/URUGUAY/2012/CD_URUGUAY_2012.csv', f)) ){
			CD$hs_column[CD$file == f] <- hs_column <- 'TOTAL_NET_WEIGHT_KG'
			CD$price_column[CD$file == f] <- price_column <- 'HARMONIZED_CODEPRODUCT_ENGLISH'
			CD$weight_column[CD$file == f] <- weight_column <- 'MEASURE_UNIT_1_QUANTITY_1'
		}
		if (grepl("SICEX25", f)){
			CD$hs_column[CD$file == f] <- hs_column <- 'Harmonized.Code.Product.English'
			CD$price_column[CD$file == f] <- price_column <- 'TOTAL.FOB.Value..US..'
			CD$weight_column[CD$file == f] <- weight_column <- 'TOTAL.Net.Weight..Kg.'
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

write.table(CD, paste0(current_folder, '/', 'CD_AWS.csv'), quote = FALSE, row.names = FALSE, dec = '.', sep = ';')




## chose which countries to run

countries <- unique(as.vector(CD$country))
parked <- c('VENEZUELA', 'COLOMBIA', 'PANAMA', 'BOLIVIA', 'MEXICO', 'ARGENTINA', 'BRAZIL')
countries <- countries[!countries %in% parked]

countries <- c('ARGENTINA')
cc <- 'ARGENTINA'



## Trase - COMTRADE weights comparison


for (cc in countries){
	
	weights_table <- data.frame(commodity = as.vector( strsplit(as.character(CD$release[CD$country == cc][1]), ', ') ))
	names(weights_table) = c('commodity')
	
	
	for (f in CD$file[CD$country == cc]){
	
		#if (grepl('MINTRADE', f)){
		
			obj <- get_object(object = f, bucket = 'trase-storage')
			data <- read.csv(text = rawToChar(obj), sep = ';', quote = '', row.names = NULL)
				
			if (grepl("data/1-TRADE/CD/EXPORT/PARAGUAY/MINTRADE/", f)){ data$hs6 <- as.integer(substr(gsub('\\.', '', data$NCM, perl=TRUE), 0, 6)) }
			
			if (grepl('BRAZIL', f)){ 
				data[, CD$hs_column[CD$file == f] ] <- formatC(data[, CD$hs_column[CD$file == f] ], width = 8, format = "d", flag = "0") 
				data[, CD$hs_column[CD$file == f] ] <- as.integer(substr(data[, CD$hs_column[CD$file == f] ], 0, 6))
			}
			
			
			# ##### correct weight_column format #############
				
			# # goal: correct any formatting errors in data_commodity[, CD$weight_column[CD$file == f] ]
			# # data_release is used to correct formatting problems, data_commodity is used for the rest
			
			
			# # create empty data frame for remaining formatting problems
			# weight_format_problems <- data.frame(matrix(ncol = ncol(data), nrow = 0))
			# colnames(weight_format_problems) <- names(data)
			
			# # select only rows where hs code is relevant for this release
			# # only relevant commodities
			# commodities_vector <- as.vector( strsplit(as.character(CD$release[CD$country == cc][1]), ', ') )
			# hs_release <- hs[hs$com_name %in% commodities_vector[[1]],]
			# # only HS_6 codesas vector (don't include code_value, missing sometimes)
			# hs6_release <- as.vector(as.numeric(hs$code_value))
							

			# #data_release <- data[ as.numeric( substr(data[, CD$hs_column[CD$file == f] ] , 1, 6)) %in% hs6_release, ]
			# data_release <- data[ as.numeric( data[, CD$hs_column[CD$file == f] ]) %in% hs6_release, ]
			
			# # weight as character
			# data_release[, CD$weight_column[CD$file == f] ] <- as.character(data_release[, CD$weight_column[CD$file == f] ])
			
			# # to correct, for each value:
			
			# for(i in 1:nrow(data_release)){
				
				# # remove all spaces
				
				# data_release[, CD$weight_column[CD$file == f] ][i] <- gsub(' ', '', data_release[, CD$weight_column[CD$file == f] ][i])
				
				# # if clean then do nothing
				# # clean means max one non-digit that's a dot
				
				# if (grepl('^[0-9]*[\\.]?[0-9]$*' , data_release[, CD$weight_column[CD$file == f] ][i])){
				
				# # if there is more than one dot in the string, remove all dots
				
				# } else if (grepl('.*[\\.][0-9]*[\\.].*' , data_release[, CD$weight_column[CD$file == f] ][i])){
					# data_release[, CD$weight_column[CD$file == f] ][i] <- gsub('.', '', data_release[, CD$weight_column[CD$file == f] ][i])
				
				# # if there is more than one comma in the string, remove all commas
			
				# } else if (grepl('.*[,][0-9]*[,].*' , data_release[, CD$weight_column[CD$file == f] ][i])){
					# data_release[, CD$weight_column[CD$file == f] ][i] <- gsub(',', '', data_release[, CD$weight_column[CD$file == f] ][i])
			
				# # if there's a comma in the second or third position, convert to dot
				# # remove any other non-digit in the string
				
				# # second position
				# } else if (grepl('.*[,][0-9]$' , data_release[, CD$weight_column[CD$file == f] ][i])){
					# data_release[, CD$weight_column[CD$file == f] ][i] <- gsub(',', '.', data_release[, CD$weight_column[CD$file == f] ][i])
					# data_release[, CD$weight_column[CD$file == f] ][i] <- gsub('[^0-9]', '', data_release[, CD$weight_column[CD$file == f] ][i])
					
				# # third position
				# } else if (grepl('.*[,][0-9]{2}$' , data_release[, CD$weight_column[CD$file == f] ][i])){
					# data_release[, CD$weight_column[CD$file == f] ][i] <- gsub(',', '.', data_release[, CD$weight_column[CD$file == f] ][i])
					# data_release[, CD$weight_column[CD$file == f] ][i] <- gsub('[^0-9]', '', data_release[, CD$weight_column[CD$file == f] ][i])
			
				# # if there's a comma in any position left of a dot,
				# # remove the comma
				
				# } else if (grepl('.*[,].*[\\.]{1}.*' , data_release[, CD$weight_column[CD$file == f] ][i])){
					# data_release[, CD$weight_column[CD$file == f] ][i] <- gsub(',', '', data_release[, CD$weight_column[CD$file == f] ][i])
			
				# # wait to write: if there's a comma in the fourth position and no comma in any
				# # second or third, then remove all commas from the string
		
				# #else: add row to dataframe 
				# # output dataframe as 'weight_format_problems_f.csv'
				
				# } else {
				
					# weight_format_problems <- rbind(weight_format_problems, data_release[i,])
				
				# }

			# }
			
					
			# # # if every value is either without non-digits or with a comma in the fourth position:
			# # # remove all commas
			# # if (all( (grepl('^[0-9]*$' , data_release[, CD$weight_column[CD$file == f] ])) |
			# # (grepl('.*[,][0-9]{3}$' , data_release[, CD$weight_column[CD$file == f] ])))){
					
				# # data_release[, CD$weight_column[CD$file == f] ][i] <- gsub(',', '', data_release[, CD$weight_column[CD$file == f] ][i])
			
			
			# #for(i in 1:nrow(data_release)){
			
			# #	data_release[, CD$weight_column[CD$file == f] ][i] <- gsub(',', '', data_release[, CD$weight_column[CD$file == f] ][i])
			
			# #}
			
					
			# # write weight_format_problems table to csv
			# f2 <- gsub('/', '_', f)
			# write.table(weight_format_problems, paste0('weight_format_problems_', f2), quote = FALSE, row.names = FALSE, dec = '.', sep = ';')

			
			# # convert weight_column to type numeric
			# data_release[, CD$weight_column[CD$file == f] ] <- as.numeric(data_release[, CD$weight_column[CD$file == f] ])
			
			# ############################################
			
			
			
			# quick fix: remove all commas from weight_column (and weight_column_2 for Argentina)
			
			if ((cc == 'URUGUAY') | (cc == 'PERU') | (cc == 'PARAGUAY') | (cc == 'BRAZIL') ){
				data[, CD$weight_column[CD$file == f] ] <- as.character(data[, CD$weight_column[CD$file == f] ])
			
				for (i in 1:nrow(data)){
					data[, CD$weight_column[CD$file == f] ][i] <- gsub(',', '', data[, CD$weight_column[CD$file == f] ][i])
				}
			
				data[, CD$weight_column[CD$file == f] ] <- as.numeric(data[, CD$weight_column[CD$file == f] ])
			}
			
			
			#if ((cc == 'ARGENTINA') & (grepl('SICEX25', f))){
			#	data[, CD$weight_column_2[CD$file == f] ] <- as.character(data[, CD$weight_column_2[CD$file == f] ])
			#
			#	for (i in 1:nrow(data)){
			#		data[, CD$weight_column_2[CD$file == f] ][i] <- gsub(',', '', data[, CD$weight_column_2[CD$file == f] ][i])
			#	}
			#
			#	data[, CD$weight_column_2[CD$file == f] ] <- as.numeric(data[, CD$weight_column_2[CD$file == f] ])
			#}
			
			
			
			if (grepl('ARGENTINA', f)){
			
				for (i in 1:nrow(data)){
				
					# try using another column if weight = 0 in a record
					# for soy, using weight_column_2 seems to work
					
					# if (as.numeric(data[, CD$hs_column[CD$file == f] ][i] ) %in% soy){
					
						# data[, CD$weight_column[CD$file == f] ][i] <- data[, CD$weight_column_2[CD$file == f] ][i]
					
					# }
					
					
					
					
					# if (data[, CD$weight_column[CD$file == f] ][i] == 0){
					
						# data[, CD$weight_column[CD$file == f] ][i] <- as.numeric(gsub(',', '', data[, CD$weight_column_2[CD$file == f] ][i]))
					
					# }
			
					if (data[, CD$units_column[CD$file == f] ][i] == 'UNIDADES'){
				
						# chicken 010511
						if (as.numeric(data[, CD$hs_column[CD$file == f]][i]) == 10511){
					
							# one unidad of live animal is 475kg
							data[, CD$weight_column[CD$file == f] ][i] <- as.numeric(data[, CD$weight_column[CD$file == f] ][i]) * 475
					
						}
						
						# beef 010221
						if (as.numeric(data[, CD$hs_column[CD$file == f]][i]) == 10221){
						
							# one unidad of live animal is 475kg
							data[, CD$weight_column[CD$file == f] ][i] <- as.numeric(data[, CD$weight_column[CD$file == f] ][i]) * 475
						
						}
						
						# beef 010229
						if (as.numeric(data[, CD$hs_column[CD$file == f]][i]) == 10229){
						
							# one unidad of live animal is 475kg
							data[, CD$weight_column[CD$file == f] ][i] <- as.numeric(data[, CD$weight_column[CD$file == f] ][i]) * 475
						
						}
				
					}
					
					if (data[, CD$units_column[CD$file == f] ][i] == 'DESCONOCIDA'){
					
						# beef 010221
						if (as.numeric(data[, CD$hs_column[CD$file == f]][i]) == 10221){
						
							# one unidad of live animal is 475kg
							data[, CD$weight_column[CD$file == f] ][i] <- as.numeric(data[, CD$weight_column[CD$file == f] ][i]) * 475
						
						}
						
						# beef 010229
						if (as.numeric(data[, CD$hs_column[CD$file == f]][i]) == 10229){
						
							# one unidad of live animal is 475kg
							data[, CD$weight_column[CD$file == f] ][i] <- as.numeric(data[, CD$weight_column[CD$file == f] ][i]) * 475
						
						}
					
					}
					
					if (data[, CD$units_column[CD$file == f] ][i] == 'METROS CUBICOS'){
					
						# timber 440729 from cubic meter to ton conversion factor 0.7 
						if (as.numeric(data[, CD$hs_column[CD$file == f]][i]) == 440729){
						
							# # 1 cubic meter is 0.7 tons, so 1 cubic meter is 700 kg
							data[, CD$weight_column[CD$file == f] ][i] <- as.numeric(data[, CD$weight_column[CD$file == f] ][i]) * 700
						
						}
					
					}
					
					
					
					if (grepl('SICEX25', f)){
					
						if (as.numeric(data[, CD$hs_column[CD$file == f]][i]) %in% c(corn, cotton, woodpulp, soy, sugarcane)){
						
							# if code is of a certain commodity, work with weight_column_2
							data[, CD$weight_column[CD$file == f] ][i] <- as.numeric(data[, CD$weight_column_2[CD$file == f] ][i])
							
						}
					}
				}
			}
			
			
			
			for (i in 1:nrow(weights_table)){
				
				hs6_commodity <- as.vector(as.numeric(hs$code_value[ hs$com_name == weights_table$commodity[i] ]))

				data_commodity <- data[ as.numeric( substr(data[, CD$hs_column[CD$file == f] ] , 1, 6)) %in% hs6_commodity, ]
				
				# notes on argentina 
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
			
			
			year <- CD$year[CD$file == f]
			if (CD$year[CD$file == f] == 2017){ year <- 2016 }
			if (CD$year[CD$file == f] == 2018){ year <- 2016 }
				
			
			if (cc == 'COSTARICA'){names(weights_table)[names(weights_table) == 'new_column'] <- paste0( f , ' tons')}
			names(weights_table)[names(weights_table) == 'new_column'] <- paste0( strsplit(f, paste0('/', cc))[[1]][2] , ' tons')
			names(weights_table)[names(weights_table) == 'comtrade'] <- paste0('comtrade_tons_', year)
			names(weights_table)[names(weights_table) == 'deviation'] <- paste0('trase_per_comtrade')
		#}
	
	}
	
	write.table(weights_table, paste0('CD_weights_', cc, '.csv'), quote = FALSE, row.names = FALSE, dec = '.', sep = ';')

}





## units test ---------------------------

for (cc in countries){
	
	units_table <- data.frame(commodity = as.vector( strsplit(as.character(CD$release[CD$country == cc][1]), ', ') ))
	names(units_table) = c('commodity')
	
	
	for (f in CD$file[CD$country == cc]){
	
		if (!is.na(units_column)){
	
			obj <- get_object(object = f, bucket = 'trase-storage')
			data <- read.csv(text = rawToChar(obj), sep = ';', quote = '', row.names = NULL)
				
			if (grepl("data/1-TRADE/CD/EXPORT/PARAGUAY/MINTRADE/", f)){ data$hs6 <- as.integer(substr(gsub('\\.', '', data$NCM, perl=TRUE), 0, 8)) }
				
			for (i in 1:nrow(units_table)){
				
				hs6_commodity <- as.vector(as.numeric(hs$code_value[ hs$com_name == units_table$commodity[i] ]))

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





## helper: print header and first lines of all files of country in countries --------------------------

for (cc in countries){	
	
	for (f in CD$file[CD$country == cc]){
	
	#	if (grepl('MINTRADE', f)){

			obj <- get_object(object = f, bucket = 'trase-storage')
			data <- read.csv(text = rawToChar(obj), sep = ';', quote = '', row.names = NULL)
			
			print(f)
			print('')
			print(data[1:5,])
			print('')
			print('')
			print('')
			print('')
			
	#	}

	}

}


for (f in ff){
	data <- fread(f)
	print(f)
	print(data[1:3,])
}


## codes for release

todownload <- data.frame(file = CD$file, release = '')
todownload$release <- as.character(todownload$release)

for (f in as.vector(todownload$file)){

	release <- as.vector( strsplit(as.character(CD$release[CD$file == f][1]), ', ') )
	hs6_release <- as.vector(as.numeric(hs$code_value[ hs$com_name %in% release[[1]] ]))
	hs6_release <- as.character(formatC(hs6_release, width = 6, format = "d", flag = "0"))
	hs2_release <- sort( unique( substr(hs6_release, 1, 2) ) )
	
	todownload$release[todownload$file == f] <- paste(hs2_release, collapse=", ")

}

write.table(todownload, 'CD_todownload.csv', quote = FALSE, row.names = FALSE, dec = '.', sep = ';')




## helpers for argentina

soy <- as.vector(as.numeric(sort(unique(hs$code_value[hs$com_name == 'SOYBEANS']))))

shrimps <- c( 30616, 30617, 30635, 30636, 30695)

data_beef <- data[as.numeric(data$Harmonized.Code.Product.English) %in% beef,]
data_chicken <- data[as.numeric(data$Harmonized.Code.Product.English) %in% chicken,]
data_corn <- data[as.numeric(data$Harmonized.Code.Product.English) %in% corn,]
data_cotton <- data[as.numeric(data$Harmonized.Code.Product.English) %in% cotton,]
data_leather <- data[as.numeric(data$Harmonized.Code.Product.English) %in% leather,]
data_timber <- data[as.numeric(data$Harmonized.Code.Product.English) %in% timber,]
data_woodpulp <- data[as.numeric(data$Harmonized.Code.Product.English) %in% woodpulp,]
data_shrimps <- data[as.numeric(data$Harmonized.Code.Product.English) %in% shrimps,]
data_soy <- data[as.numeric(data$Harmonized.Code.Product.English) %in% soy,]
data_sugarcane <- data[as.numeric(data$Harmonized.Code.Product.English) %in% sugarcane,]

sum(as.numeric(gsub(',', '', data_beef$TOTAL.Quantity.1)))/1000
sum(as.numeric(gsub(',', '', data_beef$Cantidad.Estadística)))/1000
sum(as.numeric(gsub(',', '', data_chicken$TOTAL.Quantity.1)))/1000
sum(as.numeric(gsub(',', '', data_chicken$Cantidad.Estadística)))/1000
sum(as.numeric(gsub(',', '', data_corn$TOTAL.Quantity.1)))/1000
sum(as.numeric(gsub(',', '', data_corn$Cantidad.Estadística)))/1000
sum(as.numeric(gsub(',', '', data_cotton$TOTAL.Quantity.1)))/1000
sum(as.numeric(gsub(',', '', data_cotton$Cantidad.Estadística)))/1000
sum(as.numeric(gsub(',', '', data_leather$TOTAL.Quantity.1)))/1000
sum(as.numeric(gsub(',', '', data_leather$Cantidad.Estadística)))/1000
sum(as.numeric(gsub(',', '', data_timber$TOTAL.Quantity.1)))/1000
sum(as.numeric(gsub(',', '', data_timber$Cantidad.Estadística)))/1000
sum(as.numeric(gsub(',', '', data_woodpulp$TOTAL.Quantity.1)))/1000
sum(as.numeric(gsub(',', '', data_woodpulp$Cantidad.Estadística)))/1000
sum(as.numeric(gsub(',', '', data_shrimps$TOTAL.Quantity.1)))/1000
sum(as.numeric(gsub(',', '', data_shrimps$Cantidad.Estadística)))/1000
sum(as.numeric(gsub(',', '', data_soy$TOTAL.Quantity.1)))/1000
sum(as.numeric(gsub(',', '', data_soy$Cantidad.Estadística)))/1000
sum(as.numeric(gsub(',', '', data_sugarcane$TOTAL.Quantity.1)))/1000
sum(as.numeric(gsub(',', '', data_sugarcane$Cantidad.Estadística)))/1000


sort(unique(data_beef$Unidad.Estadística))
sort(unique(data_chicken$Unidad.Estadística))
sort(unique(data_corn$Unidad.Estadística))
sort(unique(data_cotton$Unidad.Estadística))
sort(unique(data_leather$Unidad.Estadística))
sort(unique(data_timber$Unidad.Estadística))
sort(unique(data_woodpulp$Unidad.Estadística))
sort(unique(data_shrimps$Unidad.Estadística))
sort(unique(data_soy$Unidad.Estadística))
sort(unique(data_sugarcane$Unidad.Estadística))
	

sort(unique(data_beef$Measure.Unit.1..Quantity.1.[data_beef$Unidad.Estadística != 'KILOGRAMOS']))
sort(unique(data_chicken$Measure.Unit.1..Quantity.1.[data_chicken$Unidad.Estadística != 'KILOGRAMOS']))
sort(unique(data_corn$Measure.Unit.1..Quantity.1.[data_corn$Unidad.Estadística != 'KILOGRAMOS']))
sort(unique(data_cotton$Measure.Unit.1..Quantity.1.[data_cotton$Unidad.Estadística != 'KILOGRAMOS']))
sort(unique(data_leather$Measure.Unit.1..Quantity.1.[data_leather$Unidad.Estadística != 'KILOGRAMOS']))
sort(unique(data_timber$Measure.Unit.1..Quantity.1.[data_timber$Unidad.Estadística != 'KILOGRAMOS']))
sort(unique(data_woodpulp$Measure.Unit.1..Quantity.1.[data_woodpulp$Unidad.Estadística != 'KILOGRAMOS']))
sort(unique(data_shrimps$Measure.Unit.1..Quantity.1.[data_shrimps$Unidad.Estadística != 'KILOGRAMOS']))
sort(unique(data_soy$Measure.Unit.1..Quantity.1.[data_soy$Unidad.Estadística != 'KILOGRAMOS']))
sort(unique(data_sugarcane$Measure.Unit.1..Quantity.1.[data_sugarcane$Unidad.Estadística != 'KILOGRAMOS']))