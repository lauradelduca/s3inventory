## Select columns to use in comtrade_check.R for each dataset
## Store columns in dataframe CD, created in get_aws_content.R
## Laura Del Duca

## needs to have library aws.s3 and AWS S3 credentials loaded into R



for (f in as.vector(CD$file)){
	
	if (CD$country[CD$file == f] == 'ARGENTINA'){
	
		CD$hs_column[CD$file == f] <- hs_column <- 'HARMONIZED_CODE_PRODUCT_ENGLISH'
		CD$price_column[CD$file == f] <- price_column <- 'TOTAL_FOB_VALUE_US'
		CD$weight_column[CD$file == f] <- weight_column <- 'TOTAL_NET_WEIGHT_KG'
		CD$units_column[CD$file == f] <- units_column <- 'UNIDAD_ESTADSTICA'
			
		if (grepl("SICEX25", f)){
			CD$hs_column[CD$file == f] <- hs_column <- 'Harmonized.Code.Product.English'
			CD$price_column[CD$file == f] <- price_column <- 'TOTAL.FOB.Value..US..'
			#CD$weight_column[CD$file == f] <- weight_column <- 'TOTAL.Quantity.1'
			CD$weight_column[CD$file == f] <- weight_column <- 'Cantidad.Estadistica'
			#CD$weight_column[CD$file == f] <- weight_column <- 'corrected_net_weight_kg'
			#CD$weight_column_2[CD$file == f] <- weight_column_2 <- 'Cantidad.Estadistica'
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
			
		release <- c('CHICKEN', 'COFFEE', 'CORN', 'LEATHER', 'TIMBER', 'SOYBEANS')
		CD$comtrade_country[CD$file == f] <- 'Bolivia (Plurinational State of)'
	}
		
	if (grepl("data/1-TRADE/CD/EXPORT/BRAZIL/DATAMYNE/DASHBOARD/", f)){
	
		CD$hs_column[CD$file == f] <- hs_column <- 'Product.HS'
		CD$price_column[CD$file == f] <- price_column <- 'FOB.Value..US..'
		CD$weight_column[CD$file == f] <- weight_column <- 'Net.Weight'
				
		#release <- c('BEEF', 'CHICKEN', 'COCOA', 'COFFEE', 'CORN', 'COTTON', 'LEATHER', 
		#			'TIMBER', 'PALM OIL', 'WOOD PULP', 'SOYBEANS', 'SUGAR CANE')
		release <- c('PORK')
		CD$comtrade_country[CD$file == f] <- 'Brazil'
	}
		
	if (grepl("data/1-TRADE/CD/EXPORT/BRAZIL/DATAMYNE/THIRD_PARTY", f)){
		
		CD$hs_column[CD$file == f] <- hs_column <- 'COD.SUBITEM.NCM'
		CD$price_column[CD$file == f] <- price_column <- 'VMLE.DOLAR.BAL.EXP'
		CD$weight_column[CD$file == f] <- weight_column <- 'PESO.LIQ.MERC.BAL.EXP'
		
		#release <- c('BEEF', 'CHICKEN', 'COCOA', 'COFFEE', 'CORN', 'COTTON', 'LEATHER', 
		#			'TIMBER', 'PALM OIL', 'WOOD PULP', 'SOYBEANS', 'SUGAR CANE')
		release <- c('PORK')
		CD$comtrade_country[CD$file == f] <- 'Brazil'
	}

	if (CD$country[CD$file == f] == 'CHILE'){
	
		if (grepl("SICEX25", f)){
			CD$hs_column[CD$file == f] <- hs_column <- 'Harmonized.Code.Product.English'
			CD$price_column[CD$file == f] <- price_column <- 'TOTAL.FOB.Value..US..'
			CD$weight_column[CD$file == f] <- weight_column <- 'TOTAL.Net.Weight..Kg.'
		} else{
			CD$hs_column[CD$file == f] <- hs_column <- 'Harmonized CodeProduct English'
			CD$price_column[CD$file == f] <- price_column <- 'TOTAL FOB Value US'
			CD$weight_column[CD$file == f] <- weight_column <- 'TOTAL Net Weight Kg'
		}
		
		# so far not for release
		#release <- c('BEEF', 'CHICKEN', 'COCOA', 'COFFEE', 'CORN', 'LEATHER', 
		#			'TIMBER', 'PALM OIL', 'WOOD PULP', 'SHRIMPS', 'SUGAR CANE')
		# Chile not yet included in current COMTRADE zoom files, need to reproduce including Chile
		#CD$comtrade_country[CD$file == f] <- c('Chile')
		
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
		
		if (grepl('SICEX20', f)){
			CD$hs_column[CD$file == f] <- hs_column <- 'Harmonized.CodeProduct.English'
			CD$price_column[CD$file == f] <- price_column <- 'TOTAL.CIF.Value.US'
			CD$weight_column[CD$file == f] <- weight_column <- 'TOTAL.Net.Weight.Kg'
		}
		
		release <- c('BEEF', 'COFFEE', 'LEATHER', 
					'TIMBER', 'PALM OIL', 'SHRIMPS', 'SOYBEANS', 'SUGAR CANE')
		CD$comtrade_country[CD$file == f] <- c('Costa Rica')
		
	}	
		
	if (CD$country[CD$file == f] == 'ECUADOR'){
	
		CD$hs_column[CD$file == f] <- hs_column <- 'Harmonized.CodeProduct.Spanish'
		CD$price_column[CD$file == f] <- price_column <- 'TOTAL.FOB.Value.US'
		CD$weight_column[CD$file == f] <- weight_column <- 'TOTAL.Net.Weight.Kg'			
		
		release <- c('COCOA', 'COFFEE', 'LEATHER', 'PALM OIL', 'WOOD PULP', 'SHRIMPS')
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
		
		release <- c('COFFEE', 'LEATHER', 'TIMBER', 'PALM OIL', 'SHRIMPS', 'SUGAR CANE')	
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
	
		CD$hs_column[CD$file == f] <- hs_column <- 'HS6'
		CD$price_column[CD$file == f] <- price_column <- 'Valor.Fob.Dolar'
		CD$weight_column[CD$file == f] <- weight_column <- 'Kilo.Neto'
		
		if (grepl('ORIGINALS', f)){
			CD$hs_column[CD$file == f] <- hs_column <- 'hs6'
			CD$price_column[CD$file == f] <- price_column <- 'Valor.Fob.Dolar'
			CD$weight_column[CD$file == f] <- weight_column <- 'Kilo.Neto'
		}
		
		release <- c('BEEF', 'CORN', 'LEATHER', 'TIMBER', 'SOYBEANS', 'SUGAR CANE')
		CD$comtrade_country[CD$file == f] <- c('Paraguay')	
	}
		
	if (CD$country[CD$file == f] == 'PERU'){
	
		# 2012
		if (grepl("2012/SOURCE", f)){
			CD$hs_column[CD$file == f] <- hs_column <- 'Harmonized.CodeProduct.English'
			CD$price_column[CD$file == f] <- price_column <- 'TOTAL.FOB.Value.US'
			CD$weight_column[CD$file == f] <- weight_column <- 'TOTAL.Net.Weight.Kg'  
		}
		# 2013 - 2015
		if (grepl("SICEX25", f)){
			CD$hs_column[CD$file == f] <- hs_column <- 'Harmonized.Code.Product.English'
			CD$price_column[CD$file == f] <- price_column <- 'TOTAL.FOB.Value..US..'
			CD$weight_column[CD$file == f] <- weight_column <- 'TOTAL.Net.Weight..Kg.'  
		}
		# 2016 - 2017
		if (grepl("SICEX20", f)){
			CD$hs_column[CD$file == f] <- hs_column <- 'Cod..ArmonizadoProducto.Ingles'
			CD$price_column[CD$file == f] <- price_column <- 'TOTAL.Valor.FOB.US'
			CD$weight_column[CD$file == f] <- weight_column <- 'TOTAL.Peso.Neto.Kg'  
		}
		
		release <- c('CHICKEN', 'COCOA', 'COFFEE', 'CORN', 'LEATHER', 
					'TIMBER', 'PALM OIL', 'SHRIMPS', 'SUGAR CANE')
		CD$comtrade_country[CD$file == f] <- c('Peru')	
	}
	
	if (CD$country[CD$file == f] == 'URUGUAY'){
	
		CD$hs_column[CD$file == f] <- hs_column <- 'Harmonized.Code.Product.English'
		CD$price_column[CD$file == f] <- price_column <- 'TOTAL.FOB.Value..US..'
		CD$weight_column[CD$file == f] <- weight_column <- 'TOTAL.Net.Weight..Kg.'

		if ((grepl('/URUGUAY/2012', f)) ){
			CD$hs_column[CD$file == f] <- hs_column <- 'TOTAL_NET_WEIGHT_KG'
			CD$price_column[CD$file == f] <- price_column <- 'HARMONIZED_CODEPRODUCT_ENGLISH'
			CD$weight_column[CD$file == f] <- weight_column <- 'MEASURE_UNIT_1_QUANTITY_1'
		}
		
		release <- c('BEEF', 'CHICKEN', 'COCOA', 'CORN', 'LEATHER', 'WOOD PULP', 'SOYBEANS')
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
		
		release <- c('COCOA', 'LEATHER', 'WOOD PULP', 'SHRIMPS')
		CD$comtrade_country[CD$file == f] <- c('Venezuela')
		
	}
		
	CD$release[CD$file == f] <- paste(release, collapse=", ")
	
}



