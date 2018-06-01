## Write total weight by HS6 code files
## Laura Del Duca

## needs to have library aws.s3 and AWS S3 credentials loaded into R
## needs current_folder, countries defined
## needs AWS S3 1-TRADE/CD/Export content loaded into dataframe CD
## needs HS codes loaded from get_hs_codes.R


for (cc in countries){
	
	for (f in CD$file[CD$country == cc]){
	
		# f <- 'data/1-TRADE/CD/EXPORT/ARGENTINA/2013/SICEX25/CD_ARGENTINA_2013.csv'
	
		obj <- get_object(object = f, bucket = 'trase-storage')
		data <- read.csv(text = rawToChar(obj), sep = ';', quote = '', row.names = NULL)
			
		# if (grepl('BRAZIL', f)){ 
			# data[, CD$hs_column[CD$file == f] ] <- formatC(data[, CD$hs_column[CD$file == f] ], width = 8, format = "d", flag = "0") 
			# data[, CD$hs_column[CD$file == f] ] <- as.integer(substr(data[, CD$hs_column[CD$file == f] ], 0, 6))
		# }
		
		# chose only relevant columns
		data <- data[,c('Harmonized.Code.Product.English',
						'Harmonized.Code.Description.English',
						'Cantidad.Estadistica')]
		
		# aggregate by long HS code
		data <- aggregate(	data$Cantidad.Estadistica, 
							by = list(	HS6 = data$Harmonized.Code.Product.English,
										description = data$Harmonized.Code.Description.English), 
							FUN = sum, 
							na.rm = TRUE)
							
		setnames(data, old = c('x'), new = c('net_weight_kg'))
		
		# filter correct comtrade file by country
		# comtrade_weight is in kg
		
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
		
		comtrade <- comtrade[comtrade$country == CD$comtrade_country[CD$file == f],]
		comtrade <- comtrade[, c('commodity', 'comtrade_weight')]
		setnames(comtrade, old = c('commodity', 'comtrade_weight'), new = c('HS6', 'comtrade_weight_kg'))
		
		# merge the two files by short hs code
		result <- merge(data, comtrade, by = 'HS6')
		
		# add file/comtrade ratio column
		result$ratio <- result$net_weight_kg / result$comtrade_weight_kg
		
		# add leading zeros to HS6
		result$HS6 <- as.numeric(as.character(result$HS6))
		result$HS6 <- AT.add.leading.zeros(result$HS6, digits = 6)
		
		# write file, will be one per file
		write.table(result, 
					paste0(current_folder, '/CD_', cc, '_', CD$year[CD$file == f], '_comtrade_by_code', '.csv'), 
					quote = FALSE, 
					row.names = FALSE, 
					dec = '.', 
					sep = ';')
		
		# one more thing that will be useful is having one file per commodity
		# so create vectors of codes
		# filter resulting table for codes 
		# write them, with leading zeros
				
		}	
	}	
}
