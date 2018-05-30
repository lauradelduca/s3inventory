## Write weight checks files
## Laura Del Duca

## needs to have library aws.s3 and AWS S3 credentials loaded into R
## needs current_folder, countries defined
## needs AWS S3 1-TRADE/CD/Export content loaded into dataframe CD
## needs HS codes loaded from get_hs_codes.R


for (cc in countries){
	
	weights_table <- data.frame(commodity = as.vector( strsplit(as.character(CD$release[CD$country == cc][1]), ', ') ))
	names(weights_table) = c('commodity')
	
	for (f in CD$file[CD$country == cc]){
	
		obj <- get_object(object = f, bucket = 'trase-storage')
		data <- read.csv(text = rawToChar(obj), sep = ';', quote = '', row.names = NULL)

		# # comment from here
		# if (grepl('ARGENTINA', f)){
		
			# for (i in 1:nrow(data)){
			
				# # try using another column if weight = 0 in a record
				# # for soy, using weight_column_2 seems to work
				# # if (as.numeric(data[, CD$hs_column[CD$file == f] ][i] ) %in% soy){
					# # data[, CD$weight_column[CD$file == f] ][i] <- data[, CD$weight_column_2[CD$file == f] ][i]
				# # }
				
				# # if (data[, CD$weight_column[CD$file == f] ][i] == 0){
					# # data[, CD$weight_column[CD$file == f] ][i] <- as.numeric(gsub(',', '', data[, CD$weight_column_2[CD$file == f] ][i]))
				# # }
		
				# if (data[, CD$units_column[CD$file == f] ][i] == 'UNIDADES'){
					# # chicken 010511
					# if (as.numeric(data[, CD$hs_column[CD$file == f]][i]) == 10511){
						# # one unidad of live animal is 475kg
						# data[, CD$weight_column[CD$file == f] ][i] <- as.numeric(data[, CD$weight_column[CD$file == f] ][i]) * 475
					# }
					# # beef 010221
					# if (as.numeric(data[, CD$hs_column[CD$file == f]][i]) == 10221){
						# # one unidad of live animal is 475kg
						# data[, CD$weight_column[CD$file == f] ][i] <- as.numeric(data[, CD$weight_column[CD$file == f] ][i]) * 475
					# }
					# # beef 010229
					# if (as.numeric(data[, CD$hs_column[CD$file == f]][i]) == 10229){
						# # one unidad of live animal is 475kg
						# data[, CD$weight_column[CD$file == f] ][i] <- as.numeric(data[, CD$weight_column[CD$file == f] ][i]) * 475
					# }
				# }
				
				# if (data[, CD$units_column[CD$file == f] ][i] == 'DESCONOCIDA'){
					# # beef 010221
					# if (as.numeric(data[, CD$hs_column[CD$file == f]][i]) == 10221){
						# # one unidad of live animal is 475kg
						# data[, CD$weight_column[CD$file == f] ][i] <- as.numeric(data[, CD$weight_column[CD$file == f] ][i]) * 475
					# }
					# # beef 010229
					# if (as.numeric(data[, CD$hs_column[CD$file == f]][i]) == 10229){
						# # one unidad of live animal is 475kg
						# data[, CD$weight_column[CD$file == f] ][i] <- as.numeric(data[, CD$weight_column[CD$file == f] ][i]) * 475
					# }
				# }
				
				# if (data[, CD$units_column[CD$file == f] ][i] == 'METROS CUBICOS'){
					# # timber 440729 from cubic meter to ton conversion factor 0.7 
					# if (as.numeric(data[, CD$hs_column[CD$file == f]][i]) == 440729){
						# # # 1 cubic meter is 0.7 tons, so 1 cubic meter is 700 kg
						# data[, CD$weight_column[CD$file == f] ][i] <- as.numeric(data[, CD$weight_column[CD$file == f] ][i]) * 700
					# }
				# }
				
				# if (grepl('SICEX25', f)){
					# if (as.numeric(data[, CD$hs_column[CD$file == f]][i]) %in% c(corn, cotton, woodpulp, soy, sugarcane)){
						# # if code is of a certain commodity, work with weight_column_2
						# data[, CD$weight_column[CD$file == f] ][i] <- as.numeric(data[, CD$weight_column_2[CD$file == f] ][i])
					# }
				# }
			# }
		# }
		# # until here
		
		
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
	}
	
	write.table(weights_table, paste0(current_folder, '/', 'CD_weights_', cc, '.csv'), quote = FALSE, row.names = FALSE, dec = '.', sep = ';')
}
