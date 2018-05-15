## Write weight checks files
## Laura Del Duca

## needs to have library aws.s3 and AWS S3 credentials loaded into R
## needs AWS S3 1-TRADE/CD/Export content loaded into dataframe CD
## needs HS codes loaded from get_hs_codes.R


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
