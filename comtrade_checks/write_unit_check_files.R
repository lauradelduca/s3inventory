## Write unit checks files
## Laura Del Duca

## needs to have library aws.s3 and AWS S3 credentials loaded into R
## needs AWS S3 1-TRADE/CD/Export content loaded into dataframe CD
## needs HS codes loaded from get_hs_codes.R


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
