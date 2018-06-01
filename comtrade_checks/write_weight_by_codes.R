## Write total weight by HS6 code files
## Laura Del Duca

## needs to have library aws.s3 and AWS S3 credentials loaded into R
## needs current_folder, countries defined
## needs AWS S3 1-TRADE/CD/Export content loaded into dataframe CD
## needs HS codes loaded from get_hs_codes.R


for (cc in countries){
	
	for (f in CD$file[CD$country == cc]){
	
		obj <- get_object(object = f, bucket = 'trase-storage')
		data <- read.csv(text = rawToChar(obj), sep = ';', quote = '', row.names = NULL)
			
		if (grepl('BRAZIL', f)){ 
			data[, CD$hs_column[CD$file == f] ] <- formatC(data[, CD$hs_column[CD$file == f] ], width = 8, format = "d", flag = "0") 
			data[, CD$hs_column[CD$file == f] ] <- as.integer(substr(data[, CD$hs_column[CD$file == f] ], 0, 6))
		}
		
		# chose only relevant columns
		data <- data[,c('Year', 
						'Product.Schedule.B.Code',
						'Harmonized.Code.Product.English',
						'Harmonized.Code.Description.English',
						'Cantidad.Estadistica')]
		
		# aggregate by long HS code
		
		# filter correct comtrade file by country etc
		
		# keep only relevant columns
		
		# rename columns if needed
		
		# merge the two files by short hs code
		
		# add file/comtrade ratio column
		
		# reorganize the weights to be next to each other maybe
		
		# write file, will be one per file
		
		# one more thing that will be useful is having one file per commodity
		# so create vectors of codes
		# filter resulting table for codes 
		# write them
		
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
				
			comtrade <- comtrade[comtrade$country == CD$comtrade_country[CD$file == f],  ]
				
			weights_table$comtrade[i] <- sum(as.numeric( comtrade$comtrade_weight )) / 1000
			weights_table$deviation[i] <- weights_table$new_column[i] / weights_table$comtrade[i]
		}
		
		year <- CD$year[CD$file == f]
		if (CD$year[CD$file == f] == 2017){ year <- 2016 }
		if (CD$year[CD$file == f] == 2018){ year <- 2016 }
		
	}
	
	write.table(weights_table, paste0(current_folder, '/', 'CD_weights_', cc, '.csv'), quote = FALSE, row.names = FALSE, dec = '.', sep = ';')
}
