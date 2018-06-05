## Write total weight by HS6 code files
## Laura Del Duca

## needs to have library aws.s3 and AWS S3 credentials loaded into R
## needs current_folder, countries defined
## needs AWS S3 1-TRADE/CD/Export content loaded into dataframe CD
## needs HS codes loaded from get_hs_codes.R

yy <- 2013

files <- c('data/1-TRADE/CD/EXPORT/ARGENTINA/2013/SICEX25/FREIGHT0/CD_ARGENTINA_2013.csv', 
'data/1-TRADE/CD/EXPORT/ARGENTINA/2014/SICEX25/FREIGHT0/CD_ARGENTINA_2014.csv',
'data/1-TRADE/CD/EXPORT/ARGENTINA/2015/SICEX25/FREIGHT0/CD_ARGENTINA_2015.csv',
'data/1-TRADE/CD/EXPORT/ARGENTINA/2016/SICEX25/FREIGHT0/CD_ARGENTINA_2016.csv',
'data/1-TRADE/CD/EXPORT/ARGENTINA/2017/SICEX25/FREIGHT0/CD_ARGENTINA_2017.csv')

#for (cc in countries){
	
	for (f in files){
	
		
	
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
		
		if (yy == 2005){ comtrade <- comtrade05 }
		if (yy == 2006){ comtrade <- comtrade06 }
		if (yy == 2007){ comtrade <- comtrade07 }
		if (yy == 2008){ comtrade <- comtrade08 }
		if (yy == 2009){ comtrade <- comtrade09 }
		if (yy == 2010){ comtrade <- comtrade10 }
		if (yy == 2011){ comtrade <- comtrade11 }
		if (yy == 2012){ comtrade <- comtrade12 }
		if (yy == 2013){ comtrade <- comtrade13 }
		if (yy == 2014){ comtrade <- comtrade14 }
		if (yy == 2015){ comtrade <- comtrade15 }
		if (yy == 2016){ comtrade <- comtrade16 }
		if (yy == 2017){ comtrade <- comtrade16 }
		if (yy == 2018){ comtrade <- comtrade16 }
		
		#comtrade <- comtrade[comtrade$country == CD$comtrade_country[CD$file == f],]
		comtrade <- comtrade[comtrade$country == 'Argentina',]
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
					paste0(current_folder, '/CD_', cc, '_', yy, '_comtrade_by_code', '.csv'), 
					quote = FALSE, 
					row.names = FALSE, 
					dec = '.', 
					sep = ';')
		
		
		# one more thing that will be useful is having one file per commodity
		# vectors of codes are loaded if run in comtrade_check.R
		# filter resulting table for codes 
		result_beef <- result[as.numeric(result$HS6) %in% beef,]
		sum_net_weight_kg <- sum(result_beef$net_weight_kg)
		result_beef$perc_net_weight <- result_beef$net_weight_kg / sum_net_weight_kg
		
		result_chicken <- result[as.numeric(result$HS6) %in% chicken,]
		sum_net_weight_kg <- sum(result_chicken$net_weight_kg)
		result_chicken$perc_net_weight <- result_chicken$net_weight_kg / sum_net_weight_kg
		
		result_corn <- result[as.numeric(result$HS6) %in% corn,]
		sum_net_weight_kg <- sum(result_corn$net_weight_kg)
		result_corn$perc_net_weight <- result_corn$net_weight_kg / sum_net_weight_kg
		
		result_cotton <- result[as.numeric(result$HS6) %in% cotton,]
		sum_net_weight_kg <- sum(result_cotton$net_weight_kg)
		result_cotton$perc_net_weight <- result_cotton$net_weight_kg / sum_net_weight_kg
		
		result_leather <- result[as.numeric(result$HS6) %in% leather,]
		sum_net_weight_kg <- sum(result_leather$net_weight_kg)
		result_leather$perc_net_weight <- result_leather$net_weight_kg / sum_net_weight_kg
		
		result_timber <- result[as.numeric(result$HS6) %in% timber,]
		sum_net_weight_kg <- sum(result_timber$net_weight_kg)
		result_timber$perc_net_weight <- result_timber$net_weight_kg / sum_net_weight_kg
	
		result_woodpulp <- result[as.numeric(result$HS6) %in% woodpulp,]
		sum_net_weight_kg <- sum(result_woodpulp$net_weight_kg)
		result_woodpulp$perc_net_weight <- result_woodpulp$net_weight_kg / sum_net_weight_kg
		
		result_shrimps <- result[as.numeric(result$HS6) %in% shrimps,]
		sum_net_weight_kg <- sum(result_shrimps$net_weight_kg)
		result_shrimps$perc_net_weight <- result_shrimps$net_weight_kg / sum_net_weight_kg
		
		result_soy <- result[as.numeric(result$HS6) %in% soy,]
		sum_net_weight_kg <- sum(result_soy$net_weight_kg)
		result_soy$perc_net_weight <- result_soy$net_weight_kg / sum_net_weight_kg
		
		result_sugarcane <- result[as.numeric(result$HS6) %in% sugarcane,]
		sum_net_weight_kg <- sum(result_sugarcane$net_weight_kg)
		result_sugarcane$perc_net_weight <- result_sugarcane$net_weight_kg / sum_net_weight_kg
		
		
		# write them if (nrow > 0), with leading zeros
		if (nrow(result_beef) > 0){
					write.table(result_beef, 
					paste0(current_folder, '/CD_', cc, '_', yy, '_comtrade_by_code_beef', '.csv'), 
					quote = FALSE, 
					row.names = FALSE, 
					dec = '.', 
					sep = ';')}
		if (nrow(result_chicken) > 0){
					write.table(result_chicken, 
					paste0(current_folder, '/CD_', cc, '_', yy, '_comtrade_by_code_chicken', '.csv'), 
					quote = FALSE, 
					row.names = FALSE, 
					dec = '.', 
					sep = ';')}
		if (nrow(result_corn) > 0){
					write.table(result_corn, 
					paste0(current_folder, '/CD_', cc, '_', yy, '_comtrade_by_code_corn', '.csv'), 
					quote = FALSE, 
					row.names = FALSE, 
					dec = '.', 
					sep = ';')}
		if (nrow(result_cotton) > 0){
					write.table(result_cotton, 
					paste0(current_folder, '/CD_', cc, '_', yy, '_comtrade_by_code_cotton', '.csv'), 
					quote = FALSE, 
					row.names = FALSE, 
					dec = '.', 
					sep = ';')}
		if (nrow(result_leather) > 0){
					write.table(result_leather, 
					paste0(current_folder, '/CD_', cc, '_', yy, '_comtrade_by_code_leather', '.csv'), 
					quote = FALSE, 
					row.names = FALSE, 
					dec = '.', 
					sep = ';')}
		if (nrow(result_timber) > 0){
					write.table(result_timber, 
					paste0(current_folder, '/CD_', cc, '_', yy, '_comtrade_by_code_timber', '.csv'), 
					quote = FALSE, 
					row.names = FALSE, 
					dec = '.', 
					sep = ';')}
		if (nrow(result_woodpulp) > 0){
					write.table(result_woodpulp, 
					paste0(current_folder, '/CD_', cc, '_', yy, '_comtrade_by_code_woodpulp', '.csv'), 
					quote = FALSE, 
					row.names = FALSE, 
					dec = '.', 
					sep = ';')}
		if (nrow(result_shrimps) > 0){
					write.table(result_shrimps, 
					paste0(current_folder, '/CD_', cc, '_', yy, '_comtrade_by_code_shrimps', '.csv'), 
					quote = FALSE, 
					row.names = FALSE, 
					dec = '.', 
					sep = ';')}
		if (nrow(result_soy) > 0){
					write.table(result_soy, 
					paste0(current_folder, '/CD_', cc, '_', yy, '_comtrade_by_code_soy', '.csv'), 
					quote = FALSE, 
					row.names = FALSE, 
					dec = '.', 
					sep = ';')}
		if (nrow(result_sugarcane) > 0){
					write.table(result_sugarcane, 
					paste0(current_folder, '/CD_', cc, '_', yy, '_comtrade_by_code_sugarcane', '.csv'), 
					quote = FALSE, 
					row.names = FALSE, 
					dec = '.', 
					sep = ';')}
				
		
		yy <- yy + 1
	}
#}
