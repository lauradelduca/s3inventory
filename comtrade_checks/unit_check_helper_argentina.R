## unit check helper for Argentina
## is FOB/weight unusual?

# define fcn detect_unusual_units(table)
# input: table with (at least) columns Product.Schedule.B.Code, Cantidad.Estadistica, fob_per_kg
# output: table of rows with unusual units
# creates new column with median (or mean? try both) fob_per_kg per HS code
# creates table selecting all rows where fob_per_kg is <> median(or mean)*700 to detect misreported tons/kgs


table <- data_shrimps[data_shrimps$'Unidad.Estadistica' == 'KILOGRAMOS',]


detect_unusual_units <- function(table){
		
	table_means <- aggregate( fob_per_kg ~ Product.Schedule.B.Code, table, mean )
	setnames(table_means, old = c('fob_per_kg'), new = c('mean_fob_per_kg'))
	table <- merge(table, table_means, by = c('Product.Schedule.B.Code'))
	
	table_medians <- aggregate( fob_per_kg ~ Product.Schedule.B.Code, table, median )
	setnames(table_medians, old = c('fob_per_kg'), new = c('median_fob_per_kg'))
	table <- merge(table, table_medians, by = c('Product.Schedule.B.Code'))
	
	table_of_unusual_ratios <- table[(table$fob_per_kg < table$mean_fob_per_kg * 0.007 ) | (table$fob_per_kg > table$mean_fob_per_kg * 700) |
									(table$fob_per_kg < table$median_fob_per_kg * 0.007 ) | (table$fob_per_kg > table$median_fob_per_kg * 700) ,]
	
	return(table_of_unusual_ratios)
}