## 2018-06-15 download data from SIDRA IBGE using sidrar package
## tables 1092, 1093, 1094, 6669
## Laura Del Duca

rm(list=ls(all=TRUE))

require(stringr)
require(gsubfn)
require(dplyr)
require(readxl)
require(data.table)
require(libamtrack)
require(tidyr)

require(sidrar)
require(aws.s3)

options(scipen=99999999)

setwd('C:/Users/laura.delduca/Desktop/code')
current_folder <- '0618'

source('R_aws.s3_credentials.R')					# load AWS S3 credentials



### CATTLE: Table 1092 Animal kills per trimester along with carcass weights

# api test: only works for up to 20000 records
#cattle_api <- get_sidra(api = 'http://api.sidra.ibge.gov.br/values/t/1092/n1/all/n3/all/v/allxp/p/all/c12716/all/c18/all/c12529/all')

# get_sidra
# issue: limit of 100000 records
# issue: geo defaults to just the first argument
# run once for geo = 'Brazil', then once per State, get IDs form csv downloaded in browser, rbind all

cattle <- get_sidra(x = 1092,
          period = 'all',
          geo = 'Brazil',
          format = 3)
		  
table_from_browser <- fread(paste0(current_folder, '/', 'tabela1092_all.csv'), stringsAsFactors = FALSE)

codes <- as.vector(unique(as.numeric(table_from_browser$CÃ³d.)))
codes <- codes[-c(1)] 		# the first in codes, 1, is for Brazil, so skip

# same dimensions for each state/Brazil don't seem to be an error
for (i in codes){		
	cattle_state <- get_sidra(x = 1092,
          period = 'all',
          geo = 'State',
          geo.filter = i,
          format = 3)
	if (i == codes[1]){ names(cattle) <- names(cattle_state) }
	cattle <- rbind(cattle, cattle_state)
}


# bring into wider format, the units are better attached to column names
tapply(cattle$'Unidade de Medida', cattle$Variável, unique)
  # Animais abatidos   Número de informantes Peso total das carcaças 
    # "Cabeças"              "Unidades"           "Quilogramas"
			  
cattle$Variável <- paste0(cattle$Variável, ' (', cattle$'Unidade de Medida', ')')
cattle <- cattle[,-which(names(cattle) %in% c('Unidade de Medida'))]

cattle <- spread(	data = cattle, 
					key = Variável,
					value = Valor)
			
## add year column

cattle <- cattle[cattle$'Referência temporal' == 'Total do trimestre',]
cattle <- cattle[cattle$'Tipo de inspeção' == 'Total',]

cattle$Year <- str_sub(cattle$Trimestre,-4,-1)
			
			
## remove special characters from column names
setnames(	cattle, 
			old = c(	'Unidade da Federação (Código)', 
						'Unidade da Federação', 
						'Referência temporal', 
						'Tipo de inspeção', 
						'Animais abatidos (Cabeças)', 
						'Número de informantes (Unidades)', 
						'Peso total das carcaças (Quilogramas)'	),
			new = c(	'Unidade da Federacao (Codigo)', 
						'Unidade da Federacao', 
						'Referencia temporal', 
						'Tipo de inspecao', 
						'Animais abatidos (Cabecas)', 
						'Numero de informantes (Unidades)', 
						'Peso total das carcacas (Quilogramas)'	)	)

			
# save cattle	  
write.table(cattle, paste0(current_folder, '/', 'IBGE_1092_cattle_1997_1_2018.csv'), quote = FALSE, 
			row.names = FALSE, dec = '.', sep = ';')

zz <- rawConnection(raw(0), "r+")
write.table(cattle, zz, quote = FALSE, row.names = FALSE, dec = '.', sep = ';')
put_object(	file = rawConnectionValue(zz), bucket = 'trase-storage', 
			object = paste0('data/2-PRODUCTION/STATISTICS/BRAZIL/IBGE/beef/beef_1997_1_2018.csv') )
close(zz)



### PIGS: Table 1093 Animal kills per trimester along with carcass weights
		  
pigs <- get_sidra(x = 1093,
          period = 'all',
          geo = 'Brazil',
          format = 3)
		  
table_from_browser <- fread(paste0(current_folder, '/', 'tabela1093_all.csv'), stringsAsFactors = FALSE)

codes <- as.vector(unique(as.numeric(table_from_browser$CÃ³d.)))
codes <- codes[-c(1)] 		# the first in codes, 1, is for Brazil, so skip

for (i in codes){		
	pigs_state <- get_sidra(x = 1093,
          period = 'all',
          geo = 'State',
          geo.filter = i,
          format = 3)
	if (i == codes[1]){ names(pigs) <- names(pigs_state) }
	pigs <- rbind(pigs, pigs_state)
}


# bring into wider format, the units are better attached to column names
tapply(pigs$'Unidade de Medida', pigs$Variável, unique)
 # Animais abatidos   Número de informantes Peso total das carcaças 
    # "Cabeças"              "Unidades"           "Quilogramas" 

pigs$Variável <- paste0(pigs$Variável, ' (', pigs$'Unidade de Medida', ')')
pigs <- pigs[,-which(names(pigs) %in% c('Unidade de Medida'))]

pigs <- spread(	data = pigs, 
				key = Variável,
				value = Valor)
				
				
## add year column

pigs <- pigs[pigs$'Referência temporal' == 'Total do trimestre',]
pigs <- pigs[pigs$'Tipo de inspeção' == 'Total',]

pigs$Year <- str_sub(pigs$Trimestre,-4,-1)
			
			
## remove special characters from column names
setnames(	pigs, 
			old = c(	'Unidade da Federação (Código)', 
						'Unidade da Federação', 
						'Referência temporal', 
						'Tipo de inspeção', 
						'Animais abatidos (Cabeças)', 
						'Número de informantes (Unidades)', 
						'Peso total das carcaças (Quilogramas)'	),
			new = c(	'Unidade da Federacao (Codigo)', 
						'Unidade da Federacao', 
						'Referencia temporal', 
						'Tipo de inspecao', 
						'Animais abatidos (Cabecas)', 
						'Numero de informantes (Unidades)', 
						'Peso total das carcacas (Quilogramas)'	)	)

			
# save pigs
write.table(pigs, paste0(current_folder, '/', 'IBGE_1093_pigs_1997_1_2018.csv'), quote = FALSE, 
			row.names = FALSE, dec = '.', sep = ';')

zz <- rawConnection(raw(0), "r+")
write.table(pigs, zz, quote = FALSE, row.names = FALSE, dec = '.', sep = ';')
put_object(	file = rawConnectionValue(zz), bucket = 'trase-storage', 
			object = paste0('data/2-PRODUCTION/STATISTICS/BRAZIL/IBGE/pigs/pigs_1997_1_2018.csv') )
close(zz)



### CHICKEN: Table 1094 Animal kills per trimester along with carcass weights
		  
chicken <- get_sidra(x = 1094,
          period = 'all',
          geo = 'Brazil',
          format = 3)
		  
table_from_browser <- fread(paste0(current_folder, '/', 'tabela1094_all.csv'), stringsAsFactors = FALSE)
codes <- as.vector(unique(as.numeric(table_from_browser$CÃ³d.)))
codes <- codes[-c(1)] 		# the first in codes, 1, is for Brazil, so skip

for (i in codes){		
	chicken_state <- get_sidra(x = 1094,
          period = 'all',
          geo = 'State',
          geo.filter = i,
          format = 3)
	if (i == codes[1]){ names(chicken) <- names(chicken_state) }
	chicken <- rbind(chicken, chicken_state)
}


# bring into wider format, the units are better attached to column names
tapply(chicken$'Unidade de Medida', chicken$Variável, unique)
   # Animais abatidos   Número de informantes Peso total das carcaças 
      # "Cabeças"              "Unidades"           "Quilogramas" 

chicken$Variável <- paste0(chicken$Variável, ' (', chicken$'Unidade de Medida', ')')
chicken <- chicken[,-which(names(chicken) %in% c('Unidade de Medida'))]
chicken <- spread(	data = chicken, 
				key = Variável,
				value = Valor)
				
	
## add year column and aggregate by year

chicken <- chicken[chicken$'Referência temporal' == 'Total do trimestre',]
chicken <- chicken[chicken$'Tipo de inspeção' == 'Total',]		# refers to informant

chicken$Year <- str_sub(chicken$Trimestre,-4,-1)

chicken <- chicken[,-which(names(chicken) %in% c('Trimestre'))]
chicken <- chicken[,-which(names(chicken) %in% c('Referência temporal'))]
chicken <- chicken[,-which(names(chicken) %in% c('Tipo de inspeção'))]

# need to check if aggregating Número de informantes (Unidades) by sum makes sense

test1 <- aggregate(	x = c(	chicken$'Animais abatidos (Cabeças)', 
							chicken$'Número de informantes (Unidades)', 
							chicken$'Peso total das carcaças (Quilogramas)'), 
					by = list(	'Unidade da Federação (Código)' = chicken$'Unidade da Federação (Código)', 
								'Unidade da Federação' = chicken$'Unidade da Federação', 
								Year = chicken$'Year'), 
					FUN = sum, 
					na.rm = TRUE)
head(test1)


test2 <- aggregate(. ~'Unidade da Federação (Código)'+'Unidade da Federação'+Year, data = chicken, sum, na.rm = TRUE)



# save chicken
write.table(chicken, paste0(current_folder, '/', 'IBGE_1094_chicken_1997_1_2018.csv'), quote = FALSE, 
			row.names = FALSE, dec = '.', sep = ';')

zz <- rawConnection(raw(0), "r+")
write.table(chicken, zz, quote = FALSE, row.names = FALSE, dec = '.', sep = ';')
put_object(	file = rawConnectionValue(zz), bucket = 'trase-storage', 
			object = paste0('data/2-PRODUCTION/STATISTICS/BRAZIL/IBGE/chicken/chicken_1997_1_2018.csv') )
close(zz)



### Summary: Table 6669 Slaughtered animals, by herd type

herd <- get_sidra(x = 6669,
          period = 'all',
          geo = 'Brazil',
          format = 3)
# has only Brazil, no states

#table_from_browser <- fread(paste0(current_folder, '/', 'tabela6669_all.csv'), stringsAsFactors = FALSE)

# bring into wide format, the units are better attached to column names
tapply(herd$'Unidade de Medida', herd$Variável, unique)
  # Animais abatidos Peso total das carcaças 
  # "Mil cabeças"             "Toneladas" 
  
herd$Variável <- paste0(herd$Variável, ' (', herd$'Unidade de Medida', ')')
herd <- herd[,-which(names(herd) %in% c('Unidade de Medida'))]
herd <- spread(	data = herd, 
				key = Variável,
				value = Valor)

herd$Year <- str_sub(herd$Trimestre,-4,-1)		# add Year column


# save herd
write.table(herd, paste0(current_folder, '/', 'IBGE_6669_beef_pigs_chicken_1_2018.csv'), quote = FALSE, 
			row.names = FALSE, dec = '.', sep = ';')

zz <- rawConnection(raw(0), "r+")
write.table(herd, zz, quote = FALSE, row.names = FALSE, dec = '.', sep = ';')
put_object(	file = rawConnectionValue(zz), bucket = 'trase-storage', 
			object = paste0('data/2-PRODUCTION/STATISTICS/BRAZIL/IBGE/herd_beef_pigs_chicken/herd_beef_pigs_chicken_1_2018.csv') )
close(zz)



# clean up
gc()	