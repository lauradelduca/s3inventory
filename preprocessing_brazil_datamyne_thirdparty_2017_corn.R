## Brazil datamyne third party 2017 corn
## preprocessing

library(readxl)
library(data.table)
library(dplyr)

options(scipen=99999999)

din <- 'C:/Users/laura.delduca/Desktop/code/0502/brazil_corn/2017'
setwd(din)
ff <- list.files(din, pattern = 'csv', full = TRUE)

for (f in ff){
	data <- fread(f)
	print(f)
	print(dim(data))
	print(data[1:3,])
	print(names(data))
}

J <- list()
i = 1

for (f in ff){
	# read the file
	#j <- read_excel(f, 
	#				col_types = c("text", "text", "text", "text", "text", 
	#				"text", "text", "text", "text", "text", 
	#				"text", "text", "text", "text", "text", 
	#				"text", "numeric", "numeric", "numeric", "text",
	#				"numeric", "numeric", "numeric", "numeric", "text",
	#				"text", "text", "text", "text", "text",
	#				"text", "text"))
	
	j <- fread(f)
	
	#j <- read.csv(f, sep = ';')
	
	#j <- read_excel(f)
	
	#j <- read.csv(f, sep = ';')
	#				colClasses = c("text", "text", "text", "text", "text", 
	#				"text", "text", "text", "text", "text", 
	#				"text", "text", "text", "text", "text", 
	#				"text", "text", "numeric", "text", "numeric",
	#				"numeric", "numeric", "numeric", "text", "text",
	#				"text", "text", "text", "text", "text",
	#				"text", "text", "text", "text", "text",
	#				"text", "text", "text", "text", "text",
	#				"text", "text", "text"))
	
	## remove first 13 and last 0 rows if first file
	##j <- slice(j, 16:(nrow(j)-6))
	#j <- slice(j, 11:nrow(j))
	
	#j <- j[,1:12]
	# get index of all rows that have NAs across all columns
	k <- which( apply(j, 1, function(x) all(is.na(x))) )
	# remove those rows with all NAs
	if(length(k)>0) j<- j[-k,]
	
	#colnames(j) <- c("YEAR", "MONTH", "HS_CODE", "PRODUCT_DESCRIPTION", "EXPORTER",
	#				"EXPORTER_ADDRESS", "IMPORTER", "IMPORTER_ADDRESS", 
	#				"IMPORTER_COUNTRY", "COUNTRY_WHERE_IMPORT_PORT_LOCATED", 
	#				"PORT_OF_DISCHARGE", "PORT_OF_LOADING", "NET_WEIGHT_KGM",
	#				"FOB_VALUE_USD", "AMOUNT_OF_UNIT", "UNITS") 
					
	#colnames(j) <- c("NUMERO DECLARACION EXPORTACION", "FECHA AAAA-MM", "FECHA AAAA-MM-DD", 
	#				"NIT", "EXPORTADOR", "PAIS DESTINO", "POSICION", "DESCRIPCION ARANCEL",
	#				"DESCRIPCION DEL ARANCEL", "DESCRIPCION PRODUCTO", "DESCRIPCION PRODUCTO 2", 
	#				"DESCRIPCION PRODUCTO 3", "DESCRIPCION PRODUCTO 4", "DESCRIPCION PRODUCTO 5",
	#				"ESTADO DE LA MERCANCIA", "COD. ARMONIZADOPRODUCTO INGLES", 
	#				"DESC. COD. ARMONIZADO INGLES", "TOTAL CANTIDAD 1", "UNIDAD COMERCIAL 1",
	#				 "TOTAL VALOR FOB US", "VALOR FOB UNITARIO US CANTIDAD-1", 
	#				 "TOTAL PESO NETO KG", "TOTAL PESO BRUTO KG", "VIA", "ADUANA", 
	#				 "PUERTO DESTINO", "FECHA EMBARQUE", "NUMERO CONOCIMIENTO", 
	#				 "EMPRESA DE TRANSPORTE", "MATRICULA NAVE", "AGENTE ADUANA", "UNIDAD DE TRANSPORTE",
	#				 "UNIDAD 2", "FECHA RECEPCION DUE", "TIPO DOCUMENTO EXPORTADOR",
	#				 "ALMACEN", "TOTAL CANTIDAD 2", "FECHA REGULARIZACION", "ANO REGULACION", 
	#				 "ENTIDAD FINANCIERA", "NUMERO DE SERIE", "FECHA NUMERACION ORDEN DE EMBARQUE",
	#				 "NUMERO DE ORDEN DE EMBARQUE")
	
	#colnames(j) <- c("FECHA_AAAA_MM", "FECHA_AAAA_MM_DD", "EMPRESA", "ADUANA", "NOMBRE_CONSIGNATARIO",
	#					"PROVEEDOR", "PAIS_DESTINO", "PATRON_DE_EXPORTACION", "DESCRIPCION_ARANCEL",
	#					"POSICION", "COD_ARMONIZADOPRODUCTO_INGLES", "DESC_COD_ARMONIZADO_INGLES",
	#					"ACUERDO", "REGISTRO", "VALIDACION", "FECHA_VALIDACION", "CAMBIO",
	#					"TOTAL_VALOR_FOB_US", "TOTAL_VALOR_CIF_US", "DESCRIPCION_COMERCIAL_DEL_PRODUCTO",
	#					"TOTAL_PESO_NETO_KG", "TOTAL_PESO_ BRUTO_KG", "TOTAL_CANTIDAD_1", "TOTAL_BULTOS",                    
	#					"DESCRIPCION_ARANCEL_PAIS", "DEPTO_ORIGEN", "VIA", "DESC_CIIU",                        
	#					"CLAS_GRANDES_CATEGORIAS_ECONOMICAS", "CLAS_UNIFORME", "ACTIVIDAD_COMERCIAL",
	#					"PRODUCTOS_TRADICIONALES_Y_NO_TRAD")
						
	#colnames(j) <- c("NO_DECLARACIN", "FECHA_AAAA_MM", "FECHA_AAAA_MM_DD", "NIT", "DIGITO_CHEQUEO",
	#					"EXPORTADOR", "MARCA", "PRODUCTO_ATRIBUTOS", "PRODUCTO_VARIEDAD", "POSICIN",
	#					"DESCRIPCIN_ARANCEL", "DESCRIPCIN_ARANCEL_ARGENTINA", "COD_ARMONIZADO_PRODUCTO_INGLES",
	#					"DESC_COD_ARMONIZADO_INGLES", "TOTAL_CANTIDAD_1", "UNIDAD_COMERCIAL_1",
	#					"CANTIDAD_ESTADSTICA", "UNIDAD_ESTADSTICA", "TOTAL_VALOR_FOB_US",
	#					"VALOR_FOB_UNITARIO_US_CANTIDAD_1", "TOTAL_VALOR_CIF_US", "PAIS_ORIGEN",
	#					"VIA", "ADUANA", "PROVINCIA", "PAIS_DESTINO", "FLETE", "RGIMEN", "SUBREGIMEN",
	#					"INCOTERM")
	
	#argentina 2010
	#colnames(j) <- c("IMPORT_DECLARATION NUMBER" , "DATE_YYYY_MM", "DATE_YYYY_MM_DD", 
	#					"COMPANY_ID_NUMBER", "COMPANY_CHECK_DIGIT_ID", "EXPORTER",                                    
	#					"BRAND", "PRODUCT_ATTRIBUTES", "PRODUCT_VARIETY",                           
	#					"PRODUCT_SCHEDULE_B_CODE", "PRODUCT_DESCRIPTION_BY_SCHEDULE_B_CODE", "PRODUCT_SCHEDULE_B_CODE_DESCRIPTION_ARGENTINA",
	#					"HARMONIZED_CODE_PRODUCT_ENGLISH", "HARMONIZED_CODE_DESCRIPTION_ENGLISH", "TOTAL_QUANTITY_1",                          
	#					"MEASURE_UNIT_1_QUANTITY_1", "TOTAL_NET_WEIGHT_KG", "UNIDAD_ESTADSTICA",                         
	#					"TOTAL_FOB_VALUE_US", "FOB_PER_UNIT_QUANTITY_1", "TOTAL_CIF_VALUE_US",                           
	#					"COUNTRY_OF_ORIGIN", "TYPE_OF_TRANSPORT", "CUSTOM",                                 
	#					"STATE", "COUNTRY_OF_DESTINY", "FREIGHT",                                
	#					"REGIME", "SUBREGIME", "INCOTERM") 
	

	# brazil third party
	# colnames(j) <- c("NUM_RE", "NUM_DDE", "DIA_REGIS", "DIA_DESEMB", "DIA_EMBQ", "COD_IMPDR_EXPDR",
						# "NOME_IMPDR_EXPDR", "UA_LOCAL_DESBQ_EMBQ", "UF_DESBQ_EMBQ", "VIA_TRANSPORTE",             
						# "PAIS_ORIGEM_DESTINO", "COD_SUBITEM_NCM", "SUBITEM_NCM", 
						# "NOME_IMPORTADOR_ESTRANGEIRO", "DESCRICAO_PROD_EXP", "UNID_MEDIDA_EST",
						# "UNID_COMERC_PROD_EXP", "METRICA", "QTDE_PROD_BAL_EXP", "VMLE_DOLAR_UNID_BAL_EXP",    
						# "QTDE_EST_MERC_BAL_EXP", "VMLE_DOLAR_BAL_EXP", "PESO_LIQ_MERC_BAL_EXP")   
	
						
	# # add the data to the list
	J[[i]] <- j
	i <- i + 1
	
	
	#j <- data.frame(lapply(j, function(x) {gsub(";", ",", x)}))
	
	#write.table(j, paste0(yy, '_new.csv'), quote = FALSE, row.names = FALSE, dec = '.', sep = ';')
	
	#yy <- yy +1
}



# append all data, earlier stored in a list of dataframes in J
D <- do.call(rbind, J)


# set correct column names datamyne brazil 2015
# colnames(D) <- c('DATE', 'PRODUCT_HS', 'HS_DESCRIPTION', 'COUNTRY_OF_DESTINATION', 
				# 'PORT_OF_DEPARTURE', 'FOB_VALUE_USD', 'EXPORTER_NAME',
				# 'STATE_DEPARTMENT_OF_THE_EXPORTER', 'EXPORTER_MUNICIPALITY', 'TRANSPORT_METHOD',
				# 'NET_WEIGHT', 'EXPORTER_CNPJ')

				
#colnames(D) <- c("IMPORT_DECLARATION_NUMBER", "DATE_YYYY_MM", "DATE_YYYY_MM_DD", 
#				"COMPANY_ID_NUMBER", "EXPORTER", "ADDRESS", "IMPORTER", "COUNTRY_OF_ORIGIN", 
#				"COUNTRY_OF_DESTINY", "PRODUCT_SCHEDULE_B_CODE", 
#				"PRODUCT_DESCRIPTION_BY_SCHEDULE_B_CODE", "PRODUCT_DESCRIPTION", 
#				"HARMONIZED_CODEPRODUCT_SPANISH", "HARMONIZED_CODE_DESCRIPTION_ENGLISH", 
#				"TOTAL_QUANTITY_1", "MEASURE_UNIT_1_QUANTITY_1", "TOTAL_FOB_VALUE_US", 
#				"FOB_PER_UNIT_QUANTITY_1", "TOTAL_CIF_VALUE_US", "TOTAL_NET_WEIGHT_KG", 
#				"TOTAL_GROSS_WEIGHT_KG", "TYPE_OF_TRANSPORT", "CUSTOM", "DATE_LOADING", 
#				"BILL_OF_LADING", "TRANSPORT_COMPANY", "MANIFEST", "REGIME", 
#				"PORT_OF_LOADING", "COUNTERSIGN")


# in all columns replace ; with ,
D <- data.frame(lapply(D, function(x) {gsub(";", ",", x)}))

# add that hs column needs to have leading zeroes
# be sure that numbers are correctly formatted, no commas, only dot as decimal
!!


# delete duplicates NO!
#D <- D[!duplicated(D), ]

#unique(D$DATE_YYYY_MM)

# write file

### outdated, don't use write.csv2
#write.csv2(D, 'CD_BRAZIL_2016.csv', quote = FALSE, row.names = FALSE)

write.table(D, 'CD_ARGENTINA_2013.csv', quote = FALSE, row.names = FALSE, dec = '.', sep = ';')



## to clear the R environment
#rm(list = ls())


#nm <- c('DATE_YYYY_MM', 'DATE_YYYY_MM_DD', 'COMPANY_ID_NUMBER', 'EXPORTER', 
#'COUNTRY_OF_DESTINY', 'PRODUCT_SCHEDULE_B_CODE', 'PRODUCT_DESCRIPTION_BY_SCHEDULE_B_CODE', 'PRODUCT_DESCRIPTION_BY_SCHEDULE_B_CODE_IN_URUGUAY', 
#'PRODUCT_DESCRIPTION', 'HARMONIZED_CODEPRODUCT_ENGLISH', 'HARMONIZED_CODE_DESCRIPTION_ENGLISH', 
#'TOTAL_QUANTITY_1', 'MEASURE_UNIT_1_QUANTITY_1', 'TOTAL_FOB_VALUE_US', "FOB_PER_UNIT_QUANTITY_1", 'TOTAL_CIF_VALUE_US', 
#'TOTAL_NET_WEIGHT_KG', 'TOTAL_GROSS_WEIGHT_KG', 'TYPE_OF_TRANSPORT', 'CUSTOM', 'FREIGHT', 'INSURANCE',
#"TRANSPORT_COMPANY_TRANSPORT_USED", 'INCOTERM')



## replacing codes with new download

data <- fread(ff[1])
cotton <- fread(ff[2])
cotton <- cotton[, 1:12]

data$PRODUCT_HS <- formatC(data$PRODUCT_HS, width = 8, format = "d", flag = "0") 


data <- data[!(  (substr(data$PRODUCT_HS, 1, 6) == 120720) | 
				(substr(data$PRODUCT_HS, 1, 6) == 120721) | 
				(substr(data$PRODUCT_HS, 1, 4) == 5203) |
				(substr(data$PRODUCT_HS, 1, 6) == 120729) |
				(substr(data$PRODUCT_HS, 1, 6) == 140420) |
				(substr(data$PRODUCT_HS, 1, 6) == 151210) |
				(substr(data$PRODUCT_HS, 1, 6) == 151220) |
				(substr(data$PRODUCT_HS, 1, 6) == 151221) |
				(substr(data$PRODUCT_HS, 1, 6) == 151229) |
				(substr(data$PRODUCT_HS, 1, 6) == 230610) |
				(substr(data$PRODUCT_HS, 1, 6) == 470610) |
				(substr(data$PRODUCT_HS, 1, 2) == 52)  )]

names(cotton) <- names(data)

test <- rbind(data, cotton)

write.table(test, 'CD_BRAZIL_2016_NEW_COTTON.csv', quote = FALSE, row.names = FALSE, dec = '.', sep = ';')
