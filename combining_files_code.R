## combine files into one
## need to have same variables


library(readxl)
library(data.table)
library(dplyr)


options(scipen=99999999)
 

# set location of files and get all file paths
din <- 'C:/Users/laura.delduca/Desktop/code/0420/peru'

setwd(din)

ff <- list.files(din, pattern = '\\.csv$', full = TRUE)


# create an empty list to store the data of each file
J <- list()

i = 1
# for each file...
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
	j <- slice(j, 3:nrow(j))
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
	
	# argentina 2015
	#[1] "IMPORT_DECLARATION_NUMBER"              "DATE_YYYY_MM"                           "DATE_YYYY_MM_DD"                       
	#[4] "COMPANY_ID_NUMBER"                      "COMPANY_CHECK_DIGIT_ID"                 "EXPORTER"                              
	#[7] "CONTACT"                                "CITY"                                   "STATE"                                 
	#[10] "TELEPHONE"                              "FAX"                                    "ADDRESS"                               
	#[13] "EMAIL"                                  "TYPE_OF_COMPANY"                        "EXPORT_TYPE"                           
	#[16] "UAP"                                    "ALTEX"                                  "IMPORTER"                              
	#[19] "COUNTRY_OF_DESTINY"                     "IMPORTER_CITY"                          "IMPORTER_ADDRESS"                      
	#[22] "PRODUCT_SCHEDULE_B_CODE"                "PRODUCT_DESCRIPTION_BY_SCHEDULE_B_CODE" "HARMONIZED_CODE_PRODUCT_ENGLISH"       
	#[25] "HARMONIZED_CODE_DESCRIPTION_ENGLISH"    "TOTAL_QUANTITY_1"                       "MEASURE_UNIT_1_QUANTITY_1"             
	#[28] "TOTAL_FOB_VALUE_US"                     "FOB_PER_UNIT_QUANTITY_1"                "TOTAL_CIF_VALUE_US"                    
	#[31] "TOTAL_NET_WEIGHT_KG"                    "TOTAL_GROSS_WEIGHT_KG"                  "TYPE_OF_TRANSPORT"                     
	#[34] "CUSTOM"                                 "STATE_OF_ORIGIN"                        "STATE_OF_PROCEDENCE"                   
	#[37] "CITY_DEPARTURE"                         "DATE_LOADING"                           "CUSTOM_LOADING"                        
	#[40] "AUTO_LOADING_NUMBER"                    "FREIGHT"                                "INSURANCE"                             
	#[43] "VALUE_COSTS"                            "ADDED_VALUE"                            "VALUE_COP"                             
	#[46] "LOADING"                                "DECLARANT_CO_ID_NUMBER"                 "DECLARANT_COMPANY"                     
	#[49] "WAY_OF_PAYMENT"                         "TRANSPORT_INFO"                         "EXPORT_TRANSIT"                        
	#[52] "REGIME"                                 "AGREEMENT"                              "ISIC"                                  
	#[55] "ISIC_DESCRIPTION"                       "CUODE"                                  "CUODE_DESCRIPTION"   
						
						
						
	#bolivia 2015, 2016
	#colnames(j) <- c("FECHA_AAAA_MM", "FECHA_AAAA_MM_DD", "EMPRESA", "ADUANA",                         
	#					"NOMBRE_CONSIGNATARIO", "PROVEEDOR", "PAIS_DESTINO", "PATRON_DE_EXPORTACION",           
	#					"DESCRIPCION_ARANCEL", "POSICION", "COD_ARMONIZADOPRODUCTO_INGLES", "DESC_COD_ARMONIZADO_INGLES",      
	#					"ACUERDO", "REGISTRO", "VALIDACION", "FECHA_VALIDACION",              
	#					"CAMBIO", "TOTAL_VALOR_FOB_US", "TOTAL_VALOR_CIF_US", "DESCRIPCION_COMERCIAL_DEL_PRODUCTO",
	#					"TOTAL_PESO_NETO_KG", "TOTAL_PESO_.BRUTO_KG", "TOTAL_CANTIDAD_1", "TOTAL_BULTOS",                     
	#					"DESCRIPCION_ARANCEL_PAIS", "DEPTO_ORIGEN", "VIA", "DESC_CIIU",                      
	#					"CLAS_GRANDES_CATEGORIAS_ECONOMICAS", "CLAS_UNIFORME", "ACTIVIDAD_COMERCIAL",
	#					"PRODUCTOS_TRADICIONALES_Y_NO_TRAD")
						
	#bolivia 2017
	#colnames(j) <- c("DATE_YYYY_MM", "DATE_YYYY_MM_DD", "COMPANY",                                      
	#					"CUSTOM", "CONSIGNEE_COMPANY_NAME", "SUPPLIER",                                     
	#					"COUNTRY_OF_DESTINY", "PATRON_EXPO", "PRODUCT_DESCRIPTION_BY_SCHEDULE_B_CODE",        
	#					"PRODUCT_SCHEDULE_B_CODE", "COD_ARMONIZADOPRODUCTO_INGLES", "HARMONIZED_CODE_DESCRIPTION_ENGLISH",          
	#					"AGREEMENT", "REGISTRO", "VALIDACION",                                  
	#					"FECHA_VALIDACION", "CAMBIO", "TOTAL_VALOR_FOB_US",                           
	#					"TOTAL_CIF_VALUE_US", "PRODUCT_DESCRIPTION", "TOTAL_PESO_NETO_KG",                           
	#					"TOTAL_GROSS_WEIGHT_KG", "TOTAL_QUANTITY_1", "TOTAL_BUNDLES",                             
	#					"PRODUCT_DESCRIPTION_BY_SCHEDULE_B_CODE_COUNTRY")
						
	# chile 2015, 2016, 2017
	#colnames(j) <- c("DATE_YYYY_MM", "DATE_YYYY_MM_DD", "COMPANY_ID_NUMBER",                 
	#					"VERIFICATION_DIGIT", "EXPORTER", "COUNTRY_OF_DESTINY",                  
	#					"PRODUCT_SCHEDULE_B_CODE", "PRODUCT_DESCRIPTION_BY_SCHEDULE_B_CODE", "PRODUCT_DESCRIPTION",                 
	#					"HARMONIZED_CODEPRODUCT_ENGLISH", "HARMONIZED_CODE_DESCRIPTION_ENGLISH", "TOTAL_QUANTITY_1",                  
	#					"MEASURE_UNIT_1_QUANTITY_1", "TOTAL_FOB_VALUE_US", "FOB_PER_UNIT_QUANTITY_1",              
	#					"TOTAL_CIF_VALUE_US", "TOTAL_NET_WEIGHT_KG", "TYPE_OF_TRANSPORT",                
	#					"CUSTOM", "PORT_OF_LOADING", "PORT_OF_UNLOADING",                     
	#					"VESSEL_NAME", "FREIGHT", "INSURANCE",                           
	#					"ACCEPTANCE", "TRANSPORT_COMPANY_TRANSPORT_USED", "TYPE_OF_CARGO",                       
	#					"TYPE_OF_BUNDLE", "INCOTERM", "WAY_OF_PAYMENT")
						
	#colnames(j) <- c("DATE_YYYY_MM", "DATE_YYYY_MM_DD", "COMPANY_ID_NUMBER", "VERIFICATION_DIGIT",
	#					"EXPORTER", "COUNTRY_OF_DESTINY", "PRODUCT_SCHEDULE_B_CODE",
	#					"PRODUCT_DESCRIPTION_BY_SCHEDULE_B_CODE", "PRODUCT_DESCRIPTION", 
	#					"HARMONIZED_CODEPRODUCT_ENGLISH", "HARMONIZED_CODE_DESCRIPTION_ENGLISH",
	#					"TOTAL_QUANTITY_1", "MEASURE_UNIT_1_QUANTITY_1", "TOTAL_FOB_VALUE_US",
	#					"FOB_PER_UNIT_QUANTITY_1", "TOTAL_CIF_VALUE_US", "TOTAL_NET_WEIGHT_KG",
	#					"TYPE_OF_TRANSPORT", "CUSTOM", "PORT_OF_LOADING", "PORT_OF_UNLOADING",
	#					"VESSEL_NAME", "FREIGHT", "INSURANCE", "ACCEPTANCE", "TRANSPORT_COMPANY_TRANSPORT_USED",
	#					"TYPE_OF_CARGO", "TYPE_OF_BUNDLE", "INCOTERM", "WAY_OF_PAYMENT")  

	#colnames(j) <- c("NO_DECLARACIN", "FECHA_AAAA_MM", "FECHA_AAAA_MM_DD", "NIT", "DIGITO_CHEQUEO",
	#					"EXPORTADOR", "REPRESENTANTE_LEGAL", "CIUDAD_DOMICILIO", "DEPTO_DOMICILIO",
	#					"TELEFONO", "FAX", "DIRECCIN", "EMAIL", "CLASE", "CLASE_EXPORTACIN", "UAP",                          
	#					"ALTEX", "EMPRESA_IMPORTADOR", "PAIS_DESTINO", "CIUDAD_IMPORTADOR", 
	#					"DIRECCIN_IMPORTADOR", "POSICIN", "DESCRIPCIN_ARANCEL", "COD_ARMONIZADO_PRODUCTO_INGLES",
	#					"DESC_COD_ARMONIZADO_INGLES", "TOTAL_CANTIDAD_1", "UNIDAD_COMERCIAL_1",
	#					"TOTAL_VALOR_FOB_US", "VALOR_FOB_UNITARIO_US_CANTIDAD_1", "TOTAL_VALOR_CIF_US",
	#					"TOTAL_PESO_NETO_KG", "TOTAL_PESO_BRUTO_KG", "VIA", "ADUANA", "DEPTO_ORIGEN",
	#					"DEPTO_PROCENDENCIA", "CIUDAD_SALIDA", "FECHA_EMBARQUE", "ADUANA_EMBARQUE",
	#					"AUTO_EMBARQUE", "FLETE", "SEGURO", "VALOR_GASTOS", "VAL_AGREGADO", "VALOR_PESOS",
	#					"EMBARQUE", "NIT_DECLARANTE", "EMPRESA_DECLARANTE", "FORMA_PAGO", "DATOS_TRANSPORTE",
	#					"EXPO_TRNSITO", "RGIMEN", "ACUERDO", "CIIU", "DESC_CIIU", "CUODE", "DESC_CUODE")
						
	#colnames(j) <- c("IMPORT_DECLARATION_NUMBER", "DATE_YYYY_MM", "DATE_YYYY_MM_DD", "COMPANY_ID_NUMBER",
	#					"COMPANY_CHECK_DIGIT_ID", "EXPORTER", "CONTACT", "CITY", "STATE", "TELEPHONE",
	#					"FAX", "ADDRESS", "EMAIL", "TYPE_OF_COMPANY", "EXPORT_TYPE", "UAP", "ALTEX",
	#					"IMPORTER", "COUNTRY_OF_DESTINY", "IMPORTER_CITY", "IMPORTER_ADDRESS",                      
	#					"PRODUCT_SCHEDULE_B_CODE", "PRODUCT_DESCRIPTION_BY_SCHEDULE_B_CODE",
	#					"HARMONIZED_CODE_PRODUCT_ENGLISH", "HARMONIZED_CODE_DESCRIPTION_ENGLISH",
	#					"TOTAL_QUANTITY_1", "MEASURE_UNIT_1_QUANTITY_1", "TOTAL_FOB_VALUE_US",
	#					"FOB_PER_UNIT_QUANTITY_1", "TOTAL_CIF_VALUE_US", "TOTAL_NET_WEIGHT_KG", 
	#					"TOTAL_GROSS_WEIGHT_KG", "TYPE_OF_TRANSPORT", "CUSTOM", "STATE_OF_ORIGIN", 
	#					"STATE_OF_PROCEDENCE", "CITY_DEPARTURE", "DATE_LOADING", "CUSTOM_LOADING",
	#					"AUTO_LOADING_NUMBER", "FREIGHT", "INSURANCE", "VALUE_COSTS", "ADDED_VALUE", 
	#					"VALUE_COP", "LOADING", "DECLARANT_CO_ID_NUMBER", "DECLARANT_COMPANY", 
	#					"WAY_OF_PAYMENT", "TRANSPORT_INFO", "EXPORT_TRANSIT", "REGIME", "AGREEMENT", 
	#					"ISIC", "ISIC_DESCRIPTION", "CUODE", "CUODE_DESCRIPTION")
	
	
	# venezuela 2013, 2014, 2015, 2016, 2017
	#colnames(j) <- c("DATE_YYYY_MM", "EXPORTER", "COUNTRY_OF_DESTINY",                    
	#					"TYPE_OF_COMPANY", "PRODUCT_SCHEDULE_B_CODE", "PRODUCT_DESCRIPTION_BY_SCHEDULE_B_CODE",
	#					"HARMONIZED_CODE_PRODUCT_SPANISH", "HARMONIZED_CODE_DESCRIPTION_ENGLISH", "TOTAL_FOB_VALUE_US",                   
	#					"TOTAL_FOB_VALUE_BOLIVARES", "TOTAL_NET_WEIGHT_KG", "TOTAL_GROSS_WEIGHT_KG",                 
	#					"TYPE_OF_TRANSPORT", "CUSTOM", "EXCHANGE_RATE",                       
	#					"TRANSPORTER_FLAG", "REGIME", "PORT_OF_LOADING",                      
	#					"REGISTRY_NUMBER", "MANIFEST_NUMBER")
	

	# costarica 2013, 2014, 2015, 2016, 2017
	#colnames(j) <- c("DATE_YYYY_MM_DD", "DATE_YYYY_MM", "COUNTRY_OF_DESTINY", "PRODUCT_SCHEDULE_B_CODE",                          
	#					"PRODUCT_DESCRIPTION_BY_SCHEDULE_B_CODE", "PRODUCT_DESCRIPTION_BY_SCHEDULE_B_CODE_COSTA_RICA",
	#					"HARMONIZED_CODE_PRODUCT_ENGLISH", "HARMONIZED_CODE_DESCRIPTION_ENGLISH",              
	#					"TOTAL_NET_WEIGHT_KG", "REGIME", "TOTAL_CIF_VALUE_US", "IMPORT_DECLARATION_NUMBER",                       
	#					"CUSTOM", "DELIVERY_NUMBER", "MODALITY", "COMPANY_ID_NUMBER", "EXPORTER",
	#					"ADDRESS", "INVOICE_TOTAL_VALUE", "TYPE_OF_TRANSPORT",                             
	#					"RATE", "OBSERVATIONS", "NUMBER_ITEMS", "PRODUCT_DESCRIPTION",                              
	#					"TOTAL_QUANTITY_1", "MEASURE_UNIT_1_QUANTITY_1", "BRAND",
	#					"TOTAL_GROSS_WEIGHT_KG", "CUSTOM_INTERMEDIARY_AGENCY_SIA")
	
	# mexico 2013, 2014, 2015, 2016, 2017
	#colnames(j) <- c("DATE_YYYY_MM", "DATE_YYYY_MM_DD", "COUNTRY_OF_DESTINY",                      
	#					"PRODUCT_SCHEDULE_B_CODE", "PRODUCT_DESCRIPTION_BY_SCHEDULE_B_CODE", 
	#					"PRODUCT_DESCRIPTION_BY_SCHEDULE_B_CODE_COUNTRY", "HARMONIZED_CODE_PRODUCT_ENGLISH",
	#					"HARMONIZED_CODE_DESCRIPTION_ENGLISH", "TOTAL_QUANTITY_1",
	#					"MEASURE_UNIT_1_QUANTITY_1", "TOTAL_FOB_VALUE_US", "FOB_PER_UNIT_QUANTITY_1",                       
	#					"TOTAL_GROSS_WEIGHT_KG", "TYPE_OF_TRANSPORT", "CUSTOM", "EXPORTER",
	#					"ADDRESS", "CITY", "IMPORTER_STATE", "FOREIGN_COMPANY",
	#					"FOREIGN_COMPANY_ADDRESS", "PORT_OF_LOADING") 
						
	# panama 2013, 2014, 2015, 2016, 2017
	#colnames(j) <- c("DATE_YYYY_MM", "DATE_YYYY_MM_DD", "COMPANY_ID_NUMBER", "EXPORTER",
	#					"CONSIGNEE_COMPANY_NAME", "COUNTRY_OF_DESTINY", "PRODUCT_SCHEDULE_B_CODE",
	#					"PRODUCT_DESCRIPTION_BY_SCHEDULE_B_CODE", "PRODUCT_DESCRIPTION",                   
	#					"HARMONIZED_CODE_PRODUCT_ENGLISH", "HARMONIZED_CODE_DESCRIPTION_ENGLISH",
	#					"TOTAL_QUANTITY_1", "MEASURE_UNIT_1_QUANTITY_1", "TOTAL_FOB_VALUE_US",
	#					"FOB_PER_UNIT_QUANTITY_1", "TOTAL_CIF_VALUE_US", "TOTAL_NET_WEIGHT_KG",
	#					"TOTAL_GROSS_WEIGHT_KG", "TYPE_OF_TRANSPORT", "TABPTOPAN", "ZONE",                                  
	#					"FREIGHT", "INSURANCE", "NO_LIQ", "ID_DIGIT_CONTROL", "COMMERCIAL_QUANTITY",
	#					"CALCULATED_TAX", "IMP_SEL_CONSUM", "ICDDP", "IMP_IMPORT", "IMP_TOTAL_PAYMENT") 

	# brazil third party
	colnames(j) <- c("NUM_RE", "NUM_DDE", "DIA_REGIS", "DIA_DESEMB", "DIA_EMBQ", "COD_IMPDR_EXPDR",
						"NOME_IMPDR_EXPDR", "UA_LOCAL_DESBQ_EMBQ", "UF_DESBQ_EMBQ", "VIA_TRANSPORTE",             
						"PAIS_ORIGEM_DESTINO", "COD_SUBITEM_NCM", "SUBITEM_NCM", 
						"NOME_IMPORTADOR_ESTRANGEIRO", "DESCRICAO_PROD_EXP", "UNID_MEDIDA_EST",
						"UNID_COMERC_PROD_EXP", "METRICA", "QTDE_PROD_BAL_EXP", "VMLE_DOLAR_UNID_BAL_EXP",    
						"QTDE_EST_MERC_BAL_EXP", "VMLE_DOLAR_BAL_EXP", "PESO_LIQ_MERC_BAL_EXP")   
	
						
	# add the data to the list
	J[[i]] <- j
	i <- i + 1
}

# append all data, earlier stored in a list of dataframes in J
D <- do.call(rbind, J)


# set correct column names
#colnames(D) <- c('DATE', 'PRODUCT_HS', 'HS_DESCRIPTION', 'COUNTRY_OF_DESTINATION', 
#				'PORT_OF_DEPARTURE', 'FOB_VALUE_USD', 'EXPORTER_NAME',
#				'STATE_DEPARTMENT_OF_THE_EXPORTER', 'EXPORTER_MUNICIPALITY', 'TRANSPORT_METHOD',
#				'NET_WEIGHT', 'EXPORTER_CNPJ')

				
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


# delete duplicates NO!
#D <- D[!duplicated(D), ]

#unique(D$DATE_YYYY_MM)

# write file

### outdated, don't use write.csv2
write.csv2(D, 'CD_BRAZIL_2016.csv', quote = FALSE, row.names = FALSE)

write.table(janjul13, 'CD_PERU_2013.csv', quote = FALSE, row.names = FALSE, dec = '.', sep = ';')



## to clear the R environment
#rm(list = ls())


#nm <- c('DATE_YYYY_MM', 'DATE_YYYY_MM_DD', 'COMPANY_ID_NUMBER', 'EXPORTER', 
#'COUNTRY_OF_DESTINY', 'PRODUCT_SCHEDULE_B_CODE', 'PRODUCT_DESCRIPTION_BY_SCHEDULE_B_CODE', 'PRODUCT_DESCRIPTION_BY_SCHEDULE_B_CODE_IN_URUGUAY', 
#'PRODUCT_DESCRIPTION', 'HARMONIZED_CODEPRODUCT_ENGLISH', 'HARMONIZED_CODE_DESCRIPTION_ENGLISH', 
#'TOTAL_QUANTITY_1', 'MEASURE_UNIT_1_QUANTITY_1', 'TOTAL_FOB_VALUE_US', "FOB_PER_UNIT_QUANTITY_1", 'TOTAL_CIF_VALUE_US', 
#'TOTAL_NET_WEIGHT_KG', 'TOTAL_GROSS_WEIGHT_KG', 'TYPE_OF_TRANSPORT', 'CUSTOM', 'FREIGHT', 'INSURANCE',
#"TRANSPORT_COMPANY_TRANSPORT_USED", 'INCOTERM')
