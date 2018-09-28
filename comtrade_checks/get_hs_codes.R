## Load HS codes from commodity_dictionary from AWS S3 for comtrade_check.R
## Laura Del Duca

## needs to have library aws.s3 and AWS S3 credentials loaded into R


obj <- get_object(object = 'data/1-TRADE/commodity_equivalents_final.csv', bucket = 'trase-storage')

hs <- read.csv(text = rawToChar(obj), sep = ';', quote = '',
				colClasses = c("character", "character", "character", 
				"character", "character", "numeric", "character", 
				"character"))

hs6 <- as.vector(as.numeric(hs$code_value))

beef <- as.vector(as.numeric(sort(unique(hs$code_value[hs$com_name == 'BEEF']))))
chicken <- as.vector(as.numeric(sort(unique(hs$code_value[hs$com_name == 'CHICKEN']))))
corn <- as.vector(as.numeric(sort(unique(hs$code_value[hs$com_name == 'CORN']))))
cotton <- as.vector(as.numeric(sort(unique(hs$code_value[hs$com_name == 'COTTON']))))
leather <- as.vector(as.numeric(sort(unique(hs$code_value[hs$com_name == 'LEATHER']))))
pork <- as.vector(as.numeric(sort(unique(hs$code_value[hs$com_name == 'PORK']))))
timber <- as.vector(as.numeric(sort(unique(hs$code_value[hs$com_name == 'TIMBER']))))
woodpulp <- as.vector(as.numeric(sort(unique(hs$code_value[hs$com_name == 'WOOD PULP']))))
shrimps <- as.vector(as.numeric(sort(unique(hs$code_value[hs$com_name == 'SHRIMPS']))))
soy <- as.vector(as.numeric(sort(unique(hs$code_value[hs$com_name == 'SOYBEANS']))))
sugarcane <- as.vector(as.numeric(sort(unique(hs$code_value[hs$com_name == 'SUGAR CANE']))))