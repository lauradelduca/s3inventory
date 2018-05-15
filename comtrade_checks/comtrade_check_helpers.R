## Helpers for comtrade_check.R
## Laura Del Duca

## needs to have library aws.s3 and AWS S3 credentials loaded into R
## needs AWS S3 1-TRADE/CD/Export content loaded into dataframe CD


# print header and first lines of all files of country in countries

for (cc in countries){	
	
	for (f in CD$file[CD$country == cc]){
	
	#	if (grepl('MINTRADE', f)){

			obj <- get_object(object = f, bucket = 'trase-storage')
			data <- read.csv(text = rawToChar(obj), sep = ';', quote = '', row.names = NULL)
			
			print(f)
			print('')
			print(data[1:5,])
			print('')
			print('')
			print('')
			print('')
	#	}
	}
}

for (f in ff){
	data <- fread(f)
	print(f)
	print(data[1:3,])
}
