## Load codes needed for 2018 level 1 release
## Laura Del Duca

## needs to have library aws.s3 and AWS S3 credentials loaded into R
## needs AWS S3 1-TRADE/CD/Export content loaded into dataframe CD
## needs HS codes laoded from get_hs_codes.R


todownload <- data.frame(file = CD$file, release = '')
todownload$release <- as.character(todownload$release)

for (f in as.vector(todownload$file)){

	release <- as.vector( strsplit(as.character(CD$release[CD$file == f][1]), ', ') )
	hs6_release <- as.vector(as.numeric(hs$code_value[ hs$com_name %in% release[[1]] ]))
	hs6_release <- as.character(formatC(hs6_release, width = 6, format = "d", flag = "0"))
	hs2_release <- sort( unique( substr(hs6_release, 1, 2) ) )
	
	todownload$release[todownload$file == f] <- paste(hs2_release, collapse=", ")

}