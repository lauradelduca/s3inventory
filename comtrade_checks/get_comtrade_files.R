## Load preprocessed COMTRADE files from AWS S3 for comtrade_check.R
## Laura Del Duca



obj <- get_object(object = 'data/1-TRADE/STATISTICAL_DATA/GLOBAL/COMTRADE/COMTRADE_ZOOM/COMTRADE_2005_zoom.csv', bucket = 'trase-storage')
comtrade05 <- read.csv(text = rawToChar(obj), quote = '', sep = ';')
obj <- get_object(object = 'data/1-TRADE/STATISTICAL_DATA/GLOBAL/COMTRADE/COMTRADE_ZOOM/COMTRADE_2006_zoom.csv', bucket = 'trase-storage')
comtrade06 <- read.csv(text = rawToChar(obj), quote = '', sep = ';')
obj <- get_object(object = 'data/1-TRADE/STATISTICAL_DATA/GLOBAL/COMTRADE/COMTRADE_ZOOM/COMTRADE_2007_zoom.csv', bucket = 'trase-storage')
comtrade07 <- read.csv(text = rawToChar(obj), quote = '', sep = ';')
obj <- get_object(object = 'data/1-TRADE/STATISTICAL_DATA/GLOBAL/COMTRADE/COMTRADE_ZOOM/COMTRADE_2008_zoom.csv', bucket = 'trase-storage')
comtrade08 <- read.csv(text = rawToChar(obj), quote = '', sep = ';')
obj <- get_object(object = 'data/1-TRADE/STATISTICAL_DATA/GLOBAL/COMTRADE/COMTRADE_ZOOM/COMTRADE_2009_zoom.csv', bucket = 'trase-storage')
comtrade09 <- read.csv(text = rawToChar(obj), quote = '', sep = ';')
obj <- get_object(object = 'data/1-TRADE/STATISTICAL_DATA/GLOBAL/COMTRADE/COMTRADE_ZOOM/COMTRADE_2010_zoom.csv', bucket = 'trase-storage')
comtrade10 <- read.csv(text = rawToChar(obj), quote = '', sep = ';')
obj <- get_object(object = 'data/1-TRADE/STATISTICAL_DATA/GLOBAL/COMTRADE/COMTRADE_ZOOM/COMTRADE_2011_zoom.csv', bucket = 'trase-storage')
comtrade11 <- read.csv(text = rawToChar(obj), quote = '', sep = ';')
obj <- get_object(object = 'data/1-TRADE/STATISTICAL_DATA/GLOBAL/COMTRADE/COMTRADE_ZOOM/COMTRADE_2012_zoom.csv', bucket = 'trase-storage')
comtrade12 <- read.csv(text = rawToChar(obj), quote = '', sep = ';')
obj <- get_object(object = 'data/1-TRADE/STATISTICAL_DATA/GLOBAL/COMTRADE/COMTRADE_ZOOM/COMTRADE_2013_zoom.csv', bucket = 'trase-storage')
comtrade13 <- read.csv(text = rawToChar(obj), quote = '', sep = ';')
obj <- get_object(object = 'data/1-TRADE/STATISTICAL_DATA/GLOBAL/COMTRADE/COMTRADE_ZOOM/COMTRADE_2014_zoom.csv', bucket = 'trase-storage')
comtrade14 <- read.csv(text = rawToChar(obj), quote = '', sep = ';')
obj <- get_object(object = 'data/1-TRADE/STATISTICAL_DATA/GLOBAL/COMTRADE/COMTRADE_ZOOM/COMTRADE_2015_zoom.csv', bucket = 'trase-storage')
comtrade15 <- read.csv(text = rawToChar(obj), quote = '', sep = ';')
obj <- get_object(object = 'data/1-TRADE/STATISTICAL_DATA/GLOBAL/COMTRADE/COMTRADE_ZOOM/COMTRADE_2016_zoom.csv', bucket = 'trase-storage')
comtrade16 <- read.csv(text = rawToChar(obj), quote = '', sep = ';')
