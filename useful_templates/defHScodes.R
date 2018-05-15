## defining commodity HS codes

## link hs codes with comtrade data
## get total volume exported
## brazil argentina indonesia colombia
## for each hs code get volume exported
## calculate perc that each makes up of a commodity
## define threshold


library(data.table)
library(readr)


hs <- read.delim('C:/Users/laura.delduca/Desktop/commodity_equivalents.csv', sep = ';', header = TRUE)

comtrade <- read_csv("C:/Users/laura.delduca/Desktop/COMTRADE/type-C_r-ALL_ps-2016_freq-A_px-HS_pub-20171106_fmt-csv_ex-20171201.csv")


comtrade_brazil <- comtrade[which(comtrade$Reporter == 'Brazil' & comtrade$'Trade Flow' == 'Export'),]
comtrade_argentina <- comtrade[which(comtrade$Reporter == 'Argentina' & comtrade$'Trade Flow' == 'Export'),]
comtrade_indonesia <- comtrade[which(comtrade$Reporter == 'Indonesia' & comtrade$'Trade Flow' == 'Export'),]
comtrade_colombia <- comtrade[which(comtrade$Reporter == 'Colombia' & comtrade$'Trade Flow' == 'Export'),]


hs$code_value <- formatC(hs$code_value, width = 6, flag = 0, format = 'd')


hs$brazil_export_volume <- comtrade_brazil$'Netweight (kg)'[match(hs$code_value, comtrade$'Commodity Code')]
hs$argentina_export_volume <- comtrade_argentina$'Netweight (kg)'[match(hs$code_value, comtrade$'Commodity Code')]
hs$indonesia_export_volume <- comtrade_indonesia$'Netweight (kg)'[match(hs$code_value, comtrade$'Commodity Code')]
hs$colombia_export_volume <- comtrade_colombia$'Netweight (kg)'[match(hs$code_value, comtrade$'Commodity Code')]


hs$total_hs_export_volume <- hs$brazil_export_volume + hs$argentina_export_volume + hs$indonesia_export_volume + hs$colombia_export_volume


hs$brazil_commodity_export_volume <- NA

hs$brazil_commodity_export_volume[hs$prod_name == 'COCOA'] <- sum(hs$brazil_export_volume[hs$prod_name == 'COCOA'], na.rm = TRUE)
hs$brazil_commodity_export_volume[hs$prod_name == 'CORN'] <- sum(hs$brazil_export_volume[hs$prod_name == 'CORN'], na.rm = TRUE)
hs$brazil_commodity_export_volume[hs$prod_name == 'SHRIMPS'] <- sum(hs$brazil_export_volume[hs$prod_name == 'SHRIMPS'], na.rm = TRUE)
hs$brazil_commodity_export_volume[hs$prod_name == 'COTTON'] <- sum(hs$brazil_export_volume[hs$prod_name == 'COTTON'], na.rm = TRUE)
hs$brazil_commodity_export_volume[hs$prod_name == 'PAPER AND PULP'] <- sum(hs$brazil_export_volume[hs$prod_name == 'PAPER AND PULP'], na.rm = TRUE)
hs$brazil_commodity_export_volume[hs$prod_name == 'NATURAL TIMBER'] <- sum(hs$brazil_export_volume[hs$prod_name == 'NATURAL TIMBER'], na.rm = TRUE)
hs$brazil_commodity_export_volume[hs$prod_name == 'LEATHER (CATTLE)'] <- sum(hs$brazil_export_volume[hs$prod_name == 'LEATHER (CATTLE)'], na.rm = TRUE)
hs$brazil_commodity_export_volume[hs$prod_name == 'NATURAL TIMBER'] <- sum(hs$brazil_export_volume[hs$prod_name == 'NATURAL TIMBER'], na.rm = TRUE)
hs$brazil_commodity_export_volume[hs$prod_name == 'CHICKEN'] <- sum(hs$brazil_export_volume[hs$prod_name == 'CHICKEN'], na.rm = TRUE)
hs$brazil_commodity_export_volume[hs$prod_name == 'PORK'] <- sum(hs$brazil_export_volume[hs$prod_name == 'PORK'], na.rm = TRUE)


hs$argentina_commodity_export_volume <- NA

hs$argentina_commodity_export_volume[hs$prod_name == 'COCOA'] <- sum(hs$argentina_export_volume[hs$prod_name == 'COCOA'], na.rm = TRUE)
hs$argentina_commodity_export_volume[hs$prod_name == 'CORN'] <- sum(hs$argentina_export_volume[hs$prod_name == 'CORN'], na.rm = TRUE)
hs$argentina_commodity_export_volume[hs$prod_name == 'SHRIMPS'] <- sum(hs$argentina_export_volume[hs$prod_name == 'SHRIMPS'], na.rm = TRUE)
hs$argentina_commodity_export_volume[hs$prod_name == 'COTTON'] <- sum(hs$argentina_export_volume[hs$prod_name == 'COTTON'], na.rm = TRUE)
hs$argentina_commodity_export_volume[hs$prod_name == 'PAPER AND PULP'] <- sum(hs$argentina_export_volume[hs$prod_name == 'PAPER AND PULP'], na.rm = TRUE)
hs$argentina_commodity_export_volume[hs$prod_name == 'NATURAL TIMBER'] <- sum(hs$argentina_export_volume[hs$prod_name == 'NATURAL TIMBER'], na.rm = TRUE)
hs$argentina_commodity_export_volume[hs$prod_name == 'LEATHER (CATTLE)'] <- sum(hs$argentina_export_volume[hs$prod_name == 'LEATHER (CATTLE)'], na.rm = TRUE)
hs$argentina_commodity_export_volume[hs$prod_name == 'NATURAL TIMBER'] <- sum(hs$argentina_export_volume[hs$prod_name == 'NATURAL TIMBER'], na.rm = TRUE)
hs$argentina_commodity_export_volume[hs$prod_name == 'CHICKEN'] <- sum(hs$argentina_export_volume[hs$prod_name == 'CHICKEN'], na.rm = TRUE)
hs$argentina_commodity_export_volume[hs$prod_name == 'PORK'] <- sum(hs$argentina_export_volume[hs$prod_name == 'PORK'], na.rm = TRUE)


hs$indonesia_commodity_export_volume <- NA

hs$indonesia_commodity_export_volume[hs$prod_name == 'COCOA'] <- sum(hs$indonesia_export_volume[hs$prod_name == 'COCOA'], na.rm = TRUE)
hs$indonesia_commodity_export_volume[hs$prod_name == 'CORN'] <- sum(hs$indonesia_export_volume[hs$prod_name == 'CORN'], na.rm = TRUE)
hs$indonesia_commodity_export_volume[hs$prod_name == 'SHRIMPS'] <- sum(hs$indonesia_export_volume[hs$prod_name == 'SHRIMPS'], na.rm = TRUE)
hs$indonesia_commodity_export_volume[hs$prod_name == 'COTTON'] <- sum(hs$indonesia_export_volume[hs$prod_name == 'COTTON'], na.rm = TRUE)
hs$indonesia_commodity_export_volume[hs$prod_name == 'PAPER AND PULP'] <- sum(hs$indonesia_export_volume[hs$prod_name == 'PAPER AND PULP'], na.rm = TRUE)
hs$indonesia_commodity_export_volume[hs$prod_name == 'NATURAL TIMBER'] <- sum(hs$indonesia_export_volume[hs$prod_name == 'NATURAL TIMBER'], na.rm = TRUE)
hs$indonesia_commodity_export_volume[hs$prod_name == 'LEATHER (CATTLE)'] <- sum(hs$indonesia_export_volume[hs$prod_name == 'LEATHER (CATTLE)'], na.rm = TRUE)
hs$indonesia_commodity_export_volume[hs$prod_name == 'NATURAL TIMBER'] <- sum(hs$indonesia_export_volume[hs$prod_name == 'NATURAL TIMBER'], na.rm = TRUE)
hs$indonesia_commodity_export_volume[hs$prod_name == 'CHICKEN'] <- sum(hs$indonesia_export_volume[hs$prod_name == 'CHICKEN'], na.rm = TRUE)
hs$indonesia_commodity_export_volume[hs$prod_name == 'PORK'] <- sum(hs$indonesia_export_volume[hs$prod_name == 'PORK'], na.rm = TRUE)


hs$colombia_commodity_export_volume <- NA

hs$colombia_commodity_export_volume[hs$prod_name == 'COCOA'] <- sum(hs$colombia_export_volume[hs$prod_name == 'COCOA'], na.rm = TRUE)
hs$colombia_commodity_export_volume[hs$prod_name == 'CORN'] <- sum(hs$colombia_export_volume[hs$prod_name == 'CORN'], na.rm = TRUE)
hs$colombia_commodity_export_volume[hs$prod_name == 'SHRIMPS'] <- sum(hs$colombia_export_volume[hs$prod_name == 'SHRIMPS'], na.rm = TRUE)
hs$colombia_commodity_export_volume[hs$prod_name == 'COTTON'] <- sum(hs$colombia_export_volume[hs$prod_name == 'COTTON'], na.rm = TRUE)
hs$colombia_commodity_export_volume[hs$prod_name == 'PAPER AND PULP'] <- sum(hs$colombia_export_volume[hs$prod_name == 'PAPER AND PULP'], na.rm = TRUE)
hs$colombia_commodity_export_volume[hs$prod_name == 'NATURAL TIMBER'] <- sum(hs$colombia_export_volume[hs$prod_name == 'NATURAL TIMBER'], na.rm = TRUE)
hs$colombia_commodity_export_volume[hs$prod_name == 'LEATHER (CATTLE)'] <- sum(hs$colombia_export_volume[hs$prod_name == 'LEATHER (CATTLE)'], na.rm = TRUE)
hs$colombia_commodity_export_volume[hs$prod_name == 'NATURAL TIMBER'] <- sum(hs$colombia_export_volume[hs$prod_name == 'NATURAL TIMBER'], na.rm = TRUE)
hs$colombia_commodity_export_volume[hs$prod_name == 'CHICKEN'] <- sum(hs$colombia_export_volume[hs$prod_name == 'CHICKEN'], na.rm = TRUE)
hs$colombia_commodity_export_volume[hs$prod_name == 'PORK'] <- sum(hs$colombia_export_volume[hs$prod_name == 'PORK'], na.rm = TRUE)


hs$brazil_perc_export_volume <- hs$brazil_export_volume/hs$brazil_commodity_export_volume
hs$brazil_perc_export_volume <- format(round(as.numeric(hs$brazil_perc_export_volume), 2), nsmall = 2)

hs$argentina_perc_export_volume <- hs$argentina_export_volume/hs$argentina_commodity_export_volume
hs$argentina_perc_export_volume <- format(round(as.numeric(hs$argentina_perc_export_volume), 2), nsmall = 2)

hs$indonesia_perc_export_volume <- hs$indonesia_export_volume/hs$indonesia_commodity_export_volume
hs$indonesia_perc_export_volume <- format(round(as.numeric(hs$indonesia_perc_export_volume), 2), nsmall = 2)

hs$colombia_perc_export_volume <- hs$colombia_export_volume/hs$colombia_commodity_export_volume
hs$colombia_perc_export_volume <- format(round(as.numeric(hs$colombia_perc_export_volume), 2), nsmall = 2)


write.csv2(hs, 'C:/Users/laura.delduca/Desktop/commodity_equivalents_perc.csv', quote=FALSE, row.names=FALSE)


