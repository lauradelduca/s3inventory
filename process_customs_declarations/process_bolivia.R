## improving Bolivia

library(data.table)
library(dplyr)

options(scipen=99999999)

# CD_BOLIVIA_2013.csv

b13csv <- rename(b13csv, "Fecha Validacion"  = "Fecha Validación"  )
b13csv <- rename(b13csv, "Validacion"  = "Validación"  )