#Process CoCoRaHS station data for today using the API

library(tidyverse)
library(lubridate)
library(RPostgreSQL)

state = readLines(".pgcredentials/cocorahs")[6]
county = readLines(".pgcredentials/cocorahs")[7]

#Filter by station (set to "" to get data for all stations)
station = readLines(".pgcredentials/cocorahs")[8]

now = strftime(now(), format="%m/%d/%Y")

url = sprintf("http://data.cocorahs.org/export/exportreports.aspx?ReportType=Daily&dtf=1&Format=CSV&State=%s&County=%s&ReportDateType=reportdate&Date=%s&TimesInGMT=True", 
              state, county, now)

download.file(url, method = "wget", destfile="data.csv")
get.data <- read_csv("data.csv", 
                     col_types = cols(col_date(), 
                                      col_time(), 
                                      col_character(), 
                                      col_character(), 
                                      col_character(), 
                                      col_double(), 
                                      col_double(), 
                                      col_double(), 
                                      col_double(), 
                                      col_double(), 
                                      col_double(), 
                                      col_double(),
                                      col_character()))

get.data$ObservationTime <- strftime(as_datetime(get.data$ObservationTime), format="%H:%M")
names(get.data) <- str_to_lower(names(get.data))

# Fetch data in database for comparison
host = readLines(".pgcredentials/cocorahs")[1]
port = readLines(".pgcredentials/cocorahs")[2]
base = readLines(".pgcredentials/cocorahs")[3]
user = readLines(".pgcredentials/cocorahs")[4]
pass = readLines(".pgcredentials/cocorahs")[5]

driv = dbDriver("PostgreSQL")

#create connection to database "gisdata"
con = dbConnect(driv, user=user, password=pass,
                host=host, port=port, dbname=base)

if (station != ""){
  station <- str_c(state, county, station, sep="-")
  source("cocorahs_stations.r")
  stations <- dbReadTable(con, "cocorahs_stations") %>% as_tibble()
  if (station %in% stations$StationNumber) {
    get.data <- filter(get.data, stationnumber==station)
  } else {
    print(sprintf("Station %s not found!", station))
  }
}

old.data <- dbReadTable(con, "cocorahs") %>% as_tibble() %>% dplyr::select(-id)

new.data <- setdiff(get.data, old.data)

if (nrow(new.data)>0) {
  dbWriteTable(con, "cocorahs", new.data, append=T, row.names=F)
  print(sprintf("%s new rows inserted.", nrow(new.data))) 
} else {
  print("No new data found.")
}
