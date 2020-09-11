# Hit cocorah's website API for a listing of stations and add entries for anything new found

url <- sprintf("http://data.cocorahs.org/cocorahs/export/exportstations.aspx?State=%s&Format=CSV&country=usa", state)
download.file(url, method = "wget", destfile="stations.csv")

get.stations <- read_csv("stations.csv")

old.stations <- dbReadTable(con, "cocorahs_stations") %>% as_tibble()

new.stations <- setdiff(get.stations, old.stations)

if (nrow(new.stations)>0) {dbWriteTable(con, "cocorahs_stations", new.stations, append=T, row.names=F)}