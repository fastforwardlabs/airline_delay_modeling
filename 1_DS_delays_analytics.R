install.packages("geosphere")

#NOTE: In CDP find the HMS warehouse directory and external table directory by browsing to:
# Environment -> <env name> ->  Data Lake Cluster -> Cloud Storage
# copy and paste the external location to the config setting below.

#Temporary workaround for MLX-975
#In utils/hive-site.xml edit hive.metastore.warehouse.dir and hive.metastore.warehouse.external.dir based on settings in CDP Data Lake -> Cloud Storage
if(!file.exists('/etc/hadoop/conf/hive-site.xml')){
  file.copy('/home/cdsw/utils/hive-site.xml', '/etc/hadoop/conf/hive-site.xml')
} 
  
### Load libraries
library(ggplot2)
library(maps)
library(geosphere)
library (DBI)
library(sparklyr)
library(dplyr)

## Connect to Spark. Check spark_defaults.conf for the correct 
spark_home_set("/etc/spark/")

config <- spark_config()
config$spark.executor.memory <- "16g"
config$spark.executor.cores <- "4"
config$spark.driver.memory <- "6g"
config$spark.executor.instances <- "5"
config$spark.dynamicAllocation.enabled  <- "false"
config$spark.yarn.access.hadoopFileSystems <- "s3a://ml-field/demo/flight-analysis/"

sc <- spark_connect(master = "yarn-client", config=config)

## Read in the flight data from S3

src_databases(sc)
tbl_change_db(sc, 'default')
flights <- tbl(sc, 'flights')

# Plot number of flights per year

flights <- sdf_sample(flights, fraction = .00004, replacement = TRUE, seed = NULL) %>% collect()
flight_counts_by_year <-
  flights %>% 
  group_by(Year) %>% 
  summarise(count = n()) 

g <- ggplot(flight_counts_by_year, aes(x = Year, y = count))
g <- g + geom_line(colour = "magenta",
                   linetype = 1,
                   size = 0.8)
g <- g + xlab("Year")
g <- g + ylab("Flight number")
g <- g + ggtitle("US flights")
plot(g)


# #See flight number between 2010 and 2013
#Next, let’s dig it for the 2002 data. Let’s plot flight number betwewen 2001 and 2003.

#flight_counts_by_month <- flights %>% filter(Year >= 2010 & Year <= 2013) %>% group_by(Year, Month) %>% summarise(count = n())
flight_counts_by_month <- flights %>% group_by(Year, Month) %>% summarise(count = n())

g <- ggplot(flight_counts_by_month,
            aes(x = as.Date(
              sprintf(
                "%d-%02d-01",
                flight_counts_by_month$Year,
                flight_counts_by_month$Month
              )
            ), y = count))
g <- g + geom_line(colour = "magenta",
                   linetype = 1,
                   size = 0.8)
g <- g + xlab("Year/Month")
g <- g + ylab("Flight number")
g <- g + ggtitle("US flights")
plot(g)

# Next, we will summarize the data by carrier, origin and dest.

flights_by_carrier <-
  flights %>% 
  group_by(Year, UniqueCarrier, Origin, Dest) %>% 
  summarise(count = n()) 

flights

#Now we extract AA’s flight.

flights_aa <- flights %>% filter(UniqueCarrier == "AA") 
#%>% arrange(count)
  
flights_aa

#Let’s plot the flight number of AA in 2007.

# draw map with line of AA
xlim <- c(-171.738281,-56.601563)
ylim <- c(12.039321, 71.856229)

# Color settings
pal <- colorRampPalette(c("#333333", "white", "#1292db"))
colors <- pal(100)

map(
  "world",
  col = "#6B6363",
  fill = TRUE,
  bg = "#000000",
  lwd = 0.05,
  xlim = xlim,
  ylim = ylim
)

airports <- tbl(sc, 'airports') %>% collect

maxcnt <- nrow(flights_aa)
for (j in 1:length(flights_aa$UniqueCarrier)) {
  air1 <- airports[airports$iata == flights_aa[j, ]$Origin, ]
  air2 <- airports[airports$iata == flights_aa[j, ]$Dest, ]
  
  inter <-
    gcIntermediate(
      c(air1[1, ]$long, air1[1, ]$lat),
      c(air2[1, ]$long, air2[1, ]$lat),
      n = 100,
      addStartEnd = TRUE
    )
  colindex <-
    round((flights_aa[j, ]$count / maxcnt) * length(colors))
  
  lines(inter, col = colors[colindex], lwd = 0.8)
}

