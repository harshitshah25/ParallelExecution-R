library(RJSONIO)
library(dplyr)
library(rlist)
library(chron)

data.folder <- "~/loans/"
loans.file <- list.files(data.folder, pattern="*.json")


filename <- paste(data.folder,
                  loans.file,
                  sep="")

# Function which flattens the list
dfrow.from.list = function(aList) { 
  data.frame(rbind(unlist(aList)),
             stringsAsFactors=FALSE)
}

# Function to convert json into dataframe
readJSONFileIntoDataFrame <-
  function (filename) { 
    if(isValidJSON(filename)){
      paste(data.folder, 
            filename,
            sep="") %>%
        fromJSON() %>%
        { .$loans } %>% 
        list.select(name, country_code = location$country_code, 
                    country_name = location$country, 
                    activity, sector,
                    funded_amount, paid_amount, 
                    loan_amount, gender = borrowers$gender, 
                    funded_date, paid_date, status) %>%
        lapply(dfrow.from.list) %>%
        bind_rows()
    }
  }

#Load the parallel  library
library(parallel)

#Function to run the whole process in parallel
create.loans.df.mc = function(loans.file.in) {
  loans.file.in %>%
  { mclapply(X=., 
             FUN=readJSONFileIntoDataFrame,
             mc.cores=4) # number of cores to use
  } %>%
    bind_rows()
}

#Function to time the execution
system.time({
  loans.df = create.loans.df.mc(loans.file)
})

str(loans.df)
summary(loans.df)
#Files 669, 1289, 1502, 1508, 1545, 1549, 1608, 1648, 1660,
#1674, 1716, 1781, 1833, 1865, 1866 are corrupt

#Make clusters. Another approach for parallel execution
cl <- makeCluster(4) # number of cores to use
stopCluster(cl) # Stop cluster when finished
clusterExport(cl,
              c('dfrow.from.list','data.folder','%>%',
                'list.select','bind_rows','fromJSON',
                'readJSONFileIntoDataFrame'))

create.loans.df.cl = function(cl, loans.file.in) {
  loans.file.in %>% # loans.file.in = loans.file[1:3]
  { parLapply(cl, ., readJSONFileIntoDataFrame) } %>%
    bind_rows()
}

system.time({
  loans.df.1 = create.loans.df.cl(cl, loans.file)
})

loans.df$funded_year <- substr(loans.df$funded_date,1,4)
str(loans.df)

## Load World Indicators ##############################################################################

library(WDI)

# Add series codes to the `indicator.codes` variable
indicator.codes.final = c("AG.LND.IRIG.AG.ZS",
                          "AG.LND.AGRI.ZS",
                          "AG.PRD.FOOD.XD",
                          "AG.LND.TOTL.K2",
                          "AG.LND.CROP.ZS",
                          "AG.LND.ARBL.ZS",
                          "AG.LND.CREL.HA",
                          "TM.VAL.FOOD.ZS.UN",
                          "SP.POP.TOTL")

# Read the WDI data for these codes into the `df` dataframe
wdi.df <- WDI(indicator=indicator.codes.final, start = 1991, end = 2015,
              extra=TRUE # returns additional variables, see documentation
)

## Data Preprocessing ###################################################################################

#Deriving `funded_year` from `funded_date`
loans.df$funded_year <- substr(loans.df$funded_date,1,4)

#Then we convert those variables to factors using which we will be merginf the dataframes.
loans.df$funded_year <- factor(loans.df$funded_year)
loans.df$country_name <- factor(loans.df$country_name)
wdi.df$year <- factor(wdi.df$year)
wdi.df$country <- factor(wdi.df$country)

#Finally we merge the dataframes using the `merge()` function.
df.all <- merge(loans.df, wdi.df, 
                by.x = c("country_name","funded_year"), 
                by.y = c("country","year"))

#Converting variables to factors and numbers
nms <- c("country_code","activity","sector","status", "funded_year")
df.all[nms] <- lapply(df.all[nms], factor)

#Then we convert the relevant variables to numeric datatype.
df.all$funded_amount <- as.numeric(df.all$funded_amount)
df.all$loan_amount <- as.numeric(df.all$loan_amount)
df.all$paid_amount <- as.numeric(df.all$paid_amount)

#Renaming variables for better understanding
names(df.all) <- c("country_name","funded_year", "name", "country_code",     
                   "activity", "sector", "funded_amount", "loan_amount", "status",
                   "paid_amount", "funded_date", "paid_date", "iso2c", "AgriIrrigatedLand%",
                   "AgriculturalLand%", "FoodProdIndex", "LandArea.SQKM", "PermanentCropland%",
                   "ArableLand%", "LandUnderCerealProd.Hectares", "FoodImports", "TotalPopulation","iso3c", "region",
                   "capital", "longitude", "latitude", "income", "lending")

