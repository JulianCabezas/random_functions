
# Function for passing a delimited file to a SQLite database on a local folder
# Very useful for working with very large volumes of data,
# After creating the database you can make sql queries (I use dplyr for this)
# Modified from vnijs's Gist: https://gist.github.com/vnijs/e53a68c957747e82c2e3


library(dplyr)
library(readr)
library(DBI)
library(RSQLite)


read_delim2sqlite <- function(delim_file, delim, sqlite_file, table_name, batch_size = 10000, OS = "Windows") {

  # delim file: Path to the delimited file you want to pass to the SQLite
  # delim: character used to separate fields
  # Path to the sqlite file, it hat to finish with ".sqlite3"
  # table_name: Name of the data insite de SQLite database
  # batch_size: Number of records to read in one pass
  # OS: Operative System you are using, supports Windows and Unix

  ## establish a connection to the database
  condb <- dbConnect(SQLite(), sqlite_file)

  ## get the total number of records in the file
  # in Unix
  if (OS == "Unix") {
    total_records <- system2("wc", args = c("-l", delim_file), stdout = TRUE) %>%
      sub(normalizePath(delim_file), "", .) %>%
      as.integer() %>%
      {
        . - 1
      }
  } else {
    # In windows
    total_records <- system2("powershell", args = c("Get-content", delim_file, "|", "Measure-Object", "–Line"), stdout = TRUE)
    total_records <- as.numeric(gsub("[^0-9]", "", paste(total_records, collapse = ""))) - 1
  }

  message("Total records: ", total_records)

  ## find the number of passes needed based on size of each batch
  passes <- total_records %/% batch_size
  remaining <- total_records %% batch_size

  message("Total Passes to complete: ", passes)

  ## first pass determines header and column types
  dat <- read_delim(delim_file, delim, n_max = batch_size, progress = FALSE) %>% as.data.frame()
  if (nrow(problems(dat)) > 0) print(problems(dat))
  col_names <- colnames(dat)
  col_types <- c(character = "c", numeric = "d", integer = "i", logical = "l", Date = "c") %>%
    .[sapply(dat, class)] %>%
    paste0(collapse = "")

  ## write to database table
  dbWriteTable(condb, table_name, dat, overwrite = TRUE)

  ## multiple passes
  for (p in 2:passes) {
    message("Pass number: ", p, ", Progress:", round(p / passes, 2) * 100, "%")
    read_delim(delim_file, delim,
      col_names = col_names, col_types = col_types,
      skip = (p - 1) * batch_size + 1, n_max = batch_size, progress = FALSE
    ) %>%
      as.data.frame() %>%
      dbWriteTable(condb, table_name, ., append = TRUE)
  }

  if (remaining) {
    read_delim(delim_file, delim,
      col_names = col_names, col_types = col_types,
      skip = p * batch_size + 1, n_max = remaining, progress = FALSE
    ) %>%
      as.data.frame() %>%
      dbWriteTable(condb, table_name, ., append = TRUE)
  }

  ## close the database connection
  dbDisconnect(condb)
}

#--------------------------------------------------
# Example

# ## create data
# 
# # Folder with the data
# fpath <- "C:/Proyects/Bigdata"
# 
# # csv or txt file
# delim_file <- file.path(fpath, "Very_big_csv.csv")
# 
# #name for the table inside the sqlite
# table_name <- "bigdata"
# sqlite_file <- file.path(fpath,"bigdata.sqlite3")
# 
# ## using read_delim
# read_delim2sqlite(delim_file,delim=";", sqlite_file, table_name=table_name, batch_size=500000)
# 
# 
# 
# #Gather some data
# con <- dbConnect(SQLite(), paste0(fpath,"/bigdata.sqlite3"))
# con
# 
# dbListTables(con) #see the name of the database
# 
# datos_db <- tbl(con, "bigdata")
# 
# 
# summarys<-datos_db %>%
#   group_by(Variable_group) %>%
#   summarise(
#     something = n()
#   )
# 
# sum_total<-collect(summarys)
