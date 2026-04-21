library(DBI)
library(RPostgres)
library(readr) # To read your .sql file


sys_con <- dbConnect(RPostgres::Postgres(), 
                     dbname = "postgres", # Default system DB
                     host = "localhost", 
                     user = "postgres", 
                     password = "your_password_here") # 


tryCatch({
  dbExecute(sys_con, "CREATE DATABASE sales_db;")
  print("Database 'sales_db' created successfully.")
}, error = function(e) {
  print("Database 'sales_db' already exists or could not be created.")
})

dbDisconnect(sys_con) # Disconnect from system DB


con <- dbConnect(RPostgres::Postgres(), 
                 dbname = "sales_db", 
                 host = "localhost", 
                 user = "postgres", 
                 password = "ggdata2014") # 


sql_commands <- read_file("database.sql")


statements <- unlist(strsplit(sql_commands, ";"))

# Execute each statement
for (stmt in statements) {
  # clean up whitespace
  stmt_clean <- trimws(stmt)
  if (nchar(stmt_clean) > 0) {
    tryCatch({
      dbExecute(con, stmt_clean)
      print(paste("Executed:", substr(stmt_clean, 1, 50), "..."))
    }, error = function(e) {
      print(paste("Error executing:", substr(stmt_clean, 1, 50)))
      print(e)
    })
  }
}

print("Tables created successfully!")
dbDisconnect(con)