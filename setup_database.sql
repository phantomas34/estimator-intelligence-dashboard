library(DBI)
library(RPostgres)
library(readr) # To read your .sql file

# 1. Connect to the 'default' postgres database to create your specific App DB
# (You usually can't create a database while connected to it, so we connect to 'postgres' first)
sys_con <- dbConnect(RPostgres::Postgres(), 
                     dbname = "postgres", # Default system DB
                     host = "localhost", 
                     user = "postgres", 
                     password = "your_password_here") # <--- UPDATE THIS

# 2. Create the new database (if it doesn't exist)
# We wrap in tryCatch because if it exists, it throws an error.
tryCatch({
  dbExecute(sys_con, "CREATE DATABASE sales_db;")
  print("Database 'sales_db' created successfully.")
}, error = function(e) {
  print("Database 'sales_db' already exists or could not be created.")
})

dbDisconnect(sys_con) # Disconnect from system DB

# 3. Connect to YOUR new 'sales_db'
con <- dbConnect(RPostgres::Postgres(), 
                 dbname = "sales_db", 
                 host = "localhost", 
                 user = "postgres", 
                 password = "ggdata2014") # <--- UPDATE THIS

# 4. Read your SQL file and execute it
# This assumes 'database.sql' is in your R Project root folder
sql_commands <- read_file("database.sql")

# RPostgres sometimes struggles with multiple SQL commands in one string.
# We split them by the semicolon to run them one by one.
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