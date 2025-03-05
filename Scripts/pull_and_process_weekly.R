library(DBI)
library(odbc)
library(lubridate)
library(glue)
library(readr)
library(zip)

#dsn <- "OAO Cloud DB"

dsn <- "OAO Cloud DB Production"

conn <- dbConnect(odbc(), dsn)

start_date <- floor_date(Sys.Date(), "week", 1) - 8
end_date <- start_date + 6


sql_statement <- glue("SELECT CAMPUS, CAMPUS_SPECIALTY, DEPARTMENT_ID, DEPARTMENT, PROVIDER, PROV_ID, MRN, PATIENT_NAME, APPT_DTTM, APPT_DATE, APPT_STATUS, APPT_TYPE, APPT_DUR, ROOMIN_DTTM AS ROOMIN_DTTM, VISIT_END_DTTM, CHECKIN_DTTM, CHECKOUT_DTTM FROM AMBULATORY_ACCESS WHERE DEPARTMENT like '325 W 15TH ST%' AND APPT_WEEK = TO_DATE('{start_date}', 'YYYY-MM-DD')")
previous_week_data <- dbGetQuery(conn, sql_statement)

save_uncompressed_file_path <- paste0("/SharedDrive/deans/Presidents/HSPI-PM/Operations Analytics and Optimization/Projects/System Operations/Chelsea Data Pull 325 W/Uncompressed/325 WEST DATA ",
                                      start_date, " saved ", format(Sys.time(), "%Y-%m-%d %H.%M"), ".csv")

#Using write.table becasue it allows us to have blanks instead of NA and preserver timestamp format
write.table(previous_week_data, save_uncompressed_file_path,
            na = "",
            row.names = FALSE,
            col.names = TRUE,
            append = FALSE,
            sep = ",")

save_compressed_file_path <- gsub("\\<Uncompressed\\>","Compressed",save_uncompressed_file_path)
save_compressed_file_path <- gsub("\\<csv\\>","zip",save_compressed_file_path)

zip(save_compressed_file_path, save_uncompressed_file_path, include_directories = FALSE, mode = "cherry-pick")

min_date <- format(min(previous_week_data$APPT_DTTM), "%m/%d/%Y")
max_date <- format(max(previous_week_data$APPT_DTTM), "%m/%d/%Y")