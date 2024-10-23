# Requeriments
Java 21

# Instructions
To run the repo do
```
./gradlew run
```

# Example Output
```
Read 1503 records from JSON file
Read 1503 records from Parquet file
JSON read duration: 2942 ms
Parquet read duration: 4110 ms
```

# Benchmarks
```
Time to initialise 3151 ms
Time to get object request 22 ms
Time to get response 1287 ms
Time to initialise object mapper 279 ms
Time to read JSON data 699 ms
Read 1503 records from JSON file
Time to read record 1712 ms
Time to read Parquet data 47 ms
Read 1503 records from Parquet file
JSON read duration: 5447 ms
Parquet read duration: 5003 ms
Time to initialise 22 ms
Time to get object request 0 ms
Time to get response 341 ms
Time to initialise object mapper 0 ms
Time to read JSON data 4604 ms
Read 171357 records from JSON file
Time to read record 1016 ms
Time to read Parquet data 782 ms
Read 171357 records from Parquet file
JSON read duration: 4967 ms
Parquet read duration: 1873 ms
Time to initialise 16 ms
Time to get object request 0 ms
Time to get response 686 ms
Time to initialise object mapper 1 ms
Time to read JSON data 6708 ms
Read 171157 records from JSON file
Time to read record 1048 ms
Time to read Parquet data 1053 ms
Read 171157 records from Parquet file
JSON read duration: 7411 ms
Parquet read duration: 2403 ms
Time to initialise 17 ms
Time to get object request 0 ms
Time to get response 282 ms
Time to initialise object mapper 1 ms
Time to read JSON data 141 ms
Read 1503 records from JSON file
Time to read record 267 ms
Time to read Parquet data 8 ms
Read 1503 records from Parquet file
JSON read duration: 441 ms
Parquet read duration: 350 ms
Time to initialise 45 ms
Time to get object request 0 ms
Time to get response 286 ms
Time to initialise object mapper 0 ms
Time to read JSON data 4431 ms
Read 171357 records from JSON file
Time to read record 988 ms
Time to read Parquet data 879 ms
Read 171357 records from Parquet file
JSON read duration: 4762 ms
Parquet read duration: 2175 ms
Time to initialise 12 ms
Time to get object request 0 ms
Time to get response 341 ms
Time to initialise object mapper 0 ms
Time to read JSON data 2237 ms
Read 171157 records from JSON file
Time to read record 625 ms
Time to read Parquet data 668 ms
Read 171157 records from Parquet file
JSON read duration: 2590 ms
Parquet read duration: 1363 ms
```