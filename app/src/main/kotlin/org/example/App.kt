package org.example

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import java.io.InputStreamReader
import kotlin.system.measureTimeMillis

data class DataPoint(
    val timestamp: String,
    val offsetinnanos: Long?,
    val value: String,
    val quality: String
)

data class JsonData(
    val data: List<DataPoint>
)


fun main() {
    // AWS S3 bucket and keys
    val bucket = "dev-eu-west-1-enriched-sensor-data"
    // 10kb Parquet file and 100kb JSON file
    val jsonKey = "v1/json/site=TECHLAB/year=2024/month=03/day=01/tag=Room_Temperature-valueInteger/timeseries.json"
    val parquetKey = "v1/parquet/site=TECHLAB/year=2024/month=03/day=01/tag=Room_Temperature-valueInteger/part-00000-5f366933-35c1-4d3f-a561-ef039e82d365.c000.snappy.parquet"
    // >1Mb Parquet file and 15Mb Json file
    val jsonKey1 = "v1/json/site=TECHLAB/year=2024/month=09/day=06/tag=int_AI2-valueInteger/timeseries.json"
    val parquetKey1 = "v1/parquet/site=TECHLAB/year=2024/month=09/day=06/tag=int_AI2-valueInteger/part-00001-7b1c8899-4630-4176-bad2-cae3215fb2c7.c000.snappy.parquet"
    // >1Mb Parquet file and 17Mb Json file
    val parquetKey2 = "v1/parquet/site=TECHLAB/year=2024/month=10/day=21/tag=real_AI2_scaled-value/part-00000-5c1f42e5-89b7-412a-a3a2-3ff9faa4ff35.c000.snappy.parquet"
    val jsonKey2 = "v1/json/site=TECHLAB/year=2024/month=10/day=21/tag=real_AI2_scaled-value/timeseries.json"
    // Get BenchMarks
    getBenchmarks(bucket, jsonKey, parquetKey)
    getBenchmarks(bucket, jsonKey1, parquetKey1)
    getBenchmarks(bucket, jsonKey2, parquetKey2)
    getBenchmarks(bucket, jsonKey, parquetKey)
    getBenchmarks(bucket, jsonKey1, parquetKey1)
    getBenchmarks(bucket, jsonKey2, parquetKey2)


}

fun getBenchmarks(bucketName: String, jsonKey: String, parquetKey: String){
    // Read JSON from S3
    val jsonDuration = measureTimeMillis {
        val jsonData = readJsonFromS3(bucketName, jsonKey)
        println("Read ${jsonData.size} records from JSON file")
    }

    // Read Parquet from S3 using Hadoop S3A
    val parquetDuration = measureTimeMillis {
        val parquetData = readParquetFromS3(bucketName, parquetKey)
        println("Read ${parquetData.size} records from Parquet file")
    }

    // Measure read duration for S3 Select
    println("JSON read duration: $jsonDuration ms")
    println("Parquet read duration: $parquetDuration ms")

}

// Function to read JSON from S3
fun readJsonFromS3(bucket: String, key: String): List<DataPoint> {
    val beforeClient = System.currentTimeMillis()
    val s3 = S3Client.builder()
        .region(software.amazon.awssdk.regions.Region.EU_WEST_1)
        .build()
    val afterClient = System.currentTimeMillis()

    val beforeObjectRequest = System.currentTimeMillis()
    val getObjectRequest = GetObjectRequest.builder().bucket(bucket).key(key).build()
    val aftterObjectRequest = System.currentTimeMillis()

    val beforeResponse = System.currentTimeMillis()
    val response = s3.getObject(getObjectRequest)
    val afterResponse = System.currentTimeMillis()

    // Use Jackson to map JSON to Kotlin data classes
    val beforeObjectMapper = System.currentTimeMillis()
    val objectMapper = jacksonObjectMapper()
    val afterObjectMapper = System.currentTimeMillis()

    val beforeJsonData = System.currentTimeMillis()
    val jsonData: JsonData = objectMapper.readValue(InputStreamReader(response))
    val afterJsonData = System.currentTimeMillis()

    println("Time to initialise ${afterClient - beforeClient} ms")
    println("Time to get object request ${aftterObjectRequest - beforeObjectRequest} ms")
    println("Time to get response ${afterResponse - beforeResponse} ms")
    println("Time to initialise object mapper ${afterObjectMapper - beforeObjectMapper} ms")
    println("Time to read JSON data ${afterJsonData - beforeJsonData} ms")
    return jsonData.data
}

// Function to read Parquet from S3 using Hadoop S3A
fun readParquetFromS3(bucket: String, key: String): MutableList<GenericRecord> {
    val path = Path("s3a://$bucket/$key")

    val conf = Configuration().apply {
        set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        setBoolean("fs.s3a.path.style.access", true)
        setBoolean(org.apache.parquet.avro.AvroReadSupport.READ_INT96_AS_FIXED, true)
    }

    val inputFile = HadoopInputFile.fromPath(path, conf)
    val reader: ParquetReader<GenericRecord> = AvroParquetReader.builder<GenericRecord>(inputFile).build()
    val records = mutableListOf<GenericRecord>()

    val beforeRecordRead = System.currentTimeMillis()
    var record: GenericRecord? = reader.read()
    val afterRecordRead = System.currentTimeMillis()

    val beforeRead = System.currentTimeMillis()
    while (record != null) {
        records.add(record)
        record = reader.read()
    }
    val parquetAfterRead = System.currentTimeMillis()

    println("Time to read record ${afterRecordRead - beforeRecordRead} ms")
    println("Time to read Parquet data ${parquetAfterRead - beforeRead} ms")
    return records
}

