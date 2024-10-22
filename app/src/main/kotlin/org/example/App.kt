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
    val jsonKey = "v1/json/site=TECHLAB/year=2024/month=03/day=01/tag=Room_Temperature-valueInteger/timeseries.json"
    val parquetKey = "v1/parquet/site=TECHLAB/year=2024/month=03/day=01/tag=Room_Temperature-valueInteger/part-00000-5f366933-35c1-4d3f-a561-ef039e82d365.c000.snappy.parquet"
    val accessKey = System.getenv("AWS_ACCESS_KEY_ID")
    val secretKey = System.getenv("AWS_SECRET_ACCESS_KEY")
    // Read JSON from S3
    val jsonDuration = measureTimeMillis {
        val jsonData = readJsonFromS3(bucket, jsonKey, accessKey, secretKey)
        println("Read ${jsonData.size} records from JSON file")
    }

    // Read Parquet from S3 using Hadoop S3A
    val parquetDuration = measureTimeMillis {
        val parquetData = readParquetFromS3(bucket, parquetKey)
        println("Read ${parquetData.size} records from Parquet file")
    }
    // Measure read duration for S3 Select
    println("JSON read duration: $jsonDuration ms")
    println("Parquet read duration: $parquetDuration ms")
}

// Function to read JSON from S3
fun readJsonFromS3(bucket: String, key: String, accessKey: String, accessSecret: String): List<DataPoint> {
    val s3 = S3Client.builder()
        .region(software.amazon.awssdk.regions.Region.EU_WEST_1)
        .build()
    val getObjectRequest = GetObjectRequest.builder().bucket(bucket).key(key).build()
    val response = s3.getObject(getObjectRequest)

    // Use Jackson to map JSON to Kotlin data classes
    val objectMapper = jacksonObjectMapper()
    val jsonData: JsonData = objectMapper.readValue(InputStreamReader(response))
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
    var record: GenericRecord? = reader.read()
    while (record != null) {
        records.add(record)
        record = reader.read()
    }
    return records
}

