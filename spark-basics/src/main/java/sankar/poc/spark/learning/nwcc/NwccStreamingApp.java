package com.nokia.fni.wificloud;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;

import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;

public class NwccStreamingApp {
    public static void main(String[] args) throws InterruptedException {
        SparkSession spark = SparkSession.builder().appName("NWCC-STREAMING-APP").master("local[*]").getOrCreate();
        final SparkContext sparkContext = spark.sparkContext();
        final JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(sparkContext);
        final JavaStreamingContext javaStreamingContext = new JavaStreamingContext(javaSparkContext, Durations.seconds(30));
        javaStreamingContext.sparkContext().setLogLevel("WARN");

        //create dstremas
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "10.75.168.248:9092");
        kafkaParams.put("key.deserializer", ByteArrayDeserializer.class);
        kafkaParams.put("value.deserializer", KafkaAvroDeserializer.class);
        kafkaParams.put("group.id", "nwcc-streaming-app");
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", false);
        kafkaParams.put("schema.registry.url", "http://10.75.168.248:8081");
        //kafkaParams.put("specific.avro.reader", true);
        //kafkaParams.put("specific.avro.value.type", NwccPeriodicMetricRecord.class);
        
        JavaInputDStream<ConsumerRecord<Byte, GenericRecord>> records = KafkaUtils.createDirectStream(
            javaStreamingContext,
            LocationStrategies.PreferConsistent(),
            ConsumerStrategies.<Byte, GenericRecord>Subscribe(Collections.singletonList("nwcc_data"), kafkaParams)
        );
        
        StructType sqlSchema = NwccSchema.getNwccSchema();
        
        //to df
        JavaDStream<GenericRecord> valRecs = records.map(ConsumerRecord::value);
        
        valRecs.foreachRDD(rdd -> {
            JavaRDD<Row> rows = rdd.map(gr -> {
                return convertGenericRecordToRow(gr, sqlSchema);
            });
        
            Dataset<Row> ds =  spark.createDataFrame(rows, sqlSchema);
            ds.createOrReplaceTempView("nwcc");
            spark.catalog().listTables().show();

            Dataset<Row> dfRaw = spark.sql(Sql.RAW_DATA_SQL);
            dfRaw.createOrReplaceTempView("nwcc_raw");
            dfRaw.show(100, false);
            //dfRaw.write().mode("append").option("header", true).parquet("C:/Users/sankaraa/work/tmp/spark/output/rawData");

            Dataset<Row> assocDataset = dfRaw.sparkSession().sql(Sql.ASSOCIATED_DEVICE_SQL);
            assocDataset.write().mode("append").option("header", true).csv("C:/Users/sankaraa/work/tmp/spark/output/assocDevice");

            Dataset<Row> accessPointDataset = spark.sql(Sql.ACCESS_POINT_SQL);
            accessPointDataset.write().mode("append").option("header", true).csv("C:/Users/sankaraa/work/tmp/spark/output/accessPoint");

            Dataset<Row> radioDataset = spark.sql(Sql.RADIO_INTERFACE_SQL);
            radioDataset.write().mode("append").option("header", true).csv("C:/Users/sankaraa/work/tmp/spark/output/radioInterface");

            Dataset<Row> meshRouterDataset = spark.sql(Sql.MESH_ROUTER_SQL);
            meshRouterDataset.write().mode("append").option("header", true).csv("C:/Users/sankaraa/work/tmp/spark/output/meshRouter");
        });

        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            javaStreamingContext.stop(true, true);
            javaStreamingContext.close();
        }));
    }

    private static Schema getAvroSchema() {
        try {
            File file = Paths.get("src/main/resources/nwcc_schema_reg_schema.avsc").toFile();
            return new Schema.Parser().parse(file);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static Row convertGenericRecordToRow(GenericRecord genericRecord, StructType schema) {
        List<Object> fieldValues = new ArrayList<>();
        for (StructField field : schema.fields()) {
            Object value = genericRecord.get(field.name());
            DataType fieldType = field.dataType();
            Object convertedValue = convertAvroValueToSparkValue(value, fieldType);
            fieldValues.add(convertedValue);
        }
        return new GenericRowWithSchema(fieldValues.toArray(), schema);
    }

    private static Object convertAvroValueToSparkValue(Object value, DataType fieldType) {
        if (value == null) {
            return null;
        }

        if (value instanceof GenericRecord) {
            return convertGenericRecordToRow((GenericRecord) value, (StructType) fieldType);
        }

        if (value instanceof Utf8) {
            return value.toString();
        }

        if (value instanceof GenericData.Array) {
            List<Object> convertedArray = new ArrayList<>();
            for (Object element : (GenericData.Array) value) {
                convertedArray.add(convertAvroValueToSparkValue(element, ((ArrayType) fieldType).elementType()));
            }
            return convertedArray.toArray();
        }

        if (value instanceof GenericData.Record) {
            throw new IllegalArgumentException("Nested records should be handled by convertGenericRecordToRow");
        }

        return value;
    }
}