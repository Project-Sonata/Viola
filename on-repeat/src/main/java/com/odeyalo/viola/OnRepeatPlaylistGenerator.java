package com.odeyalo.viola;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.*;

public final class OnRepeatPlaylistGenerator {
    public static void main(String[] args) {
        final SparkSession spark = SparkSession
                .builder()
                .appName("OnRepeatPlaylistGenerator")
                .master("local[*]")
                .getOrCreate();

        final Dataset<Row> userHistory = spark.read().parquet("on-repeat/history/30/1/user_listening_history.parquet");

        Dataset<Row> dataset = userHistory.groupBy("user_id", "track_id")
                .count();

        WindowSpec window = Window.partitionBy("user_id").orderBy(desc("count"));

        Dataset<Row> top20 = dataset.withColumn("rank", row_number().over(window))
                .filter(col("rank").$less$eq(50));


        top20.write().partitionBy("user_id").json("output.parquet");

        spark.close();
    }
}