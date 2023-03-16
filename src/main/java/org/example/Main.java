package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {
    public static void main(String[] args) {
        System.out.println("Hello world!");
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark  basic example")
                .config("spark.master", "local")
                .getOrCreate();

        Dataset<Row> df = spark.read().csv("src/main/resources/Hogwarts.csv");
        df.show();
    }
}
