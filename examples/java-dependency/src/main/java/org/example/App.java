package org.example;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class App {

    public static void main(String[] args) {
        SparkSession session = SparkSession.builder()
            .master("local[*]")
            .getOrCreate();

        try {
            List<Row> rows = session
                .read()
                .format("com.marklogic.spark")
                .option("spark.marklogic.client.uri", "spark-example-user:password@localhost:8020")
                .option("spark.marklogic.read.opticQuery", "op.fromView('example', 'employee', '')")
                .load()
                .filter("City == 'San Diego'")
                .collectAsList();

            rows.forEach(row -> System.out.println(row.prettyJson()));
            System.out.println("Row count: " + rows.size());
        } finally {
            session.close();
        }
    }
}
