/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package org.example;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ImportCustomersWithRentalsAndPayments {

    public static void main(String[] args) {
        // The MarkLogic admin password is assumed to be "admin" per the docker-compose.yml file. This is purely for
        // demonstrational purposes and should never be used in a real application.
        final String markLogicAdminPassword = "admin";

        // Create a vanilla local Spark session.
        SparkSession session = SparkSession.builder()
            .master("local[*]")
            .getOrCreate();

        Map<String, String> jdbcOptions = new HashMap<String, String>() {{
            put("driver", "org.postgresql.Driver");
            put("url", "jdbc:postgresql://localhost/dvdrental");
            put("user", "postgres");
            put("password", "postgres");
        }};

        String query =
            "select c.customer_id, c.last_name, r.rental_id, r.rental_date, p.payment_id, p.amount " +
            "from customer c " +
            "inner join rental r on c.customer_id = r.customer_id " +
            "inner join payment p on r.rental_id = p.rental_id " +
            "where (c.customer_id >= 180 and c.customer_id < 190) ";

        session
            .read()
            // Use Spark's built-in JDBC support to read rows from Postgres.
            .format("jdbc").options(jdbcOptions)
            .option("query", query)
            .load()

            .groupBy("rental_id")
            .agg(
                functions.first("customer_id").alias("customer_id"),
                functions.first("last_name").alias("last_name"),
                functions.collect_list(functions.struct("payment_id","amount")).alias("payments")
            )
            .groupBy("customer_id")
            .agg(
                functions.first("last_name").alias("last_name"),
                functions.collect_list(functions.struct("rental_id","payments")).alias("Rentals")
            )


            // The remaining calls use the MarkLogic Spark connector to write customer rows, with nested rentals and
            // sub-nested payments, to the Documents database in MarkLogic.
            .write()
            .format("marklogic")
            .option("spark.marklogic.client.host", "localhost")
            .option("spark.marklogic.client.port", "8000")
            .option("spark.marklogic.client.username", "admin")
            .option("spark.marklogic.client.password", markLogicAdminPassword)
            .option("spark.marklogic.write.uriTemplate", "/customerWithDoubleNesting/{customer_id}.json")
            .option("spark.marklogic.write.collections", "CustomerRentalPayments")
            .option("spark.marklogic.write.permissions", "rest-reader,read,rest-writer,update")
            .mode(SaveMode.Append)
            .save();
    }
}
