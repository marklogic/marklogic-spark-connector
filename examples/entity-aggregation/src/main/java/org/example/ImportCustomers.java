package org.example;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.util.Properties;

public class ImportCustomers {

    public static void main(String[] args) {
        // The MarkLogic admin password is assumed to be "admin" per the docker-compose.yml file. This is purely for
        // demonstrational purposes and should never be used in a real application.
        final String markLogicAdminPassword = "admin";

        // Create a vanilla local Spark session.
        SparkSession session = SparkSession.builder()
            .master("local[*]")
            .getOrCreate();

        // Define the JDBC properties for connecting to Postgres.
        Properties jdbcProperties = new Properties();
        jdbcProperties.setProperty("driver", "org.postgresql.Driver");
        jdbcProperties.setProperty("user", "postgres");
        jdbcProperties.setProperty("password", "postgres");

        // See https://stackoverflow.com/a/39129546 for information on aliased queries with Spark. A table name may be
        // used, but in this example, we want to join customers and rentals together. The query is placed in parentheses
        // with an alias after it; the alias can have any name.
        String query = "(select c.*, r.rental_id, r.rental_date, r.return_date " +
            "from customer c " +
            "inner join public.rental r on c.customer_id = r.customer_id " +
            ") the_query ";

        session
            .read()
            // Use Spark's built-in JDBC support to read rows from Postgres.
            .jdbc("jdbc:postgresql://localhost/dvdrental", query, jdbcProperties)

            // The following is one approach for aggregating and is not meant to be authoritative.
            // See https://sparkbyexamples.com/spark/spark-sql-aggregate-functions/ for a reference of all aggregate
            // functions.

            // Group all the rows by customer_id.
            .groupBy("customer_id")

            // Produce a new Dataset based on aggregating the following columns.
            .agg(
                // Add a few of the basic customer properties. Because these will all be the same for each customer_id
                // grouping, the "first" function is used to grab the first value from each group of rows.
                functions.first("first_name").alias("first_name"),
                functions.first("last_name").alias("last_name"),
                functions.first("email").alias("email"),

                // The following first creates structs based on each set of the 3 rental columns. These are then
                // collected into a list and assigned to a new column named "rentals". This is the key technique for
                // nested related entities into an array within the JSON document written to MarkLogic.
                functions.collect_list(
                    functions.struct("rental_id", "rental_date", "return_date")
                ).alias("rentals")
            )

            // The remainder calls use the MarkLogic Spark connector to write customer rows, with nested rentals, to
            // the Documents database in MarkLogic.
            .write()
            .format("com.marklogic.spark")
            .option("spark.marklogic.client.host", "localhost")
            .option("spark.marklogic.client.port", "8000")
            .option("spark.marklogic.client.username", "admin")
            .option("spark.marklogic.client.password", markLogicAdminPassword)
            .option("spark.marklogic.write.uriTemplate", "/customer/{customer_id}.json")
            .option("spark.marklogic.write.collections", "Customer")
            .option("spark.marklogic.write.permissions", "rest-reader,read,rest-writer,update")
            .mode(SaveMode.Append)
            .save();
    }
}
