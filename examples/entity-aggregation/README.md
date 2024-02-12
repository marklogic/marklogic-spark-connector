This project demonstrates how Spark can be used to solve a common problem encountered when importing data from 
a relational database into MarkLogic. In order to fit a logical entity - such as a person with many addresses - into a 
set of tables, it is necessary to "break apart" the entity - i.e. person data is stored in a "persons" table, and address
data is stored in an "address" table. Treating the entity as a whole always requires joining the rows together, which 
can require large numbers of joins based on the nature of the entity. Additionally, securing access to the logical
entity is far more difficult when the entity is spread across many tables. 

In a document database such as MarkLogic, a logical entity doesn't need to be broken apart. The hierarchical nature of 
a JSON or XML document allows for [composition relationships](https://www.uml-diagrams.org/composition.html) to be
represented via nested data structures. This approach simplifies searching, viewing, updating, and securing the entity. 
Related data can still be stored in other documents when desired, but being able to "reassemble" the entity when importing
the data into MarkLogic is a key feature to leverage these advantages. 

To try out this example, you will need [Docker](https://www.docker.com/get-started/) installed. Docker is used to 
create an instance of MarkLogic, a PostgreSQL (referred to in this document as "Postgres") 
relational database, and [pgadmin](https://www.pgadmin.org/). You will also need Java 8 or Java 11, and if you are already running an 
instance of MarkLogic locally, it is recommended to stop that instance first. Note that the pgadmin instance is included
solely for users wishing to use it to explore the Postgres database; using pgadmin is outside the scope of this 
document. If you do wish to use it, please see the [docker-compose.yml] file in this directory for login information.

## Setting up the Docker containers

To begin, clone this repository if you have not already, open a terminal, and run the following commands:

    cd examples/entity-aggregation
    docker-compose up -d --build

Depending on whether you've downloaded the MarkLogic and Postgres images, this may take a few minutes to complete. 

## Setting up the Postgres database

Go to [this Postgres tutorial](https://www.postgresqltutorial.com/postgresql-getting-started/postgresql-sample-database/)
in your web browser. Scroll down to the section titled "Download the PostgreSQL sample database". Follow the
instructions in that section for downloading the `dvdrental.zip` and extracting it to produce a file named 
`dvdrental.tar` (any zip extraction tool should work, including Java via `java xvf dvdrental.zip`). Copy the 
`dvdrental.tar` file to the `./docker/postgres` directory in this example project. 

The DVD rental dataset is used as a source of relational data that can be imported into MarkLogic. The dataset contains 
sample data about movies, customers, rentals, and other related entities. In this example, customers and their rentals
will be joined together and loaded into MarkLogic as customer documents with their related rentals nested inside.

The DVD rental dataset needs a database to be loaded into in Postgres. Run the following Docker command to create a 
new database named `dvdrental`:

    docker exec -it entity_aggregation-postgres-1 psql -U postgres -c "CREATE DATABASE dvdrental"

This will output "CREATE DATABASE" as an indication that it ran successfully.

Then run the following Docker command to use the Postgres `pg_restore` tool to load the `dvdrental.tar` file into the
`dvdrental` database:

    docker exec -it entity_aggregation-postgres-1 pg_restore -U postgres -d dvdrental /opt/dvdrental.tar

After this completes, the `dvdrental` database will contain 15 tables. You can use the pgadmin application included
in this project's `docker-compose.yml` file to browse the database if you wish. 

## Importing customers from Postgres to MarkLogic

This project contains an example Java program that uses Spark and its aggregation ability to accomplish the following:

1. Retrieve customers from Postgres, joined with rentals.
2. Aggregate the rentals for each customer into a "rentals" column containing structs. 
3. Write each customer row as a new document to the "Documents" database in MarkLogic. 

Each customer document in MarkLogic captures the desired data structure with rentals aggregated in their associated 
customer, as opposed to having separate customer and rental documents. 

Run the Spark program via the following command (the use of Gradle is solely for demonstration purposes as it 
simplifies constructing the necessary classpath for the program; in production, a user would use 
[spark-submit](https://spark.apache.org/docs/latest/submitting-applications.html) or some similar mechanism provided by their Spark environment):

    ./gradlew importCustomers

You can then use [MarkLogic's qconsole application](https://docs.marklogic.com/guide/qconsole/intro) to view the 
customer documents written to the Documents database. As there are 599 customer rows in the Postgres "customers" table,
you will also see 599 customer documents in the Documents database.

As an example, each customer document will have a "rentals" array (the one shown here is abbreviated for demonstration
purposes):

```
{
    "customer_id": 12,
    "first_name": "Nancy",
    "last_name": "Thomas",
    "email": "nancy.thomas@sakilacustomer.org",
    "rentals": [
        {
            "rental_id": 988,
            "rental_date": "2005-05-31T03:08:03.000Z",
            "return_date": "2005-06-07T04:22:03.000Z"
        },
        {
            "rental_id": 1084,
            "rental_date": "2005-05-31T15:10:17.000Z",
            "return_date": "2005-06-01T15:15:17.000Z"
        },
        {
            "rental_id": 1752,
            "rental_date": "2005-06-16T21:02:55.000Z",
            "return_date": "2005-06-23T23:09:55.000Z"
        }
    }
}
```

## How the data is aggregated together

The `./src/main/java/org/example/ImportCustomers.java` file in this project contains the source code for the Spark 
program. Please see this file for inline comments that explain how Spark is used to aggregate rentals into their 
related customers.

Note as well that this example is not intended to be authoritative. Please see 
[the Spark programming guide](https://spark.apache.org/docs/latest/sql-programming-guide.html) for complete information 
on writing Spark programs. You may also find
[this reference on Spark aggregate functions](https://sparkbyexamples.com/spark/spark-sql-aggregate-functions/) helpful.

## Importing customers, rentals and payments from Postgres to MarkLogic

This project also contains an example of nested joins being aggregated. In this example, the query includes a join with
rentals (as above), but with the addition that payments are joined with rentals, producing a nested join in the query.
Then Spark aggregation functions are used to perform nested aggregations, resulting in customer documents with JSON
similar to the following snippet.

```
{
  "customer_id": 182,
  "last_name": "Lane",
  "Rentals": [
    {
      "rental_id": 1542,
      "payments": [
        {
          "payment_id": 19199,
          "amount": 3.99
        }
      ]
    },
...
    {
      "rental_id": 4591,
      "payments": [
        {
          "payment_id": 19518,
          "amount": 1.99
        },
        {
          "payment_id": 25162,
          "amount": 1.99
        },
        {
          "payment_id": 29163,
          "amount": 0.99
        },
        {
          "payment_id": 31069,
          "amount": 3.99
        },
        {
          "payment_id": 31834,
          "amount": 3.99
        }
      ]
    },
...
}
```

To try this out, there is another Gradle command similar to the first:

    ./gradlew importCustomersWithRentalsAndPayments

You can then use [MarkLogic's qconsole application](https://docs.marklogic.com/guide/qconsole/intro) to view the
customer documents written to the Documents database. In this example, 10 customer rows are queried from the Postgres
"customers" table, so you will see 10 customer documents in the Documents database.

The JSON snippet above is from the document for customer 182. One of that customer's rentals, 4591, has multiple
payments. If you examine the document for that customer, (/customerWithDoubleNesting/182.json) you will be able to
verify the nested aggregation.
