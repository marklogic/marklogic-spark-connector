/*
 * Copyright 2023 MarkLogic Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.marklogic.spark.reader.MarkLogicReadDataSource;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertTrue;


public class MarkLogicSparkReadDriver {
    static Set<String> hashSet = new HashSet<>();
    public static void main(String args[]) {

        SparkSession sparkSession = SparkSession.builder()
                .master("local")
                .getOrCreate();
        try {
            readInput(sparkSession);
        } catch(Exception e) {
            e.printStackTrace();
        }

    }
    private static void readInput(SparkSession sparkSession) {
        StructType struct = new StructType()
                .add("docNum", DataTypes.IntegerType)
                .add("docName", DataTypes.StringType);
        Dataset<Row> reader = sparkSession.read()
                .schema(struct)
                .format(MarkLogicReadDataSource.class.getName())
            .option("host", "localhost")
            .option("port", 8012)
            .option("user", "admin")
            .option("password","admin")
            .option("plan", "op.fromView(\"Medical\",\"Authors\");")
            // loads till MarkLogicReader
            .load();

        // Needed to navigate from MarkLogicReader to MarkLogicPartitionReader
      //  reader.show();
        reader.foreach((ForeachFunction<Row>) row -> {
            hashSet.add((row.get(1).toString()));
        });
        assertTrue(hashSet.size()==15);
    }
}
