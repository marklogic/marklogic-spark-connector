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
package com.marklogic.spark.reader;

import com.marklogic.spark.Options;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PushDownCountTest extends AbstractPushDownTest {

    @Test
    void count() {
        long count = newDefaultReader()
            .option(Options.READ_NUM_PARTITIONS, 2)
            .option(Options.READ_BATCH_SIZE, 1000)
            .load()
            .count();

        assertEquals(15, count, "Expecting all 15 authors to be counted");
        assertEquals(1, countOfRowsReadFromMarkLogic, "When count() is used, only one call should be made to " +
            "MarkLogic, regardless of the number of partitions and the batch size. The connector is expected to both " +
            "modify the inferred schema so that a schema with just one column - 'Count' - is used. And it is also " +
            "expected to modify the plan analysis so that a single bucket is used. That is based on the assumption " +
            "that regardless of the number of matching rows, MarkLogic can efficiently determine a count in a single " +
            "request.");
    }
}
