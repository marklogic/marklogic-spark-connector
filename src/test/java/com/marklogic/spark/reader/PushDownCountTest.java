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
            .load()
            .count();

        assertEquals(15, count, "Expecting all 15 authors to be counted");
        assertEquals(1, countOfRowsReadFromMarkLogic);
    }

    @Test
    void noRowsFound() {
        long count = newDefaultReader()
            .option(Options.READ_OPTIC_QUERY, NO_AUTHORS_QUERY)
            .load()
            .count();

        assertEquals(0, count);
        assertEquals(0, countOfRowsReadFromMarkLogic, "When no rows exist, neither the count() operation nor the " +
            "pruneColumns() operation should be pushed down since there's no optimization to be done.");
    }
}
