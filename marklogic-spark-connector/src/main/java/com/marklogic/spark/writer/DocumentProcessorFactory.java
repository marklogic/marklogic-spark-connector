/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.spark.Context;
import com.marklogic.spark.Options;
import com.marklogic.spark.Util;

import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import java.util.function.Function;

/**
 * This class exists in preparation for two events. First, upgrading to langchain4j 0.36.x will involve requiring Java
 * 17, and we don't yet want to force Flux users to use Java 17. Second, we may want to have marklogic-langchain4j be
 * a separate module that is available as a dependency. This class acts as a bridge, trying to instantiate the
 * langchain4j-specific class. If that's not on the classpath, that's fine - the splitter/embedder options simply won't
 * work.
 * <p>
 * At least for the 2.5.0 connector release, everything is being packaged together as we're sticking with langchain4j
 * 0.35.0 which only requires Java 11.
 */
abstract class DocumentProcessorFactory {

    private static final String FACTORY_CLASS_NAME = "com.marklogic.spark.langchain4j.Langchain4jDocumentProcessorFactory";

    static Function<DocumentWriteOperation, Iterator<DocumentWriteOperation>> buildDocumentProcessor(Context context) {
        // We'll need to remove some of the abstraction here, as we need to determine if the user has configured either
        // the splitter or embedder, both of which depend on langchain4j and thus Java 17.
        boolean shouldSplitOrEmbed = context.getProperties().keySet().stream()
            .anyMatch(key -> key.startsWith(Options.WRITE_SPLITTER_PREFIX) || key.startsWith(Options.WRITE_EMBEDDER_PREFIX));
        if (!shouldSplitOrEmbed) {
            return null;
        }

        try {
            Object factory = Class.forName(FACTORY_CLASS_NAME).getDeclaredConstructor().newInstance();
            Function<Context, Function<DocumentWriteOperation, Iterator<DocumentWriteOperation>>> processorFactory = (Function<Context, Function<DocumentWriteOperation, Iterator<DocumentWriteOperation>>>) factory;
            return processorFactory.apply(context);
        }
        // Catch every checked exception from trying to instantiate the class. Any exception from the factory class
        // itself is expected to be a RuntimeException that should bubble up.
        catch (ClassNotFoundException | InstantiationException | IllegalAccessException | NoSuchMethodException |
               InvocationTargetException ex) {
            if (Util.MAIN_LOGGER.isDebugEnabled()) {
                Util.MAIN_LOGGER.debug(
                    "Unable to instantiate factory class {}; this is expected when the marklogic-langchain4j module is not on the classpath. Cause: {}",
                    FACTORY_CLASS_NAME, ex.getMessage()
                );
            }
            return null;
        }
    }

    private DocumentProcessorFactory() {
    }
}
