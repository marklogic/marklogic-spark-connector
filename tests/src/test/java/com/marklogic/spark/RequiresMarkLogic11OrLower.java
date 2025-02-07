/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark;

import com.marklogic.junit5.MarkLogicVersion;
import com.marklogic.junit5.VersionExecutionCondition;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;

public class RequiresMarkLogic11OrLower extends VersionExecutionCondition {

    @Override
    protected ConditionEvaluationResult evaluateVersion(MarkLogicVersion markLogicVersion) {
        return markLogicVersion.getMajor() <= 11 ?
            ConditionEvaluationResult.enabled("MarkLogic is 11 or lower.") :
            ConditionEvaluationResult.disabled("MarkLogic is 12 or higher.");
    }
}
