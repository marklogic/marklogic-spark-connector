/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.classifier;

public interface ClassifierTestUtil {

    String MOCK_RESPONSE_OPTION = "spark.marklogic.testing.mockClassifierResponse";

    static String buildMockResponse(int articleCount) {
        String start = """
            <?xml version="1.0" encoding="UTF-8"?>
            <response>
              <STRUCTUREDDOCUMENT>
              <URL>../tmp/ca002056-e3f6-4c81-8c9f-00ca218330c4/1739460469_43eb</URL>
              <SYSTEM name="HASH" value="2c3bcaf41fbabf8ff2e236c7580893ec"/>
              <META name="Type" value="TEXT (4003)"/>
              <META name="title/document_title" value="/some-uri.xml"/>
              <SYSTEM name="Template" value="default"/>""";

        StringBuilder response = new StringBuilder(start);
        for (int i = 0; i < articleCount; i++) {
            response.append("""
                <ARTICLE>
                 <SYSTEM name="DeterminedLanguage" value="english"/>
                 <SYSTEM name="LanguageGuessed" value="no"/>
                </ARTICLE>""");
        }

        return response.append("</STRUCTUREDDOCUMENT></response>").toString();
    }
}
