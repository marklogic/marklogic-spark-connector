xquery version "1.0-ml";

module namespace transform = "http://marklogic.com/rest-api/transform/toText";

(:
Demonstrates how a transform can be used to store JSON or XML as a text document. This is useful for a use case of
a document with a URI that ends in e.g. ".json" or ".xml" but the user wants to treat the content as text data so
it gets text indexing but not structured indexing.
:)
declare function transform($context as map:map, $params as map:map, $content as document-node()) as document-node()
{
    let $node := $content/node()
    let $text := xdmp:quote($node)
    return document { text { $text } }
};
