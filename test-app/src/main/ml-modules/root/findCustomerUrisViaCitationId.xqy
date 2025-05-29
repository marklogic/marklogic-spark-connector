xquery version "1.0-ml";
declare namespace json = "http://marklogic.com/xdmp/json";

declare variable $URIs external;

let $values := json:array-values($URIs)
let $citationIds := cts:element-values(xs:QName("CitationID"), (), (),
cts:document-query($values))

return cts:uris((), (), cts:and-query((
    cts:not-query(cts:document-query($values)),
    cts:collection-query('author'),
    cts:json-property-value-query('CitationID', $citationIds)
)))
