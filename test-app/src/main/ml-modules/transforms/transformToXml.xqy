xquery version "1.0-ml";

module namespace transform = "http://marklogic.com/rest-api/transform/transformToXml";

import module namespace json="http://marklogic.com/xdmp/json" at "/MarkLogic/json/json.xqy";

declare function transform(
  $context as map:map,
  $params as map:map,
  $content as document-node()
  ) as document-node()
{
  document {
    json:transform-from-json($content)
  }
};
