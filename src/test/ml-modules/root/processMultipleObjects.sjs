declareUpdate();

const array = fn.head(xdmp.fromJSON(authors));

for (var doc of array) {
  xdmp.documentInsert(`/multiple/${doc["CitationID"]}/${doc["LastName"]}.json`, doc, {
    "permissions": [xdmp.permission("spark-user-role", "read"), xdmp.permission("spark-user-role", "update")],
    "collections": ["custom-schema-test"]
  });
}
