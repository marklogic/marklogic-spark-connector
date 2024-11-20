declareUpdate();
const doc = fn.head(xdmp.fromJSON(author));
xdmp.documentInsert("/temp/" + doc["LastName"] + ".json", doc, {
  "permissions": [xdmp.permission("spark-user-role", "read"), xdmp.permission("spark-user-role", "update")],
  "collections": ["custom-schema-test"]
});
