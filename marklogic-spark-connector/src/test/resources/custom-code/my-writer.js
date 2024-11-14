declareUpdate();
var URI;

xdmp.documentInsert(URI + ".json", {"hello": "world"}, {
  "permissions": [xdmp.permission("spark-user-role", "read"), xdmp.permission("spark-user-role", "update")]
});

