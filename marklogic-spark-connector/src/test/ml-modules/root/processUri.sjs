declareUpdate();
var URI;

// A user can do whatever they want in their custom code.
// This is just a simple example for testing purposes.
xdmp.documentInsert(URI + ".json", {"hello": "world"}, {
  "permissions": [xdmp.permission("spark-user-role", "read"), xdmp.permission("spark-user-role", "update")]
});

