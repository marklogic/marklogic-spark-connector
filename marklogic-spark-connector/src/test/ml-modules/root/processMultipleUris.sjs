declareUpdate();

var LAST_NAMES;

for (var lastName of LAST_NAMES.split(",")) {
  xdmp.documentInsert(`/multiple/${lastName}.json`, {"LastName": lastName}, {
    "permissions": [xdmp.permission("spark-user-role", "read"), xdmp.permission("spark-user-role", "update")],
    "collections": ["process-multiple-test"]
  });
}
