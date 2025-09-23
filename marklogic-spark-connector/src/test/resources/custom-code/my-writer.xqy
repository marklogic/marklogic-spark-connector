xquery version "1.0-ml";

declare variable $URI external;

xdmp:document-insert($URI || ".xml", <hello>world</hello>, (
  xdmp:permission("spark-user-role", "read"),
  xdmp:permission("spark-user-role", "update")
));
