
## Error handling

When reading data with the connector, any error that occurs - whether it is before any data is read or during a request
to MarkLogic to read data - is immediately thrown to the user and the read operation ends without any result being
returned. This is consistent with the design of Spark connectors, where a partition reader is allowed to throw
exceptions and is not given any mechanism for reporting an error and continuing to read data. The connector will strive
to provide meaningful context when an error occurs to assist with debugging the cause of the error.

In practice, it is expected that most errors will be a result of a misconfiguration. For example, the connection and
authentication options may be incorrect, or the Optic query may have a syntax error. Any errors that cannot be
fixed via changes to the options passed to the connector should be
[reported as new issues](https://github.com/marklogic/marklogic-spark-connector/issues) in the connector's GitHub
repository.


