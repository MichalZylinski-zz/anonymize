# JSON Schema reference

Anonymize CLI is fully driven by JSON configuration file. Such file should consist of four major sections:

* `input` - describes input data properties:
  * `inputType` -  `file` or `rabbitmq`
  * `inputProperties` - defines set of properties depending on selected type. `inputProperties` may contain:
    * `format` - either `csv` or `json`
    * `schema` - list of column names if file is in CSV format
    * `fileName` - location and name of input file 
    * `separator` - delimiter used to separate columns in CSV format (`","` is used by default)
    * `quote` - quote character if used in CSV format
    * `URI` - URI to RabbitMQ server
    * `QueueName` - queue name if RabbitMQ is used
* `output` - describes output data properties:
  * `outputType` - `file` or `rabbitmq`
  * `outputProperties` - defines set of properties depending on selected type. `outputProperties` may contain:
    * `format` - either `csv` or `json`
    * `fileName` - location and name of input file 
    * `replace` - either `true` or `false` - should output file be replaced if exists already
    * `separator` - delimiter used to separate columns in CSV format (`","` is used by default)
    * `quote` - quote character if used in CSV format
    * `URI` - URI to RabbitMQ server
    * `QueueName` - queue name if RabbitMQ is used
* `storage` - properties of metarepository:
  * `storageType` - `memory` (repo is kept in memory while processing and flushed to disk once operation completes)
  * `storageProperties`:
    * `fileName` - location and name of repository file
    * `fileRestore` - if `false` repository file will be created from scratch with every running job. If `true` the same repository may be shared by multiple jobs.

* `rules` - set of transformations to be applied on input file:
  * `remove`: list of columns to be removed
    * `fieldName` - name of the column
  * `replace` - list of columns to be replaced with hash values
    * `fieldName` - name of the column
    * `GlobalName` - unique name of the column used in metarepository. Useful when processing data from multiple sources to avoid naming conflicts.

## Schema example

```json
{
 "rules": {
  "remove": [
   {
    "fieldName": "Date"
   }
  ], 
  "replace": [
   {
    "fieldName": "SessionId", 
    "GlobalName": "id"
   }
  ]
 }, 
 "input": {
  "inputProperties": {
   "schema": [
    "Date", 
    "SessionId", 
    "Value"
   ], 
   "format": "CSV", 
   "fileName": "test.csv"
  }, 
  "inputType": "File"
 }, 
 "storage": {
  "storageType": "memory", 
  "storageProperties": {
   "fileRestore": true, 
   "fileName": "storage.bin"
  }
 }, 
 "output": {
  "outputProperties": {
   "replace": true, 
   "format": "JSON", 
   "fileName": "test_output.json"
  }, 
  "outputType": "File"
 }
}
```


