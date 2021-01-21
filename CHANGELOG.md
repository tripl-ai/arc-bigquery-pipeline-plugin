# Change Log

## 1.6.0

- added `projectId` option to `BigQueryExecute`

## 1.5.0

- **NOTE** this plugin now requires `assembly` rather than normal publishing so will not be published via Maven at this time.
- fix the complex shading rules to prevent collisions.
- bump to Arc 3.7.0.

## 1.4.0

- update BigQuery library to 0.18.0.
- add support for updating Google Data Catalog by supplying `dataCatalog`.

## 1.3.1

- revert to BigQuery 0.17.1 due to issue [introduced upstream](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/issues/244).

## 1.3.0

- bump to Arc 3.4.0.
- update BigQuery library to 0.17.2.

## 1.2.0

- **FIX** `BigQueryExecute` was not waiting for execution to complete properly resulting in missed errors.
- add `location` and `jobName` parameters to `BigQueryExecute` to allow finer control of the BigQuery Job.

## 1.1.0

- change dependency to included `spark-bigquery-with-dependencies` only which has bundled shaded dependencies.

## 1.0.0

- initial release.