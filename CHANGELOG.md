# Change Log

## 1.2.0

- **FIX** `BigQueryExecute` was not waiting for execution to complete properly resulting in missed errors.
- add `location` and `jobName` parameters to `BigQueryExecute` to allow finer control of the BigQuery Job.

## 1.1.0

- change dependency to included `spark-bigquery-with-dependencies` only which has bundled shaded dependencies.

## 1.0.0

- initial release.