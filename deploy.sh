#!/bin/sh
# for testing purpose only
# deploys assembly jar to qa environment
sbt assembly
aws s3 --profile cqgc --no-verify-ssl --endpoint-url https://minio-cqgc.infojutras.com cp target/scala-2.12/clin-variant-etl.jar s3://cqgc-qa-app-datalake/jobs/jars/clin-variant-etl.jar