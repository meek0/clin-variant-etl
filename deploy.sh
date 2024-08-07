#!/bin/sh

VERSION="${1}"
ENV="${2:-qa}"
USAGE="<version> <env=[qa|staging|prod](default=qa)>"

showUsage()
{
  echo "Usage:"
  echo "  sh ./deploy.sh $USAGE"
  echo "  ./deploy.sh $USAGE (required: chmod +x ./launch.sh)"
  echo
}

if [ -z "$VERSION" ]
  then
    showUsage
    exit 1
fi

# download or build jar
echo "=== JAR ==="
#wget -O clin-variant-etl-$VERSION.jar https://github.com/Ferlab-Ste-Justine/clin-variant-etl/releases/download/$VERSION/clin-variant-etl.jar
sbt clean assembly

# copy + list S3, update profile name if necessary
echo "=== COPY TO S3 ($ENV) ==="
aws --profile cqgc-$ENV --endpoint https://s3.cqgc.hsj.rtss.qc.ca s3 cp target/scala-2.12/clin-variant-etl.jar s3://cqgc-$ENV-app-datalake/jars/clin-variant-etl-$VERSION.jar

echo "=== JARS IN S3 ($ENV) ==="
aws --profile cqgc-$ENV --endpoint https://s3.cqgc.hsj.rtss.qc.ca s3 ls s3://cqgc-$ENV-app-datalake/jars --recursive
