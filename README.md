Clin Variant ETL
===============

### Run Zeppelin :
```
docker run -p 8080:8080 --rm -v $PWD/logs:/logs -v $PWD/notebook:/notebook -e ZEPPELIN_LOG_DIR='/logs' -e ZEPPELIN_NOTEBOOK_DIR='/notebook' --name zeppelin apache/zeppelin:0.9.0
```

### How to run jobs

#### build jar
```
sbt assembly
```

#### upload jar

Assuming your local aws profile is called `clin`
```shell
aws --profile clin --endpoint https://esc.calculquebec.ca:8080 s3 cp target/scala-2.12/clin-variant-etl.jar s3://spark/clin-variant-etl.jar
```
#### Go to desired job in clin-environments repository

```shell
cd ~/???/clin-environments/qa/spark-jobs/genes-tables-creation/
```

#### run the job using kustomize (https://kustomize.io/)

```shell
kustomize build . | kubectl apply -f -
```

#### check the logs

```shell
kubectl get pods
kubectl logs xxxxx
```
