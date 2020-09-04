Clin Variant ETL
===============

Run Zeppelin :
```
docker run -p 8080:8080 --rm -v $PWD/logs:/logs -v $PWD/notebook:/notebook -e ZEPPELIN_LOG_DIR='/logs' -e ZEPPELIN_NOTEBOOK_DIR='/notebook' --name zeppelin apache/zeppelin:0.9.0
```



- Create Clinvar table
- How to run spark submit ? dependencies?


TODO
- Field clinvar_trait (still required?)
- Field orphanet_group (not use?)
- Transmission
- Frequencies by lab



