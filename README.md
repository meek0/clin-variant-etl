Clin Variant ETL
===============

Run Zeppelin :
```
docker run -p 8080:8080 --rm -v $PWD/logs:/logs -v $PWD/notebook:/notebook -e ZEPPELIN_LOG_DIR='/logs' -e ZEPPELIN_NOTEBOOK_DIR='/notebook' --name zeppelin apache/zeppelin:0.9.0
```



- Create Clinvar table
- How to run spark submit ? dependencies?


TODO
- Genes
- Exomiser
- Zygosity (https://github.com/Ferlab-Ste-Justine/clin-workflow/blob/17ed3f2853a53a7eaeb30edf57ca455dbf717050/src/main/java/org/chusj/VepHelper.java#L1605)
- Transmission
- Higher impact by variant (impactScore : https://github.com/Ferlab-Ste-Justine/clin-workflow/blob/17ed3f2853a53a7eaeb30edf57ca455dbf717050/src/main/java/org/chusj/VepHelper.java#L1652) 
- freqs by lab



