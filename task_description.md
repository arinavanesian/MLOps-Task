# Description
As an ML Engineer, you need to develop a prototype (a demo project) of 
a scheduled feature pipeline. The feature pipeline must compute daily features 
for each client of the transaction dataset MBD-mini. You can load the dataset from
 huggingface: https://huggingface.co/datasets/ai-lab/MBD-mini/tree/main detail.tar.gz. In the archive, you find the folder trx with data for the demo feature pipeline development.


Your pipeline should compute daily aggregate features for each client in a month window. 
For your prototype, you need to implement at least 2 aggregate features, for example, the daily 
mean amount per client and the daily mean by event_type per client(for each date in month period). The pipeline must work in two modes: compute only one day's worth of data (daily mode) and compute the features for all dates within the range from the min to the max date in the dataset(backfill mode). The pipeline must be containerized with Docker and run with a scheduler.

Try to develop the computing function that can be used for both modes: daily and backfill. 
To do this, you can use
Polars group_by_dynamic.https://docs.pola.rs/api/python/stable/reference/dataframe/api/polars.DataFrame.group_by_dynamic.html#polars.DataFrame.group_by_dynamic

https://docs.pola.rs/user-guide/transformations/time-series/rolling/#annual-average-example. 
In the attachment you find notebook proto_data_eng_mbd.ipynb which help you to get familiar with the data.

 

Wrap the functions into a Dagster pipeline. Use the Dagster’s mechanisms:

DailyPartitionsDefinition

BackfillPolicy.single_run()

build_schedule_from_partitioned_job

https://docs.dagster.io/guides/build/partitions-and-backfills/partitioning-assets

https://docs.dagster.io/guides/automate/schedules/constructing-schedules-for-partitioned-assets-and-jobs

 

Write the README on how to build and deploy your pipeline with Docker. It would be a plus
 if you can deploy your pipeline on a local Kubernetes cluster (minikube, kind, Docker Desktop) with
Dagster helm charts https://docs.dagster.io/deployment/oss/deployment-options/kubernetes/deploying-to-kubernetes

Publish your demo pipeline project on GitHub


Explain how to test your pipeline (how to do unit, integration, performance tests)


#  TODO
use https://crontab.guru

