# hail-ukbb-200k-callset

The setup and scripts for setting up Dataproc and using Hail to generate a sparse matrix callset for the UKBB Pharma5 effort.

## Prereqs

 - A sample map tsv file containing two columns, the sample name and a gs:// path to the sample
   - This tsv should include any control samples you want included in your sparse matrix output
 - A Google Cloud Services project that has permissions to access the input and output buckets for your batch

## High level steps

 1. Setup your local environment
 1. Import the autoscaling policy to a gcloud project
 1. Use `hailctl` to launch a Dataproc instance
 1. Use the `submit.sh` script to launch the job


## Local setup


```
$ mkvirtualenv --python=python3 200k-callset
$ pip install -r requirements.txt
$ hailctl
usage: hailctl [-h] {dataproc,auth,dev,version,batch} ...

```

## Autoscaling

To use [Google Cloud Dataproc autoscaling], you must first have a policy imported
into the project. Here is a sample policy .yaml file, also present in
`autoscaling.yaml`.

```yaml
basicAlgorithm:
  cooldownPeriod: 120s
  yarnConfig:
    gracefulDecommissionTimeout: 0s
    scaleDownFactor: 1.0
    scaleUpFactor: 1.0
secondaryWorkerConfig:
  maxInstances: 1000
  weight: 1
workerConfig:
  maxInstances: 2
  minInstances: 2
  weight: 1
```

To import this into the project, do the following:

```
$ gcloud --project=broad-pharma5 beta dataproc autoscaling-policies import \
    1k-preemptibles --region=us-central1 --source=autoscaling.yaml
```

## Creating a DataProc cluster

To use it when starting a cluster:

```
$ hailctl dataproc --beta start ukbbcallset --project=broad-pharma5 --master-machine-type=n1-highmem-32 --region=us-central1 \
    --max-idle 24h --autoscaling-policy=1k-preemptibles --packages gnomad
```

[Google Cloud Dataproc autoscaling]: https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/autoscaling
