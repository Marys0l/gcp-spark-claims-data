# :hospital: Analyzing Healthcare Claims Data using Google Dataproc :microscope:
# Background
## About this Simulation
We will be simulating the case where we've been hired as an IT consultant for a state government agency interested in "using Hadoop for data science." Our work with the state involves managing a cluster that will store the state's All Payers Claims Data. The state will send us a roughly 3GB file every quarter and we will be responsible for loading the file into the cluster and performing a series of operations that help make the data more usable for the state's analysts.
## Why Google Cloud Platform's Dataproc?
* Easier to access machines via SSH
* Cheaper HIPAA compliance


# Getting Started
## 1. Setting up a GCP Account
You will need to create a GCP account. When you get an account, Google will provide you with $300 in credits which is more than enough to complete this simulation.

## 2. Setting up a GCP Project
Create a project in GCP and enable billing for the project so you can use Dataproc.

## 3. Setting up a GCP Bucket
From the console, create a bucket that will be used by your cluster.

## 4. Setting up you Spark Cluster
Now, we will create a Dataproc cluster that has Jupyter and the PySpark kernel. You can do this from the GCP console or by using this statement below. Things to note:
* Creates a three node cluster (1 master, 2 workers)
* I use smaller instances, master and workers nodes all n1-standard-2 which is 2 core, 7.5 GB of memory each
* Set `Initialization Actions` to execute some command to configure:
  * Jupyter (gs://dataproc-initialization-actions/jupyter/jupyter.sh)
  * Hue (gs://dataproc-initialization-actions/hue/hue.sh)

It will take some time for Google to deploy your cluster and for the installation to complete.

## 5. Configure networking so that you can access your cluster
Google recommends using SSH tunneling and SOCKS proxy to access your cluster. For the purposes of this simulation, we're going to poke holes in the network to allow us to access what we need on our cluster. Because we're using a simulated data set, I'm OK doing this.

:warning: This is not recommended for production deployments and will put your data at risk!

You will want to open up:
```
Hadoop  -> tcp:8088
Jyupter -> tcp:8123
Hue     -> tcp:8888
```
# :weight_lifting_man: Exercises

## 1. Loading Data from Jyupter
We'll start by writing a script to load a file from **Jyupter** Notebook.

Look at the `load_csv.py` script.

## 2. Exploring Data with Hue
Next, we'll look at that data using **Hue** running on port `8888`.

## 3. Loading Data by Job Submission
Then, we'll upload a parameterized Python script and use it to submit a **Spark** job.

Look at the `load_claims.py` script.

# Resources
http://www.informit.com/articles/article.aspx?p=2756471&seqNum=5
