# Distributed-processing-AWS

**Description:**

- Tutorial of how to use Amazon Web Services to process big data using a Hadoop cluster.

First of all, extract some data from Twitter api:

```
python3 twitter_stream.py > big_data.txt
```

To run in local:

```
python3 twitter_analyzer.py ./big_data.txt > output.txt
```

To run in a Hadoop cluster using AWS:

```
ssh -i Ireland.pem *your cluster*
```
```
sudo update-alternatives --set python /usr/bin/python3.4
```
```
sudo easy_install pip
```
```
sudo pip install mrjob
```

```
mkdir project
```
```
cd project/
```

```
scp -i Ireland.pem mrjob.conf *your cluster*:project
```

```
scp -i Ireland.pem twitter_analyzer.py *your cluster*:project
```
```
scp -i Ireland.pem AFIN-111.txt *your cluster*:project
```
```
scp -i Ireland.pem States-USA.csv *your cluster*:project
```

Then upload your file *big_data.txt* to a S3 bucket, create a dir to your output and run:

```
python3 twitter_analyzer.py -r emr *your S3 path file*/big_data.txt --conf-path mrjob.conf --states=States-USA.csv --dic=AFINN-111.txt --output-dir=*your S3 path output* --instance-type m3.xlarge --num-core-instances 2 --region eu-west-1
```
