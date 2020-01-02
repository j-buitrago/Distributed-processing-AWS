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
