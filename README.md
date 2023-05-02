# Big Table Example

Based on [**Python Samples for Cloud Bigtable**](https://github.com/googleapis/python-bigtable/tree/main/samples/hello).

Showcase how to create a table, populate with data, and query rows by key and key-range.

## Execution
- Install dependencies:

```
pip install -r requirements.txt
```

- Create a BigTable instance with a cluster in [GCP](https://console.cloud.google.com/bigtable/instances).
- Login using the console
```
gcloud auth application-default login
```

- Execute the program:
```
python main.py <project-id> <bigtable-instance-id> data/words_mit_10k.txt
```
