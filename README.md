# tspark
TimeSeries Java client for Facebook Beringei. It also includes query service with tags support for metrics.

## Prerequisites - Download and deploy the beringei server from facebook
	https://github.com/facebookincubator/beringei

## Start the Beringei Server
	./beringei_main -beringei_configuration_path /beringeidata/beringei.json -data_directory /beringeidata/data -port 9999 -create_directories -sleep_between_bucket_finalization_secs 60 -allowed_timestamp_behind 900 -bucket_size 1200 -buckets 144 -mintimestampdelta 0 -logtostderr -v=1

## Building the Beringei Java Client
	mvn clean package
	mvn clean package assembly:single (fat jar)

## Samples
    Send & Retrieve metrics data using Java Client

## Grafana dashboards
### Setting up the grafana
	Add data source - sample http://127.0.0.1:58080/tsdb
### Installing the dashboards
### Accesing the data from grafana
### Importing the dashboard from the sample json file

## Features
###	Store and Retieve Metrics to/from Beringei
###	Aggregations

## QueryService APIs
	http://<host>:<port>/tsdb/api/query
	http://<host>:<port>/tsdb/api/aggregators
	http://<host>:<port>/tsdb/api/suggest
	
	(Default sample)	
	http://127.0.0.1:58080/tsdb/api/query
	http://127.0.0.1:58080/tsdb/api/aggregators
	http://127.0.0.1:58080/tsdb/api/suggest
