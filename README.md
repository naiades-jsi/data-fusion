# Data fusion

## influxDB
Download indluxDB from: [influxDB](https://www.influxdata.com/) (we used docker to download and run).
(Components are not tested yet and not finished.)

## Deployment
Directory: `/users/mcerin/services/data-fusion`

## Database Schema

InfluxDB hierarchy:

* organisation
* bucket
* measurement
* field
* tag


## Config

```
config2 = {
    // influx data
    "token":"B1WdM9eu5E-GV5l3zA-eRvbboy_-2jzPyIZUW7x2rbhVLd2DRYRU-UgQrmdOf8bXTKAhMp8XVRhmSUCaeGpTZA==",
    "url": "http://localhost:8086",
    "organisation": "TestOrg",
    // bucket is equivalent of an SQL table
    "bucket": "TestBucket",
    // optional part for generating historic values
    "startTime":"-7h",
    "stopTime":"-0h",
    // startTime and stopTime can be defiend with ISO timestamp
    "every":"10m",
    
    "fusion": [
        {
            "aggregate": "mean",
            "measurement": "python_test", // influx specific
            "fields": ["Value1"],
            // "tags": { "null": null } if there are no tags
            "tags": {"tag1": ["water"]},
            // window can be defined also in milliseconds
            "window": "2m"
            // what: "python", what: "influx" (default) - mode for calculating aggregates
        },
        {
            "aggregate":"mean",
            "measurement":"python_test",
            "fields": ["Value2", "Value1"],
            "tags": {"tag1": ["pressure"]},
            // before 23 hours
            "when": "-23h",
            "window":"1m"
        },
        {
            "aggregate": "mean",
            "measurement": "python_test",
            "fields": ["Value2", "Value1"],
            "tags": {"tag1": ["pressure"]},
            "window": 5 * 60 * 1000
        }
    ]
}
```

List of aggregates: these aggregates are directly Influx aggregates. 


## Python
```
from src.fusion.stream_fusion import bachFusion
config = ...
sf = bachFusion(config)
l, t = sf.buildFeatureVectors()
// t - vector of times
// l - matrix of feature values
```

## Running
Example: `index.NAIADES.leakage_pressure.py`.

Use environment:
`source /home/mcerin/influx_env/bin/activate`
Using `pm2` (user `mcerin`).

pm2 start [name of file]


## Source code

* `/src` - main directory
* `/data_base` - sending to Influx and reading from Influx
  * `push_data.py` - class to input data to Influx (via point object)
  * `query_data.py` - definition of queries (e. g. aggregate query)
  * `kafka.py` - connect to kafka - testing
* `/fusion` - data fusion
  * `time_parser.py` - help function for time
  * `stream_fusion.py` - main class, with main functionality
  * `/aggregates` - everything about aggregates, includes influx aggregates and python aggregates
