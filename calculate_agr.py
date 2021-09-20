from src.fusion.stream_fusion import streamFusion

config = {
    "token":"B1WdM9eu5E-GV5l3zA-eRvbboy_-2jzPyIZUW7x2rbhVLd2DRYRU-UgQrmdOf8bXTKAhMp8XVRhmSUCaeGpTZA==",
    "url": "http://localhost:8086",
    "organisation": "TestOrg",
    "bucket": "TestBucket",
    "fusion": [
        {
        "aggregate":"mean",
        "measurement":"flow",
        "fields":["Value"],
        "tags":None,
        "window":"1h",
        "when": "-0m"
        },
        {
        "aggregate":"max",
        "measurement":"flow",
        "fields":["Value"],
        "tags":None,
        "window":"1d",
        "when": "0s"
        },
        {
        "aggregate":"mean",
        "measurement":"flow",
        "fields":["Value"],
        "tags":None,
        "window": "1h",
        "when": "-1h"
        },
        {
        "aggregate":"median",
        "measurement":"flow",
        "fields":["Value"],
        "tags":None,
        "window": "1h",
        "when": "-2h"
        }
    ]
}
