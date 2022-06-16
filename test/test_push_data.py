from src.data_base.push_data import PushToDB
from influxdb_client import Point


def test_push_data():
    #TODO
    pass

def test_create_point():
    ptdb = PushToDB(token='test_token', url='test_url', org='test_org')
    pt = ptdb.create_point(measurement='test_measurement', time=1257894000123456000, tags={'tag_1':'tag1'}, fields={'field_1':1})
    p = Point("test_measurement").tag("tag_1", "tag1").field("field_1", 1).time(1257894000123456000)

    assert pt._fields == p._fields, "field"
    assert pt._tags == p._tags, "tag"
    assert pt._name == p._name, "measurement"
    assert pt._time == p._time, "time"
