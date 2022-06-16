from src.data_base.query_data import QueryFromDB

# TODO tests for querry
# python -m pytest .\test\test_query_data.py

def test_query():
    #TODO
    pass

def test_query_df():
    #TODO
    pass

def test_query_data():
    querry = QueryFromDB(token='token', url='url', organisation='test_org', bucket='test_bucket')
    assert querry.bucket_query() == 'from(bucket: "test_bucket")', "bucket querry"

def test_time_query():
    querry = QueryFromDB(token='token', url='url', organisation='test_org', bucket='test_bucket')
    assert querry.time_query(start_time='-1h', stop_time='-0h') == '|> range(start:-1h, stop:-0h)', "time query"

def test_group():
    querry = QueryFromDB(token='token', url='url', organisation='test_org', bucket='test_bucket')
    assert querry.group() == '|> group(columns:["_field"])', "group query"

def test_filter_query():
    querry = QueryFromDB(token='token', url='url', organisation='test_org', bucket='test_bucket')
    assert querry.filter_query(measurement=None, fields=None, tags={None:None}) == '|> filter(fn: (r) => )'
    assert querry.filter_query(measurement='measurement_test', fields=None, tags={None:None}) == '|> filter(fn: (r) => r._measurement == "measurement_test")'
    assert querry.filter_query(measurement=None, fields=['field_test'], tags={None:None}) == '|> filter(fn: (r) => r._field == "field_test"))'
    assert querry.filter_query(measurement=None, fields=['field_test', 'field_test_2'], tags={None:None}) == '|> filter(fn: (r) => r._field == "field_test" or r._field == "field_test_2"))'
    assert querry.filter_query(measurement=None, fields=None, tags={'tag1':'tag_1'}) == '|> filter(fn: (r) => r.tag1 == "tag_1")'
    assert querry.filter_query(measurement=None, fields=None, tags={'tag1':'tag_1', 'tag2':'tag_2'}) == '|> filter(fn: (r) => r.tag1 == "tag_1" and (r.tag2 == "tag_2")'

def test_sort():
    querry = QueryFromDB(token='token', url='url', organisation='test_org', bucket='test_bucket')
    assert querry.sort() == '|> sort(columns: ["_time"])'

def test_yi():
    querry = QueryFromDB(token='token', url='url', organisation='test_org', bucket='test_bucket')
    assert querry.yi() == '|> yield()'

def test_window():
    querry = QueryFromDB(token='token', url='url', organisation='test_org', bucket='test_bucket')
    assert querry.window(every='5m', period='5m', offset='0m', createEmpty='true') == '|> window(every: 5m, period: 5m, offset: 0m, createEmpty: true)'

def test_duplicate():
    querry = QueryFromDB(token='token', url='url', organisation='test_org', bucket='test_bucket')
    assert querry.duplicate() == '|> duplicate(column: "_stop", as: "_time")'

def test_agregate():
    querry = QueryFromDB(token='token', url='url', organisation='test_org', bucket='test_bucket')
    assert querry.agregate(query='', agr='mean', every='5m', period='5m', offset='0m', timeSrc="_stop", timeDst="_time", createEmpty='true') == '|> aggregateWindow(every: 5m, fn: mean, timeSrc: "_stop", timeDst: "_time", createEmpty: true )'
    assert querry.agregate(query='', agr='mean', every='5m', period='5m', offset='1m', timeSrc="_stop", timeDst="_time", createEmpty='true') == '|> window(every: 5m, period: 5m, offset: 1m, createEmpty: true)|> mean()|> duplicate(column: "_stop", as: "_time")'
    assert querry.agregate(query='', agr='mean', every='1m', period='5m', offset='0m', timeSrc="_stop", timeDst="_time", createEmpty='true') == '|> window(every: 1m, period: 5m, offset: 0m, createEmpty: true)|> mean()|> duplicate(column: "_stop", as: "_time")'

def test_shift_time():
    querry = QueryFromDB(token='token', url='url', organisation='test_org', bucket='test_bucket')
    assert querry.shift_time() == '|> timeShift(duration: 0m)'
    assert querry.shift_time(shift = '1m') == '|> timeShift(duration: 1m)'
