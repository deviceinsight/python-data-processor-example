from common import DatapointValue, Event, invoke_data_processor

test_processor = "../src/python-data-processor"

def test_data_transformation():


    emitted_datapoints, emitted_events = invoke_data_processor(test_processor, {}, [
        DatapointValue("temp", "2014-01-01T10:23:00+0100", "1", "Float"),
        DatapointValue("pressure", "2014-01-01T10:23:05+0100", "5.0", "Float"),
        Event("pressure-high", "2014-01-01T10:23:05+0100", True, "Pressure is high")
    ])

    assert len(emitted_datapoints) == 2
    assert emitted_datapoints[0].datapoint_key == "temp"
    assert emitted_datapoints[0].timestamp == "2014-01-01T10:23:00+0100"
    assert emitted_datapoints[0].value == "42.0"
    assert emitted_datapoints[0].data_type == "Float"
    assert emitted_datapoints[1].datapoint_key == "pressure"
    assert emitted_datapoints[1].timestamp == "2014-01-01T10:23:05+0100"
    assert emitted_datapoints[1].value == "42.0"
    assert emitted_datapoints[1].data_type == "Float"

    assert len(emitted_events) == 1
    assert emitted_events[0].datapoint_key == "pressure-high"
    assert emitted_events[0].timestamp == "2014-01-01T10:23:05+0100"
    assert emitted_events[0].come is True
    assert emitted_events[0].text == "Pressure is high"
    assert emitted_events[0].priority == 100
