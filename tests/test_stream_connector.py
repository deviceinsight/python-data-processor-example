from subprocess import Popen, PIPE
import json


class Data:

    def __init__(self, timestamp, asset_urn, asset_type_urn):
        self.timestamp = timestamp
        self.asset_urn = asset_urn
        self.asset_type_urn = asset_type_urn


class DatapointValue(Data):

    def __init__(self, datapoint_key, timestamp, value, data_type="Integer", asset_urn=None, asset_type_urn=None):
        super().__init__(timestamp, asset_urn, asset_type_urn)
        self.datapoint_key = datapoint_key
        self.timestamp = timestamp
        self.value = value
        self.data_type = data_type

    def to_json(self):
        return json.dumps({'datapointValue': {
            'datapointKey': self.datapoint_key,
            'tsIso8601': self.timestamp,
            'value': self.value,
            'dataType': self.data_type
        },
            'assetUrn': self.asset_urn,
            'assetTypeUrn': self.asset_type_urn
        })


def json_dict_to_datapoint_value(json_dict):
    dpv_dict = json_dict['datapointValue']
    return DatapointValue(
        datapoint_key=dpv_dict['datapointKey'],
        timestamp=dpv_dict['tsIso8601'],
        value=dpv_dict['value'],
        data_type=dpv_dict['dataType'],
        asset_urn=json_dict.get('assetUrn'),
        asset_type_urn=json_dict.get('assetTypeUrn'))


def get_datapoint_values(stdout):
    result = []

    for line in stdout.split('\n'):

        if len(line) == 0:
            continue

        line_json = json.loads(line)
        if "datapointValue" not in line_json:
            continue

        result.append(json_dict_to_datapoint_value(line_json))

    return result


class Event(Data):

    def __init__(self, event_key, timestamp, come, text="", extra_properties=None, priority=0, asset_urn=None,
                 asset_type_urn=None):
        super().__init__(timestamp, asset_urn, asset_type_urn)

        # Due to default argument value being mutable otherwise
        if extra_properties is None:
            extra_properties = {}

        self.datapoint_key = event_key
        self.timestamp = timestamp
        self.come = come
        self.text = text
        self.extra_properties = extra_properties
        self.priority = priority

    def to_json(self):
        return json.dumps({'event': {
            'eventKey': self.datapoint_key,
            'tsIso8601': self.timestamp,
            'come': self.come,
            'text': self.text,
            'priority': self.priority,
            'extraProperties': self.extra_properties
        },
            'assetUrn': self.asset_urn,
            'assetTypeUrn': self.asset_type_urn
        })


def json_dict_to_event(json_dict):
    event_dict = json_dict['event']
    return Event(
        event_key=event_dict['eventKey'],
        timestamp=event_dict['tsIso8601'],
        come=event_dict['come'],
        text=event_dict['text'],
        extra_properties=event_dict['extraProperties'],
        priority=event_dict['priority'],
        asset_urn=json_dict.get('assetUrn'),
        asset_type_urn=json_dict.get('assetTypeUrn'))


def get_events(stdout):
    result = []

    for line in stdout.split('\n'):

        if len(line) == 0:
            continue

        line_json = json.loads(line)
        if "event" not in line_json:
            continue

        result.append(json_dict_to_event(line_json))

    return result


def invoke_data_processor(binary, config_dict, input_data, timeout=1.0):
    p = Popen(binary, shell=True, stdin=PIPE, stdout=PIPE, stderr=PIPE, encoding='utf-8')

    data_str = '\n'.join(map(lambda data: data.to_json(), input_data))

    to_connector = "%s\n%s\n" % (json.dumps(config_dict), data_str)

    print(to_connector)
    stdout, stderr = p.communicate(to_connector, timeout=timeout)

    print("--- Begin of stderr from data processor ---")
    print(stderr)
    print("--- End of stderr from data processor ---")

    return get_datapoint_values(stdout), get_events(stdout)


def test_data_transformation():
    binary = "../src/python-data-processor"

    emitted_datapoints, emitted_events = invoke_data_processor(binary, {}, [
        DatapointValue("temp", "2014-01-01T10:23:00+0100", "1.0"),
        Event("pressure-high", "2014-01-01T10:23:05+0100", True, "Pressure is high")
    ])

    assert len(emitted_datapoints) == 1
    assert emitted_datapoints[0].datapoint_key == "temp"
    assert emitted_datapoints[0].value == "42.0"
    assert emitted_datapoints[0].data_type == "Integer"

    assert len(emitted_events) == 1
    assert emitted_events[0].datapoint_key == "pressure-high"
    assert emitted_events[0].timestamp == "2014-01-01T10:23:05+0100"
    assert emitted_events[0].come is True
    assert emitted_events[0].text == "Pressure is high"
    assert emitted_events[0].priority == 100
