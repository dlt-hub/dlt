from dlt.common import json, Decimal, pendulum
from dlt.common.arithmetics import numeric_default_context
from dlt.common.json import _DECIMAL, custom_pua_decode, json_typed_dumps

from tests.cases import JSON_TYPED_DICT

def test_json_decimals() -> None:
    # deserialize as float
    d = json.loads('{"v": 21.37}')
    assert type(d["v"]) is float

    # serialize as string
    s = json.dumps({"decimal": Decimal("21.37")})
    assert s == '{"decimal": "21.37"}'

    # serialize max precision which is 10**38
    s = json.dumps({"decimal": Decimal(10**29) - Decimal("0.001")})
    assert s == '{"decimal": "99999999999999999999999999999.999"}'

    # serialize untypical context
    with numeric_default_context(precision=77):
        doc = {"decimal": Decimal(10**74) - Decimal("0.001")}
    # serialize out of local context
    s = json.dumps(doc)
    # full precision. you need to quantize yourself if you need it
    assert s == '{"decimal": "99999999999999999999999999999999999999999999999999999999999999999999999999.999"}'


def test_json_pendulum() -> None:
    dt_str = "2005-04-02T20:37:37.358236+00:00"
    r = json.loads('{"t": "%s"}' % dt_str)
    # iso datetime string not deserialized to dates automatically
    assert r["t"] == dt_str
    # use zulu notation for UTC
    now = pendulum.parse("2005-04-02T20:37:37.358236Z")
    s = json.dumps({"t": now})
    # must serialize UTC timezone
    assert s.endswith('Z"}')
    s_r = json.loads(s)
    assert pendulum.parse(s_r["t"]) == now
    # mock hh:mm (incorrect) TZ notation which must serialize to UTC as well
    s_r = json.loads(s[:-3] + '+00:00"}')
    assert pendulum.parse(s_r["t"]) == now


def test_json_encode() -> None:
    j = json.dumps(JSON_TYPED_DICT)
    d = json.loads(j)
    # all our values are strings
    assert all(isinstance(v, str) for v in d.values())


def test_json_typed_encode() -> None:
    j = json_typed_dumps(JSON_TYPED_DICT)
    # use normal decoder
    d = json.loads(j)
    # we have pua chars
    assert d["decimal"][0] == _DECIMAL
    # decode all
    d_d = {k: custom_pua_decode(v) for k,v in d.items()}
    assert d_d == JSON_TYPED_DICT
