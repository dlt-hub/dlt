import os
import re
import itertools
from textwrap import indent
from hexbytes import HexBytes
import requests
from typing import Any, Dict, Iterable, Iterator, List, Sequence, Type, TypedDict, Tuple, cast

from web3 import Web3
from web3.types import ABI, ABIElement, ABIFunction, ABIEvent, ABIFunctionParams, ABIFunctionComponents, LogReceipt, EventData
from web3._utils.abi import get_abi_input_names, get_abi_input_types, map_abi_data, get_indexed_event_inputs, normalize_event_input_types
from web3._utils.normalizers import BASE_RETURN_NORMALIZERS
from web3._utils.events import get_event_data, get_event_abi_types_for_decoding
from eth_typing import HexStr
from eth_typing.evm import ChecksumAddress
from eth_abi.codec import ABIDecoder
from eth_abi.exceptions import DecodingError
from eth_utils.abi import function_abi_to_4byte_selector, event_abi_to_log_topic

from dlt.common import json
from dlt.common.typing import DictStrAny, StrAny, StrStr


class TABIInfo(TypedDict):
    name: str
    abi_file: str
    abi: ABI
    unknown_selectors: DictStrAny
    file_content: StrAny
    selectors: Dict[HexBytes, ABIElement]


class EthSigItem(TypedDict):
    name: str
    filtered: bool


def abi_to_selector(abi: ABIElement) -> HexBytes:
    if abi["type"] == "event":
        return HexBytes(event_abi_to_log_topic(abi))  # type: ignore
    elif abi["type"] == "function":
        return HexBytes(function_abi_to_4byte_selector(abi))  # type: ignore
    else:
        raise ValueError(abi)


def load_abis(w3: Web3, abi_dir: str) -> Dict[ChecksumAddress, TABIInfo]:
    contracts: Dict[ChecksumAddress, TABIInfo] = {}
    if abi_dir:
        for abi_file in os.scandir(abi_dir):
            if not abi_file.is_file():
                continue
            address  = w3.toChecksumAddress(os.path.basename(abi_file).split(".")[0])
            with open(abi_file, mode="r", encoding="utf-8") as f:
                abi: DictStrAny = json.load(f)
                # print(f"adding contract {abi['name']} with address {address}")
                contracts[address] = {
                    "name": abi['name'],
                    "abi": abi["abi"],
                    "abi_file": abi_file.name,
                    "unknown_selectors": abi.setdefault("unknown_selectors", {}),
                    "file_content": abi,
                    "selectors": {abi_to_selector(a):a for a in abi["abi"] if a["type"] in ["function", "event"]}
                }
    return contracts


def save_abis(abi_dir: str, abis: Iterable[TABIInfo]) -> None:
    if abi_dir:
        for abi in abis:
            save_path = os.path.join(abi_dir, abi["abi_file"])
            # print(f"saving {save_path}")
            with open(save_path, mode="w", encoding="utf-8") as f:
                json.dump(abi["file_content"], f, indent=2)


def maybe_update_abi(abi_info: TABIInfo, selector: HexBytes, new_abi: ABIElement, in_block: int) -> None:
    add_info = {
        "selector": selector.hex(),
        "block": in_block
    }
    if not new_abi:
        abi_info["unknown_selectors"][selector.hex()] = add_info
        print(f"could not be found or decoded: {add_info}")
    else:
        print(f"found and decoded {add_info} to {new_abi['name']}")
        new_abi["_dlt_meta"] = add_info  # type: ignore
        abi_info["abi"].append(new_abi)  # type: ignore
        abi_info["selectors"][selector] = new_abi


def signature_to_abi(sig_type: str, sig: str) -> ABIElement:
    # get name and pass remainder for tokenization
    name, remainder = sig.split("(", maxsplit=1)

    # simple tokenizer that yields "(" ")" "," and tokens between them. empty tokens are ignored
    def tokens() -> Iterator[str]:
        start = 0
        pos = 0
        while pos < len(remainder):
            char = remainder[pos]
            if char in ["(", ")",","]:
                if pos - start > 0:
                    yield remainder[start:pos].strip()
                yield char
                start = pos = pos + 1
            else:
                # move to next "," and return token
                pos += 1

    tokens_gen = tokens()

    abi: ABIElement = {
        "name": name,
        "type": sig_type,  # type: ignore
        "inputs": [],
        "outputs": []
    }

    def _to_components(inputs: List[ABIFunctionComponents], param: str) -> None:
        typ_: str = None
        input_def: ABIFunctionParams = {}
        i = 0

        while tok := next(tokens_gen):
            if tok == "(":
                # step into component parsing
                # tok = tok[1:]
                input_def["components"] = []
                _to_components(input_def["components"], f"{param}_{i}")  # type: ignore
                typ_ = "tuple"
            elif tok == "[]":
                # mod last typ_ to be array
                assert typ_ is not None
                typ_ += tok
            elif tok in [")",","]:
                # add current type
                assert typ_ is not None
                input_def.update({
                    "name": f"{param}_{i}",
                    "type": typ_
                })
                inputs.append(input_def)
                if tok == ")":
                    # up from component parsing
                    return
                else:
                    # prepare for new type
                    input_def = {}
                    typ_ = None
                    i += 1
            else:
                typ_ = tok
    _to_components(abi["inputs"], "param")  # type: ignore

    return abi


def decode_tx(codec: ABIDecoder, abi: ABIFunction, params: HexBytes) -> DictStrAny:
    names = get_abi_input_names(abi)
    types = get_abi_input_types(abi)
    decoded = codec.decode_abi(types, params)
    normalized = map_abi_data(BASE_RETURN_NORMALIZERS, types, decoded)

    return dict(zip(names, normalized))


def decode_log(codec: ABIDecoder, abi: ABIEvent, log: LogReceipt) -> EventData:
    log_topics = log["topics"][1:]
    log_topics_abi = get_indexed_event_inputs(abi)
    log_topic_normalized_inputs = normalize_event_input_types(log_topics_abi)
    log_topic_types = get_event_abi_types_for_decoding(log_topic_normalized_inputs)

    if len(log_topics) != len(log_topic_types):
        # we have incorrect information on topic indexes in abi so we'll recursively try to discover the right combination
        for indexed_inputs in itertools.combinations(abi["inputs"], len(log_topics)):
            print(f"attempt recursive decoding for {log['topics'][0].hex()}")
            for input_ in abi["inputs"]:
                input_["indexed"] = False
            for input_ in indexed_inputs:
                input_["indexed"] = True

            try:
                print(abi)
                rv: EventData = get_event_data(codec, abi, log)
                print("recovered indexes in abi: ")
                print(abi)
                return rv
            except DecodingError:
                pass

            raise ValueError("None of the indexed topic combinations decoded correctly")

    return cast(EventData, get_event_data(codec, abi, log))


def fetch_sig(sig_type: str, selector: HexStr) -> Sequence[EthSigItem]:
    r = requests.get(f"https://sig.eth.samczsun.com/api/v1/signatures?{sig_type}={selector}")
    if r.status_code >= 300:
        print(f"{selector} is not known at sig.eth.samczsun.com")
        r.raise_for_status()
    resp = r.json()
    if not resp["ok"]:
        print(f"{selector} is not OK at sig.eth.samczsun.com")
        r.raise_for_status()

    return resp["result"][sig_type][selector]  # type: ignore


def fetch_sig_and_decode_log(codec: ABIDecoder, log: LogReceipt) -> Tuple[str, EventData, ABIEvent]:
    topic = log["topic"]

    for sig in fetch_sig("event", HexStr(topic.hex())):
        sig_name: str = sig["name"]
        abi = cast(ABIEvent, signature_to_abi("event", sig_name))
        assert abi_to_selector(abi) == topic
        abi["anonymous"] = False
        for input_ in abi["inputs"]:
            input_.setdefault("indexed", False)

        try:
            return sig_name, decode_log(codec, abi, log), abi
        except DecodingError:
            print("Could not be decoded")
            continue

    # no known signatures or nothing could be decoded
    return None, None, None


def fetch_sig_and_decode_tx(codec: ABIDecoder, tx_input: HexBytes) -> Tuple[str, str, DictStrAny, ABIFunction]:
    selector, params = tx_input[:4], tx_input[4:]

    for sig in fetch_sig("function", HexStr(selector.hex())):
        sig_name: str = sig["name"]
        abi = cast(ABIFunction, signature_to_abi("function", sig_name))
        assert abi_to_selector(abi) == selector
        try:
            return sig_name, abi["name"], decode_tx(codec, abi, params), abi
        except DecodingError as ex:
            print(f"Could not be decoded {ex}")
            continue

    # no known signatures or nothing could be decoded
    return None, None, None, None


def prettify_decoded(decoded: DictStrAny, abi: ABIElement) -> DictStrAny:
    # TODO: detect ERC20 and format decimals properly
    recode_tuples(decoded, abi)
    flatten_batches(decoded, abi)
    return decoded


def flatten_batches(decoded: DictStrAny, abi: ABIElement) -> None:
    batch = []
    for input_ in abi["inputs"]:
        prev_len: int = None
        decoded_val = decoded.get(input_["name"])
        if decoded_val is not None:
            if isinstance(decoded_val, Sequence):
                if prev_len is None or prev_len == len(decoded_val):
                    batch.append(input_["name"])
                    prev_len = len(decoded_val)
                else:
                    # list length not equal so this is not a batch
                    return
            else:
                # not all elements are lists so this is not a batch
                return

    decoded["batch"] = [{n:decoded[n][i] for n in batch} for i in range(prev_len)]
    for n in batch:
        del decoded[n]


def recode_tuples(decoded: DictStrAny, abi: ABIElement) -> None:
    # replaces lists with dicts representing named tuples

    def _replace_component(decoded_component: Sequence[Any], inputs: Sequence[ABIFunctionComponents]) -> Dict[str, Any]:
        for idx, i_i in enumerate(zip(decoded_component, inputs)):
            item, input_ = i_i
            if input_["type"] == "tuple" and isinstance(item, Sequence):
                recoded = _replace_component(item, input_["components"])
                decoded_component[idx] = recoded  # type: ignore
        return {input_["name"]:item for item, input_ in zip(decoded_component, inputs)}

    input_: ABIFunctionParams = None
    for input_ in abi["inputs"]:  # type: ignore
        decoded_val = decoded.get(input_["name"])
        if input_["type"] == "tuple" and isinstance(decoded_val, Sequence):
            decoded[input_["name"]] = _replace_component(decoded_val, input_["components"])
