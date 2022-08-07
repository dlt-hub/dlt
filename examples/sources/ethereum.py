from typing import Any, Dict, Iterator, List, Tuple, Type, TypedDict, Union, cast, Sequence
from hexbytes import HexBytes
import requests

from dlt.common.typing import DictStrAny, StrAny
from dlt.common.sources import TDeferred, TItem, defer_iterator, with_retry, with_table_name

from dlt.pipeline.exceptions import MissingDependencyException
from examples.sources.eth_source_utils import decode_log, decode_tx, fetch_sig_and_decode_log, fetch_sig_and_decode_tx, maybe_update_abi, prettify_decoded, save_abis

try:
    # import gracefully and produce nice exception that explains the user what to do
    from web3 import Web3, HTTPProvider
    from web3.middleware import geth_poa_middleware
    from eth_typing.evm import ChecksumAddress
    from web3._utils.method_formatters import get_result_formatters
    from web3._utils.rpc_abi import RPC
    from web3.types import LogReceipt, EventData, ABIEvent

    from examples.sources.eth_source_utils import load_abis, TABIInfo, ABIFunction
except ImportError:
    raise MissingDependencyException("Ethereum Source", ["web3"], "Web3 is a all purpose python library to interact with Ethereum-compatible blockchains.")

# this source gets full state of any Ethereum compatible blockchain.
# - can be easily scaled up to hundreds of parallel extractions (ie. via Airflow)
# - supports retrying if node fails
# - supports multi threaded extraction on single machine via get_deferred_source
# - supports pipeline state so it may be used to just get new blocks when they are available
# - the return value is iterator (or deferred iterator) so still mappings may be used (ie. to decode transactions and logs), see example


def get_source(node_url: str, no_blocks: int, last_block: int = None, abi_dir: str = None, lag: int = 2, is_poa: bool = False, supports_batching: bool = True, state: DictStrAny = None) -> Iterator[DictStrAny]:
    return _get_source(False, node_url, no_blocks, last_block, abi_dir, lag, is_poa, supports_batching, state)  # type: ignore


def get_deferred_source(node_url: str, no_blocks: int, last_block: int = None, abi_dir: str = None, lag: int = 2, is_poa: bool = False, supports_batching: bool = True, state: DictStrAny = None) -> Iterator[TDeferred[DictStrAny]]:
    return _get_source(True, node_url, no_blocks, last_block, abi_dir, lag, is_poa, supports_batching, state)  # type: ignore


def _get_source(is_deferred: bool, node_url: str, no_blocks: int, last_block: int, abi_dir: str, lag: int, is_poa: bool, supports_batching: bool, state: DictStrAny = None) -> Union[Iterator[TItem], Iterator[TDeferred[DictStrAny]]]:

    # this code is run only once
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.77 Safari/537.36"
    }
    w3 = Web3(Web3.HTTPProvider(node_url, request_kwargs={"headers": headers}))
    if is_poa:
        w3.middleware_onion.inject(geth_poa_middleware, layer=0)

    # load abis from abi_dir
    contracts = load_abis(w3, abi_dir)

    # last block is not provided then take the highest block from the chain
    if last_block is None:
        # 14788082
        last_block = w3.eth.get_block_number() - lag

    # get default current block
    current_block = last_block - no_blocks + 1

    # get current block from the state if available
    if state:
        current_block = state.get("ethereum_current_block", current_block)
        print(f"Will continue from {current_block} to {last_block}")
    else:
        print(f"Will start from {current_block} to {last_block}")

    assert current_block >= 0
    if current_block > last_block:
        print(f"Nothing to do")
        return

    # get chain id
    chain_id = w3.eth.chain_id

    # do not get highest block (in fact we should have 3 block lag here)
    while current_block <= last_block:
        # get rid of AttributeDict (web3 didn't adopt TypedDict)
        print(f"requesting block {current_block}")

        @defer_iterator
        @with_retry()
        def _get_block_deferred() -> DictStrAny:
            return _get_block(w3, current_block, chain_id, supports_batching)

        @with_retry()
        def _get_block_retry() -> DictStrAny:
            return _get_block(w3, current_block, chain_id, supports_batching)

        # yield deferred items or actual item values
        if is_deferred:
            yield _get_block_deferred()
        else:
            block = _get_block_retry()
            yield block
            yield from _decode_block(w3, block, contracts)
        current_block += 1

    # update state, it will be read and stored with the pipeline instance ONLY when whole iterator finishes
    # state participates in the same atomic operation as data
    # this must happen after last yield
    print(f"finalizing {state}")
    save_abis(abi_dir, contracts.values())
    if state is not None:
        state["ethereum_current_block"] = current_block


def _get_block(w3: Web3, current_block: int, chain_id: int, supports_batching: bool) -> DictStrAny:
    print(f"producing block {current_block}")
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.77 Safari/537.36"
    }

    # get block with all transaction
    block = dict(w3.eth.get_block(current_block, full_transactions=True))
    # set explicit chain id
    block["chain_id"] = chain_id
    # get rid of AttributeDict (web3 didn't adopt TypedDict)
    attr_txs = cast(Sequence[Any], block["transactions"])
    transactions: Sequence[DictStrAny] = [dict(tx) for tx in attr_txs]
    # maybe_unknown_inputs: List[str] = []
    for tx in transactions:
        if "accessList" in tx and len(tx["accessList"]) > 0:
            tx["accessList"] = [dict(al) for al in tx["accessList"]]
        # propagate sorting and clustering info (we could also do it in unpacker: TODO:!)
        tx["blockTimestamp"] = block["timestamp"]
        # overwrite chain_id which is not provided in all cases
        tx["chainId"] = chain_id

    block["transactions"] = transactions
    block["logsBloom"] = bytes(cast(HexBytes, block["logsBloom"]))  # serialize as bytes

    receipts = []
    log_formatters = get_result_formatters(RPC.eth_getLogs, w3.eth)
    provider = cast(HTTPProvider, w3.provider)
    rpc_endpoint_url = provider.endpoint_uri
    if supports_batching:
        # get transaction receipts using batching. web3 does not support batching so we must
        # call node directly and then convert hex numbers to ints
        batch = []
        for idx, tx in enumerate(transactions):
            batch.append({
                "jsonrpc": "2.0",
                "method": "eth_getTransactionReceipt",
                "params": [tx["hash"]],
                "id": idx
            })

        r = requests.post(rpc_endpoint_url, json=batch, timeout=(20, 12), headers=headers)
        r.raise_for_status()
        receipts = r.json()
    else:
        for idx, tx in enumerate(transactions):
            r = requests.post(rpc_endpoint_url, json={
                "jsonrpc": "2.0",
                "method": "eth_getTransactionReceipt",
                "params": [tx["hash"]],
                "id": idx
            }, timeout=(20, 12), headers=headers)
            r.raise_for_status()
            receipts.append(r.json())
    for tx_receipt, tx in zip(receipts, transactions):
        if "result" not in tx_receipt:
            raise ValueError(tx_receipt)
        tx_receipt = tx_receipt["result"]
        assert tx_receipt["transactionHash"] == tx["hash"].hex()
        tx["transactionIndex"] = tx_receipt["transactionIndex"]
        tx["status"] = tx_receipt["status"]
        tx["logs"] = [dict(log) for log in log_formatters(tx_receipt["logs"])]
        log: LogReceipt = None
        for log in tx["logs"]:
            log["topic"] = log["topics"][0]
            # log["blockHash"] = block["hash"]

    return block


def _decode_block(w3: Web3, block: StrAny, contracts: Dict[ChecksumAddress, TABIInfo]) -> Iterator[StrAny]:
    transactions: Sequence[Any] = block["transactions"]
    for tx in transactions:
        tx_info = {
            "_tx_blockNumber": tx["blockNumber"],
            "_tx_blockTimestamp": tx["blockTimestamp"],
            "_tx_transactionHash": tx["hash"],
            "_tx_transactionIndex": tx["transactionIndex"],
            "_tx_address": tx["to"],
            "_tx_status": tx["status"]
        }
        # decode transaction
        if tx["to"] in contracts:
            abi_info = contracts[tx["to"]]
            tx_input = HexBytes(tx["input"])
            selector = tx_input[:4]
            tx_abi = cast(ABIFunction, abi_info["selectors"].get(selector))
            tx_args: DictStrAny = None
            fn_name: str = None

            if tx_abi:
                tx_args = decode_tx(w3.codec, tx_abi, tx_input[4:])
                fn_name = tx_abi["name"]
            else:
                if abi_info["unknown_selectors"].get(selector.hex()) is None:
                    # try to decode with an api
                    sig, fn_name, tx_args, tx_abi = fetch_sig_and_decode_tx(w3.codec, tx_input)
                    maybe_update_abi(abi_info, selector, tx_abi, block["number"])

            if tx_args:
                tx_args =  with_table_name(tx_args, abi_info["name"] + "_calls_" + fn_name)
                # yield arguments with reference to transaction
                tx_args.update(tx_info)
                yield prettify_decoded(tx_args, tx_abi)

        # decode logs
        log: LogReceipt
        for log in tx["logs"]:
            if log["address"] in contracts:
                abi_info = contracts[log["address"]]
                selector = log["topic"]
                event_abi = cast(ABIEvent, abi_info["selectors"].get(selector))
                event_data: EventData = None
                if event_abi:
                    event_data = decode_log(w3.codec, event_abi, log)
                else:
                    if abi_info["unknown_selectors"].get(selector.hex()) is None:
                        # try to decode with an api
                        sig, event_data, event_abi = fetch_sig_and_decode_log(w3.codec, log)
                        maybe_update_abi(abi_info, selector, event_abi, block["number"])

                if event_data:
                    ev_args =  with_table_name(dict(event_data["args"]), abi_info["name"] + "_logs_" + event_data["event"])
                    # yield arguments with reference to transaction and log
                    ev_args.update(tx_info)
                    ev_args.update({
                        "_tx_logIndex": event_data["logIndex"]
                    })
                    yield prettify_decoded(ev_args, event_abi)
