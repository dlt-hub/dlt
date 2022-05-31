import requests
from typing import Iterator, Sequence, cast
from web3 import Web3, HTTPProvider

from autopoiesis.common import Decimal
from autopoiesis.common.arithmetics import numeric_default_context, numeric_default_quantize
from autopoiesis.common.schema import Schema
from autopoiesis.common.typing import DictStrAny, StrAny

from dlt.pipeline import Pipeline, TExtractorItemWithTable, TExtractorItem


# generator function allows a robust extracting of data source
# - may be scaled up to many threads (generator instance runs in thread pool)
# - may be scaled up to many processes (ie. via airflow but then you need to partition initial state of the generator)
# - the extractor that will provide will execute the full generator atomically and commit data to unpacker when all data is yielded
# - it provides retry mechanism (mind that the data that generator yields is (table_name, function that returns data) so that function may be called several times if needed)
# - it provides special exception type that when thrown from the function will mark the item as permanently failed (TODO:)
# - it will save and restore generator state on next run (TODO:)
def block_generator(state: DictStrAny) -> Iterator[TExtractorItemWithTable]:
    # this code is run only once
    w3 = Web3(Web3.HTTPProvider("https://mainnet.infura.io/v3/9aa3d95b3bc440fa88ea12eaa4456161"))

    # initialize generator with initial state
    last_block = state.get("last_block", 0)

    # 14788082
    highest_block = w3.eth.get_block_number()
    # get 5 blocks max from last run
    current_block = max(last_block + 1, highest_block - 2)

    # do not get highest block (in fact we should have 3 block lag here)
    while current_block < highest_block:

        # this function return tuple (table_name, function returning data item)
        def get_block(block_no: int) -> TExtractorItem:
            def _get() -> Iterator[StrAny]:
                # get rid of AttributeDict (web3 didn't adopt TypedDict)
                print(f"block {block_no}")

                # get block with all transaction
                block = dict(w3.eth.get_block(block_no, full_transactions=True))
                # get rid of AttributeDict (web3 didn't adopt TypedDict)
                transactions: Sequence[DictStrAny] = [dict(tx) for tx in block["transactions"]]  # type: ignore
                for tx in transactions:
                    if "accessList" in tx and len(tx["accessList"]) > 0:
                        tx["accessList"] = [dict(al) for al in tx["accessList"]]
                    # propagate sorting and clustering info (we could also do it in unpacker: TODO:!)
                    tx["block_timestamp"] = block["timestamp"]
                    # convert wei to eth providing with numeric precision at the target
                    with numeric_default_context():
                        # 9 scale vs 18 digits ether scale: a no go for any financial operations
                        # TODO: better support for decimal types
                        tx["eth_value"] = numeric_default_quantize(Decimal(tx["value"]) / 10**18)
                block["transactions"] = transactions

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
                provider = cast(HTTPProvider, w3.provider)
                r = requests.post(provider.endpoint_uri, json=batch, timeout=(20, 12))
                r.raise_for_status()
                receipts = r.json()
                for tx_receipt, tx in zip(receipts, transactions):
                    if "result" not in tx_receipt:
                        raise ValueError(tx_receipt)
                    tx_receipt = tx_receipt["result"]
                    assert tx_receipt["transactionHash"] == tx["hash"].hex()
                    tx["transactionIndex"] = Web3.toInt(hexstr=tx_receipt["transactionIndex"])
                    tx["status"] = Web3.toInt(hexstr=tx_receipt["status"])
                    tx["logs"] = tx_receipt["logs"]
                    for log in tx_receipt["logs"]:
                        log["blockNumber"] = Web3.toInt(hexstr=log["blockNumber"])
                        log["transactionIndex"] = Web3.toInt(hexstr=log["transactionIndex"])
                        log["logIndex"] = Web3.toInt(hexstr=log["logIndex"])
                        log["block_timestamp"] = block["timestamp"]
                        log["block_hash"] = block["hash"]

                return iter([block])
            return _get

        # def get_transactions(block_no: int) -> TExtractorItem:
        #     def _get() -> Iterator[StrAny]:
        #         print(f"txs {block_no}")
        #         block = w3.eth.get_block(block_no, full_transactions=True)
        #         transactions: Sequence[DictStrAny] = [dict(tx) for tx in block["transactions"]]
        #         for tx in transactions:
        #             if "accessList" in tx and len(tx["accessList"]) > 0:
        #                 print(tx["accessList"])
        #                 tx["accessList"] = [dict(al) for al in tx["accessList"]]
        #                 print(tx["accessList"])
        #         return iter(transactions)
        #     return _get

        yield "blocks", get_block(current_block)
        # no problem with returning may different tables from single generator
        # yield "transactions", get_transactions(current_block)
        current_block += 1
        # update state, it will be read and stored with the pipeline instance ONLY when whole generator finishes
        # state participates in the same atomic operation as data
        state["last_block"] = current_block


credentials = Pipeline.load_gcp_credentials("_secrets/project1234_service.json", "mainnet_4")
# credentials = PostgresPipelineCredentials("redshift", "chat_analytics_rasa", "mainnet", "loader", "3.73.90.3")

pipeline = Pipeline("ethereum")
schema: Schema = None
# you can comment the schema loading below to see what will be automatically inferred
# in case of ethereum data the fundamental problem is 2^256 integer size which does not fit in any BIGINT
# type. that is fixed in schema loaded below
schema = Pipeline.load_schema_from_file("examples/schemas/ethereum_schema.yml")
pipeline.create_pipeline(credentials, schema=schema)
print(pipeline.root_path)

m = pipeline.extract_generator(block_generator)
if m.has_failed:
    print("Extracting failed")
    print(pipeline.last_run_exception)
    exit(0)

m = pipeline.unpack()
if m.has_failed:
    print("Unpacking failed")
    print(pipeline.last_run_exception)
    exit(0)

# get inferred schema
schema = pipeline.get_current_schema()
# print(schema.as_yaml(remove_default_hints=True))
# pipeline.save_schema_to_file("examples/schemas/inferred_ethereum_schema.yml", schema)

# get loads
new_loads = pipeline.list_unpacked_loads()
print(new_loads)

# load packages
m = pipeline.load()
if m.has_failed:
    print("Loading failed, fix the problem, restore the pipeline and run loading packages again")
    print(pipeline.last_run_exception)
else:
    # should be empty
    new_loads = pipeline.list_unpacked_loads()
    print(new_loads)

    # now enumerate all complete loads if we have any failed packages
    # complete but failed job will not raise any exceptions
    completed_loads = pipeline.list_completed_loads()
    # print(completed_loads)
    for load_id in completed_loads:
        print(f"Checking failed jobs in {load_id}")
        for job, failed_message in pipeline.list_failed_jobs(load_id):
            print(f"JOB: {job}\nMSG: {failed_message}")
