from hexbytes import HexBytes
from dlt.common import Decimal
from dlt.common.time import sleep
from dlt.common.typing import DictStrAny
from dlt.pipeline import Pipeline, GCPPipelineCredentials
from dlt.common.arithmetics import numeric_default_context, numeric_default_quantize

from examples.schemas.ethereum import discover_schema
from examples.sources.eth_source_utils import recode_tuples, flatten_batches, abi_to_selector, signature_to_abi
from examples.sources.ethereum import get_source

# abi = signature_to_abi("event", "AxieggSpawned(uint256,uint256,uint256,uint256[],(uint256,uint256)[],(uint256,uint256))")
# # abi = signature_to_abi("event", "AxieggSpawned(uint256,uint256,uint256,uint256,(uint256,uint256),(uint256,uint256))")
# import pprint
# pprint.pprint(abi)
# assert abi_to_selector(abi) == HexBytes("0x280ad04291df39737839c19243929c95199e424e2518ea891b63deef3808bfe2")
# exit(-1)
# decoded = {
#         "_tokenTypes": [
#             1
#         ],
#         "_tokenAddresses": [
#             "0x32950db2a7164aE833121501C797D79E7B79d74C"
#         ],
#         "_tokenNumbers": [
#             11351913
#         ],
#         "_startingPrices": [
#             40000000000000000
#         ],
#         "_endingPrices": [
#             20000000000000000
#         ],
#         "_exchangeTokens": [
#             "0xc99a6A985eD2Cac1ef41640596C5A5f9F4E19Ef5"
#         ],
#         "_durations": [
#             432000
#         ],
#         "_dlt_meta": {
#             "table_name": "Marketplace_calls_createAuction"
#         },
#         "_tx_blockNumber": 16039643,
#         "_tx_blockTimestamp": 1659723680,
#         "_tx_transactionHash": "\uf02a0x5714a265cf3456ad6bbacdf7a577f094f99b676bf19fde81153eea96fa224850",
#         "_tx_transactionIndex": "0x0",
#         "_tx_address": "0x213073989821f738A7BA3520C3D31a1F9aD31bBd",
#         "_tx_status": "0x1"
#     }

# flatten_batches(decoded,
# {
#       "constant": False,
#       "inputs": [
#         {
#           "internalType": "enum IExchange.TokenType[]",
#           "name": "_tokenTypes",
#           "type": "uint8[]"
#         },
#         {
#           "internalType": "address[]",
#           "name": "_tokenAddresses",
#           "type": "address[]"
#         },
#         {
#           "internalType": "uint256[]",
#           "name": "_tokenNumbers",
#           "type": "uint256[]"
#         },
#         {
#           "internalType": "uint256[]",
#           "name": "_startingPrices",
#           "type": "uint256[]"
#         },
#         {
#           "internalType": "uint256[]",
#           "name": "_endingPrices",
#           "type": "uint256[]"
#         },
#         {
#           "internalType": "contract IERC20[]",
#           "name": "_exchangeTokens",
#           "type": "address[]"
#         },
#         {
#           "internalType": "uint256[]",
#           "name": "_durations",
#           "type": "uint256[]"
#         }
#       ],
#       "name": "createAuction",
#       "outputs": [],
#       "stateMutability": "nonpayable",
#       "type": "function"
#     })

# import pprint
# pprint.pprint(decoded)
# exit(0)

# decoded = {
#         "param_0": 11403430,
#         "param_1": [
#             14474011154664526034909069339965131741826270611619044490146571011551995691272,
#             [
#                 1766959143701397048994461030926868181200402784820958467115499487534072066,
#                 0,
#                 "hah"
#             ]
#         ],
#         "_dlt_meta": {
#             "table_name": "AXIE_logs_AxieEvolved"
#         },
#         "_tx_blockNumber": 16039643,
#         "_tx_blockTimestamp": 1659723680,
#         "_tx_transactionHash": "\uf02a0x6302c20a4ef5426ee1da68fa3bdfb2c234a1f16fe441c51a35227ba4bd6746fa",
#         "_tx_transactionIndex": "0x3",
#         "_tx_address": "0x32950db2a7164aE833121501C797D79E7B79d74C",
#         "_tx_status": "0x1",
#         "_tx_logIndex": 15
#     }
# recode_tuples(decoded,
#     {
#       "name": "AxieEvolved",
#       "type": "event",
#       "inputs": [
#         {
#           "name": "param_0",
#           "type": "uint256",
#           "indexed": True
#         },
#         {
#           "components": [
#             {
#               "name": "param_1_1",
#               "type": "uint256"
#             },
#             {
#               "name": "param_1_2",
#               "type": "tuple",
#               "components": [
#                 {
#                     "name": "param_deep_i",
#                     "type": "uint256"
#                 },
#                 {
#                     "name": "param_deep_2",
#                     "type": "uint256"
#                 },
#                 {
#                     "name": "param_deep_3",
#                     "type": "string"
#                 }
#               ]
#             }
#           ],
#           "name": "param_1",
#           "type": "tuple",
#           "indexed": False
#         }
#       ],
#       "outputs": [],
#       "anonymous": False,
#       "_dlt_meta": {
#         "selector": "0xa006fbbbc9600fe3b3757442d103355696bba0d2b8f9201852984b64d72a0a0b",
#         "block": 16039643
#       }
#     }
# )

# import pprint
# pprint.pprint(decoded)
# exit(0)

credentials = GCPPipelineCredentials.from_services_file("_secrets/project1234_service.json", "ronin_4")
# credentials = PostgresPipelineCredentials("redshift", "chat_analytics_rasa", "mainnet_6", "loader", "3.73.90.3")

pipeline = Pipeline("ethereum")

# print(pipeline.root_path)

# get ethereum source which is python iterator, request 3 newest blocks with default lag. also pass the state (optional)
# so you can get newly produced blocks when the pipeline is run again
# i = get_source("https://mainnet.infura.io/v3/9aa3d95b3bc440fa88ea12eaa4456161", 2, state=pipeline.state)

# from web3 import Web3
# from web3._utils.abi import abi_to_signature
# from web3.contract import ContractFunction
# from examples.sources.eth_utils import signature_to_abi, decode_tx
# from pprint import pprint
# pprint(signature_to_abi("function", "AxieEvolved(uint256,(uint256,(uint256,string[]))[])"))
# w3 = Web3()
# pprint(decode_tx(w3, "0x70a88b27000000000000000000000000dd4ca6d2565aeabe0b469896141a23f9d3cb181100000000000000000000000000000000000000000000000000000000000000e000000000000000000000000000000000000000000000000000000000000011af00000000000000000000000000000000000000000000000000000000008467e7a03e101f2dec380e797fd683bae9f692d16aa07de900f720791539f474d8a4c8000000000000000000000000000000000000000000000000000001826f53c08000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000002000000000000000000000000a8754b9fa15fc18bb59458815510e40a12cd2014000000000000000000000000c99a6a985ed2cac1ef41640596c5a5f9f4e19ef5"))

# print(abi_to_signature({
#     "name": "f",
#     "type": "function",
#     "inputs": [
#       {
#         "name": "s",
#         "type": "tuple[]",
#         "components": [
#           {
#             "name": "a",
#             "type": "uint256"
#           },
#           {
#             "name": "b",
#             "type": "uint256[]"
#           },
#           {
#             "name": "c",
#             "type": "tuple[]",
#             "components": [
#               {
#                 "name": "x",
#                 "type": "uint256"
#               },
#               {
#                 "name": "y",
#                 "type": "uint256"
#               }
#             ]
#           }
#         ]
#       },
#       {
#         "name": "t",
#         "type": "tuple",
#         "components": [
#           {
#             "name": "x",
#             "type": "uint256"
#           },
#           {
#             "name": "y",
#             "type": "uint256"
#           }
#         ]
#       },
#       {
#         "name": "a",
#         "type": "uint256"
#       }
#     ],
#     "outputs": ["uint256"]
#   }))

# exit(0)

# "https://mainnet.infura.io/v3/9aa3d95b3bc440fa88ea12eaa4456161"
rpc_url = "https://api.roninchain.com/rpc"
working_dir = "experiments/pipeline/ronin"
abi_dir="experiments/data/ronin/abi"
# pipeline.create_pipeline(credentials, schema=discover_schema(), working_dir=working_dir)
# i = get_source(rpc_url, 4, abi_dir=abi_dir, is_poa=True, supports_batching=False, state=pipeline.state)

# you can transform the blocks by defining mapping function
def from_wei_to_eth(block: DictStrAny) -> DictStrAny:
    # convert wei to eth providing with numeric precision at the target
    with numeric_default_context():
        for tx in block["transactions"]:
            v_wei = tx["value"]
            if v_wei > 0:
                # 9 scale vs 18 digits ether scale: a no go for any financial operations
                # TODO: better support for decimal types
                tx["eth_value"] = numeric_default_quantize(Decimal(v_wei) / 10**18)
                # print(f"Produced nice decimal {tx['eth_value']}")
    return block


# extract data from the source. operation is atomic and like all atomic operations in pipeline, it does not raise but returns
# execution status
# map(from_wei_to_eth, i)
# pipeline.extract(i, table_name="blocks")
# print(pipeline.state)

# wait for ethereum network to produce some more blocks
# sleep(20)

# restore the pipeline from the working directory (simulate continuation from the saved state)
pipeline.restore_pipeline(credentials, working_dir)
# while True:
#     # obtain new iterator (the old one is expired), this time use deferred iterator to allow parallel block reading
#     i = get_source(rpc_url, 8, abi_dir=abi_dir, is_poa=True, supports_batching=False, state=pipeline.state)
#     pipeline.extract(i, table_name="blocks")
#     sleep(10)
# print(pipeline.state)

# this will unpack and load all extracted data
pipeline.unpack()
# pipeline.flush()
