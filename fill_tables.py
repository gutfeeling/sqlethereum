import time

import argparse
from web3 import Web3, HTTPProvider

def get_api(infura_api_endpoint):

    api = Web3(HTTPProvider(infura_api_endpoint))

    return api

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--startblocknumber", default = 1)
    parser.add_argument("--endblocknumber", default = None)
    args = parser.parse_args()

    start_block_number = args.startblocknumber
    end_block_number = args.endblocknumber

    for block_number in range(start_block_number, end_block_number):
        time.sleep(1)
        block = api.eth.getBlock(
            block_identifier = block_number,
            full_transactions = True
            )
        transactions = block["transactions"]
