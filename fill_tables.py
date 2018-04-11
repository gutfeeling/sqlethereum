import datetime
import time

import argparse
import arrow
from sqlalchemy.sql import select
from web3 import Web3, HTTPProvider

def get_api(infura_api_endpoint):

    api = Web3(HTTPProvider(infura_api_endpoint))

    return api

def get_engine_and_metadata(database_url):

    engine = create_engine(database_url, echo = False)
    metadata = MetaData()
    metadata.reflect(bind = engine)

    return {"engine" : engine, "metadata" : metadata}

def address_belongs_to_smart_contract(api, address):

    code = api.getCode(address)

    if code == "0x":
        return False

    return True

def get_address_id(api, conn, address,  address_table):

    s = select(
        [address_table.c.id]
        ).where(
        address_table.c.hex == address
        )

    result = conn.execute(s)

    try:
        address_id = result.fetchone()[0]
    except TypeError:
        result = conn.execute(
            address_table.insert(),
            hex = address,
            type = address_belongs_to_smart_contract(api, address)
            )
        address_id = result.inserted_primary_key[0]

    return address_id

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--startblocknumber", default = 1)
    parser.add_argument("--endblocknumber", default = None)
    args = parser.parse_args()

    start_block_number = args.startblocknumber
    end_block_number = args.endblocknumber

    infura_api_endpoint = os.environ["INFURA_API_ENDPOINT"]
    api = get_api(infura_api_endpoint)

    database_url = os.environ["DATABASE_URL"]
    engine_and_metadata = get_engine_and_metadata(database_url)
    metadata = engine_and_metadata["metadata"]
    engine = engine_and_metadata["engine"]

    block_table = metadata.tables["block"]
    transaction_table = metadata.tables["transaction"]
    address_table = metadata.tables["address"]

    for block_number in range(start_block_number, end_block_number):
        time.sleep(1)

        block = api.eth.getBlock(
            block_identifier = block_number,
            full_transactions = True
            )
        timestamp = block["timestamp"]
        transactions = block["transactions"]

        with engine.begin() as conn:
            result = conn.execute(
                block_table.insert(),
                block_number = block_number,
                timestamp = arrow.get(timestamp)
                )
            block_id = result.inserted_primary_key[0]

            for transaction in transactions:
                from_address = transaction["from"]
                to_address = transaction["to"]
                amount = transaction["value"]
                gas_price = transaction["gasPrice"]
                gas = transaction["gas"]

                from_address_id = get_address_id(
                    api, conn, from_address, address_table
                    )
                to_address_id = get_address_id(
                    api, conn, to_address, address_table
                    )

                conn.execute(
                    transaction_table.insert(),
                    amount = amount,
                    gas_price = gas_price,
                    gas = gas,
                    from = from_address_id,
                    to = to_address_id,
                    block_id = block_id
                    )

        if block_number - start_block_number % 100 == 0:
            print(
                "[{0}] Downloaded till block number {1}".format(
                    datetime.datetime.now(), block_number
                    )
                )
