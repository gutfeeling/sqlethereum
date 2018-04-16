import datetime
import os
import time

import argparse
import arrow
from sqlalchemy import create_engine, MetaData
from sqlalchemy.sql import select
from web3.auto import w3
from web3.utils.threads import Timeout

def get_engine_and_metadata(database_url):

    engine = create_engine(database_url, echo = False)
    metadata = MetaData()
    metadata.reflect(bind = engine)

    return {"engine" : engine, "metadata" : metadata}

def address_belongs_to_smart_contract(address):

    # Keep retrying forever on timeout
    while True:
        try:
            code = w3.eth.getCode(address)
            break
        except Timeout:
            pass

    if code == "0x":
        return False

    return True

def get_address_id(conn, address, address_table):

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
            type = address_belongs_to_smart_contract(address)
            )
        address_id = result.inserted_primary_key[0]

    return address_id

def insert_blocks_and_transactions(conn, block_table, transaction_table,
                                   block_list, transaction_list
                                   ):

    conn.execute(
        block_table.insert(), block_list
        )

    if not len(transaction_list) == 0:
        conn.execute(
            transaction_table.insert(), transaction_list
        )


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--startblocknumber", type = int, default = 1)
    parser.add_argument("--endblocknumber", type = int, default = None)
    args = parser.parse_args()

    start_block_number = args.startblocknumber
    end_block_number = args.endblocknumber

    database_url = os.environ["DATABASE_URL"]
    engine_and_metadata = get_engine_and_metadata(database_url)
    metadata = engine_and_metadata["metadata"]
    engine = engine_and_metadata["engine"]

    block_table = metadata.tables["block"]
    transaction_table = metadata.tables["transaction"]
    address_table = metadata.tables["address"]

    block_list = []
    transaction_list = []

    conn = engine.connect()

    for block_number in range(start_block_number, end_block_number):
        with conn.begin() as trans:
            try:
                # Keep retrying forever on timeout
                while True:
                    try:
                        block = w3.eth.getBlock(
                            block_identifier = block_number,
                            full_transactions = True
                            )
                        break
                    except Timeout:
                        pass


                timestamp = block["timestamp"]
                timestamp_datetime = arrow.get(timestamp).datetime
                transactions = block["transactions"]


                block_list.append(
                    {
                        "id" : block_number,
                        "timestamp" : timestamp_datetime
                        }
                    )

                for transaction in transactions:
                    from_address = transaction["from"]
                    to_address = transaction["to"]

                    amount = transaction["value"]*1e-18
                    gas_price = transaction["gasPrice"]
                    gas = transaction["gas"]

                    from_address_id = get_address_id(
                        conn, from_address, address_table
                        )
                    if to_address is None:
                        to_address_id = None
                    else:
                        to_address_id = get_address_id(
                            conn, to_address, address_table
                            )

                    transaction_list.append(
                        {
                            "amount" : amount,
                            "gas_price" : gas_price,
                            "gas" : gas,
                            "from_address" : from_address_id,
                            "to_address" : to_address_id,
                            "block_id" : block_number,
                            }
                        )

                reached_end_block = False
                if block_number == end_block_number - 1:
                    reached_end_block = True

                if ((block_number - start_block_number) % 1000 == 0 or
                        reached_end_block):
                    insert_blocks_and_transactions(
                        conn, block_table, transaction_table,
                        block_list, transaction_list
                        )
                    block_list = []
                    transaction_list = []
                    print(
                        "[{0}] Downloaded till block number {1}".format(
                            datetime.datetime.now(), block_number
                            )
                        )

                trans.commit()
            except:
                trans.rollback()
                raise
