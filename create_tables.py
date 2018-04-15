import os

from sqlalchemy import (create_engine, MetaData, Table, Column,
                        Integer, String, DateTime, Boolean, ForeignKey,
                        BigInteger, Float
                        )

if __name__ == "__main__":

    DATABASE_URL = os.environ["DATABASE_URL"]

    engine = create_engine(DATABASE_URL, echo = False)
    metadata = MetaData()
    metadata.reflect(bind = engine)

    block_table = Table("block", metadata,
        # id is the block number
        Column("id", Integer, primary_key=True),
        Column("timestamp", DateTime)
        )

    transaction_table = Table("transaction", metadata,
        Column("id", Integer, primary_key=True),
        # amount is stored in Eth, not Wei
        Column("amount", Float),
        Column("gas_price", BigInteger),
        Column("gas", Integer),
        Column(
            "from_address",
            Integer,
            ForeignKey("address.id", ondelete = "CASCADE"),
            nullable=False,
            ),
        Column(
            "to_address",
            Integer,
            ForeignKey("address.id", ondelete = "CASCADE"),
            # to address may be NULL - represents contract creation
            nullable=True,
            ),
        Column(
            "block_id",
            Integer,
            ForeignKey("block.id", ondelete = "CASCADE"),
            nullable=False,
            )
        )

    address_table = Table("address", metadata,
        Column("id", Integer, primary_key=True),
        Column("hex", String(length = 42), unique = True, index = True),
        # 0 means normal, 1 means smart contract
        Column("type", Boolean),
        )

    metadata.create_all(engine)
