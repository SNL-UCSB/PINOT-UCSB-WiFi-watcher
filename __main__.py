import asyncio
import json
import asyncpg

import aiohttp
import os
from typing import TypedDict, Optional

import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

SLEEP_TIME = int(os.environ.get('SLEEP_TIME', 3 * 60))

SALT_API_URL = os.environ['SALT_API_URL']
SALT_API_USER = os.environ['SALT_API_USER']
SALT_API_PASSWORD = os.environ['SALT_API_PASSWORD']

PG_HOST = os.environ['PG_HOST']
PG_USER = os.environ['PG_USER']
PG_PASSWORD = os.environ['PG_PASSWORD']
PG_DB = os.environ['PG_DB']

PING_TARGET = os.environ['PING_TARGET']
SALT_TARGET = os.environ.get('SALT_TARGET', '*')


class SaltResponse(TypedDict):
    ping: str
    packet_loss: float
    signal: float
    tx_bitrate: str
    rx_bitrate: str
    connected_time: str


COMMAND = rf"""echo "{{\"ping\": \"$(ping -c 5 -q {PING_TARGET})\", \"iw\": \"$(iw wlan0 station dump | grep -E 'signal|bitrate|connected')\"}}" """


def parse_salt_response(response: str) -> Optional[SaltResponse]:
    try:
        response = response.replace("\t", "").replace("\n", "\\n")
        response = json.loads(response)
        iw_output = response["iw"].split("\n")
        assert len(iw_output) == 4

        return {
            "ping": response["ping"].split("\n")[-1],
            "packet_loss": float(response["ping"].split("\n")[-2].split(",")[-2].strip().removesuffix("% packet loss").strip()),
            "signal": float(iw_output[0].split(":")[-1].strip().removesuffix("dBm").strip()),
            "tx_bitrate": iw_output[1].split(":")[-1].strip(),
            "rx_bitrate": iw_output[2].split(":")[-1].strip(),
            "connected_time": iw_output[3].split(":")[-1].strip(),
        }
    except Exception:
        return None


async def retrieve_data(client: aiohttp.ClientSession) -> dict[str, SaltResponse]:
    async with client.post(
            SALT_API_URL,
            json={
                "client": "local",
                "tgt": SALT_TARGET,
                "fun": "cmd.run",
                "arg": COMMAND,
                "username": SALT_API_USER,
                "password": SALT_API_PASSWORD,
                "eauth": "pam",
            },
    ) as response:
        response.raise_for_status()
        nodes = await response.json()
        nodes = nodes.get("return", [{}])[0]

    return {node: parse_salt_response(response) for node, response in nodes.items() if response is not None}


async def main():
    db_connection_pool = await asyncpg.create_pool(
        host=PG_HOST,
        user=PG_USER,
        password=PG_PASSWORD,
        database=PG_DB,
    )

    while True:
        try:
            async with aiohttp.ClientSession(headers={"Accept": "application/json"}) as client:
                data = await retrieve_data(client)
                logging.info(f"Retrieved data for {len(data)} nodes")

            async with db_connection_pool.acquire() as connection:
                for node, response in data.items():
                    if not response:
                        continue
                    await connection.execute(
                        f"INSERT INTO wifistats (label, ping, packet_loss, signal, tx_bitrate, rx_bitrate, connected_time, time) "
                        f"VALUES ($1, $2, $3, $4, $5, $6, $7, now())",
                        node,
                        response["ping"],
                        response["packet_loss"],
                        response["signal"],
                        response["tx_bitrate"],
                        response["rx_bitrate"],
                        response["connected_time"],
                    )
            logging.info("Updated information in the database")
        except Exception as e:
            logging.exception(e)

        logging.info(f"Sleeping for {SLEEP_TIME} seconds")
        await asyncio.sleep(SLEEP_TIME)

if __name__ == '__main__':
    asyncio.run(main())
