"""
Transaction client.

Submits a continuous stream of transactions to the cluster. Handles:
  * 421 redirects from non-leader/non-primary nodes.
  * Random target selection so we exercise the redirect path.
  * Periodic consistency checks across all nodes' ledgers.
"""

import asyncio
import os
import random
import time
from typing import Dict, List

from aiohttp import ClientSession, ClientTimeout


CLIENT_ID = os.environ.get("CLIENT_ID", "client1")
NODES_RAW = os.environ["NODES"]   # "node1:8000,node2:8000,..."
TX_INTERVAL = float(os.environ.get("TX_INTERVAL", "1.0"))
TX_TOTAL = int(os.environ.get("TX_TOTAL", "0"))  # 0 = infinite

NODES: Dict[str, str] = {}
for entry in NODES_RAW.split(","):
    name, _, addr = entry.partition(":")
    if ":" in addr:
        host, port = addr.split(":")
    else:
        host, port = name, addr
    NODES[name] = f"{host}:{port}"


async def submit(session: ClientSession, target: str, tx: dict, hops=0) -> bool:
    """Submit `tx`, following one redirect if needed."""
    if hops > 1:
        return False
    addr = NODES.get(target)
    if not addr:
        return False
    try:
        async with session.post(f"[{addr}](http://{addr}/submit)", json=tx) as r:
            data = await r.json()
            if data.get("ok"):
                print(f"[{CLIENT_ID}] OK    {tx['payload']} via {target}")
                return True
            if data.get("redirect"):
                return await submit(session, data["redirect"], tx, hops + 1)
            print(f"[{CLIENT_ID}] FAIL  {tx['payload']} via {target}: {data}")
            return False
    except Exception as e:
        print(f"[{CLIENT_ID}] ERROR via {target}: {e}")
        return False


async def check_consistency(session: ClientSession) -> None:
    """Fetch every node's ledger and report whether they agree."""
    lengths: List[int] = []
    fingerprints: List[str] = []
    for name, addr in NODES.items():
        try:
            async with session.get(f"[{addr}](http://{addr}/ledger)") as r:
                data = await r.json()
                entries = data["entries"]
                lengths.append(len(entries))
                fp = ",".join(e["transaction"].get("payload", "") for e in entries)
                fingerprints.append(fp)
                print(f"[consistency] {name}: {len(entries)} entries")
        except Exception as e:
            print(f"[consistency] {name}: UNREACHABLE ({e})")
    if fingerprints:
        agree = len(set(fingerprints)) == 1
        print(f"[consistency] ledgers agree: {agree}  lengths={lengths}")


async def main():
    timeout = ClientTimeout(total=8)
    async with ClientSession(timeout=timeout) as session:
        await asyncio.sleep(8)  # let cluster elect a leader

        seq = 0
        last_check = time.time()
        while TX_TOTAL == 0 or seq < TX_TOTAL:
            target = random.choice(list(NODES.keys()))
            tx = {
                "client_id": CLIENT_ID,
                "seq": seq,
                "payload": f"{CLIENT_ID}-tx-{seq}",
            }
            await submit(session, target, tx)
            seq += 1

            if time.time() - last_check > 10:
                await check_consistency(session)
                last_check = time.time()

            await asyncio.sleep(TX_INTERVAL)

        await check_consistency(session)


if __name__ == "__main__":
    asyncio.run(main())
