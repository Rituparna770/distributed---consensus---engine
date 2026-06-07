"""
Byzantine adversary node.

This node speaks the same wire protocol as a normal `ConsensusNode` but
intentionally violates PBFT in ways the assignment calls out explicitly:

  1. EQUIVOCATION: when it's the primary, it sends DIFFERENT pre-prepare
     requests (different digests) to different halves of the cluster.
  2. EQUIVOCATION (backup): it broadcasts PREPARE messages with mismatched
     digests to different peers.
  3. SUPPRESSION: it silently drops COMMIT messages it should send.
  4. FORGERY ATTEMPT: it tries to send a pre-prepare claiming to be
     another node (which honest replicas reject because the signature
     doesn't match the claimed sender's public key).

It still produces valid signatures on its own messages — the whole point
is that PBFT must tolerate it even though it's playing dirty.
"""

import asyncio
import os
import random
import time

from aiohttp import web

from node import ConsensusNode, build_app, NODE_ID, ALL_NODE_IDS, PEERS, log
import node as node_module


BEHAVIOUR = os.environ.get("BYZANTINE_BEHAVIOUR", "all").lower()
# Options: "equivocate" | "suppress" | "forge" | "all"

ATTACK_PROB = float(os.environ.get("ATTACK_PROBABILITY", "0.8"))


class ByzantineNode(ConsensusNode):

    def _should_attack(self) -> bool:
        return random.random() < ATTACK_PROB

    # ----- 1. Equivocating primary -----
    async def pbft_propose(self, transaction: dict) -> bool:
        if not self._pbft_is_primary() or BEHAVIOUR not in ("equivocate", "all") \
                or not self._should_attack():
            return await super().pbft_propose(transaction)

        seq = self.pbft_next_seq
        self.pbft_next_seq += 1
        view = self.pbft_view

        # Real transaction for half the cluster, fake for the other half.
        real_tx = transaction
        fake_tx = {**transaction, "payload": "ADVERSARY_FORGED_VALUE"}

        log.warning(f"ADVERSARY equivocating at seq={seq}: "
                    f"sending different pre-prepares to peers")

        peers = list(PEERS.items())
        random.shuffle(peers)
        half = len(peers) // 2

        for i, (peer_id, peer_addr) in enumerate(peers):
            tx = real_tx if i < half else fake_tx
            digest = self._digest(tx)
            msg = {
                "type": "pre-prepare",
                "view": view, "seq": seq, "digest": digest,
                "request": tx, "sender": NODE_ID,
            }
            msg["signature"] = self.keyring.sign(self._unsigned(msg))
            asyncio.create_task(self._post(peer_addr, "/pbft/pre_prepare", msg))
        return False  # we don't expect this to commit

    # ----- 2. Equivocating prepare/commit -----
    async def _send_prepare(self, view, seq, digest):
        if BEHAVIOUR in ("equivocate", "all") and self._should_attack():
            log.warning(f"ADVERSARY sending mismatched PREPAREs at seq={seq}")
            peers = list(PEERS.items())
            for i, (peer_id, peer_addr) in enumerate(peers):
                bad_digest = digest if i % 2 == 0 else "0" * 64
                msg = {
                    "type": "prepare", "view": view, "seq": seq,
                    "digest": bad_digest, "sender": NODE_ID,
                }
                msg["signature"] = self.keyring.sign(self._unsigned(msg))
                asyncio.create_task(self._post(peer_addr, "/pbft/prepare", msg))
            return
        await super()._send_prepare(view, seq, digest)

    # ----- 3. Suppressing commits -----
    async def _send_commit(self, view, seq, digest):
        if BEHAVIOUR in ("suppress", "all") and self._should_attack():
            log.warning(f"ADVERSARY SUPPRESSING commit at seq={seq}")
            # Record locally but never broadcast.
            self._pbft_slot(seq)["commits"][NODE_ID] = digest
            return
        await super()._send_commit(view, seq, digest)

    # ----- 4. Forged-identity attempt -----
    async def _forgery_attempt(self):
        """Periodically attempt to spoof another node's identity."""
        await asyncio.sleep(15)
        victim = next((n for n in ALL_NODE_IDS if n != NODE_ID), None)
        if not victim:
            return
        msg = {
            "type": "pre-prepare", "view": 0, "seq": 999_999,
            "digest": "deadbeef" * 8,
            "request": {"client_id": "x", "seq": 0,
                        "payload": "FORGED", "submitted_at": time.time()},
            "sender": victim,  # claim we're the victim!
        }
        # Sign with OUR key — but claim to be `victim`. Honest replicas
        # will look up `victim`'s public key and reject this.
        msg["signature"] = self.keyring.sign(self._unsigned(msg))
        log.warning(f"ADVERSARY forging identity of {victim}")
        for addr in PEERS.values():
            asyncio.create_task(self._post(addr, "/pbft/pre_prepare", msg))

    async def on_startup(self, app):
        await super().on_startup(app)
        if BEHAVIOUR in ("forge", "all"):
            asyncio.create_task(self._forgery_attempt())


# Replace the node class used by build_app() with our Byzantine subclass.
def build_byzantine_app() -> web.Application:
    node_module.ConsensusNode = ByzantineNode  # type: ignore
    return build_app()


if __name__ == "__main__":
    web.run_app(build_byzantine_app(), host="0.0.0.0",
                port=int(os.environ.get("PORT", "8000")))
