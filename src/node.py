"""
Main consensus daemon.

Implements:
  * Heartbeat-based leader election (Raft-style) with term numbers
    to prevent split-brain.
  * Basic Paxos for Mode A (crash-fault tolerance).
  * PBFT (Pre-prepare / Prepare / Commit) for Mode B (Byzantine-fault
    tolerance) with Ed25519 signatures on every message.
  * Append-only on-disk transaction ledger; entries are persisted ONLY
    after consensus is reached.

Cluster size N = 5:
  * Paxos: tolerates f = 2 crashes (majority quorum = 3).
  * PBFT : tolerates f = 1 Byzantine (needs 2f+1 = 3 prepares /
           2f+1 = 3 commits including self).
"""

import asyncio
import json
import logging
import os
import random
import time
from dataclasses import dataclass, asdict, field
from typing import Dict, List, Optional, Set

from aiohttp import web, ClientSession, ClientTimeout

from crypto_utils import generate_and_store_keys, KeyRing, canonical


# ---------------------------------------------------------------------------
# Configuration (from environment — populated by docker-compose)
# ---------------------------------------------------------------------------
NODE_ID = os.environ["NODE_ID"]                       # e.g. "node1"
MODE = os.environ.get("MODE", "PAXOS").upper()        # "PAXOS" or "PBFT"
PEERS_RAW = os.environ.get("PEERS", "")               # "node2:8000,node3:8000,..."
PORT = int(os.environ.get("PORT", "8000"))
DATA_DIR = os.environ.get("DATA_DIR", "/app/data")

PEERS: Dict[str, str] = {}
for entry in PEERS_RAW.split(","):
    entry = entry.strip()
    if not entry:
        continue
    name, _, addr = entry.partition(":")
    # addr is "host:port"; we already have the host as `name`. Use full URL form.
    if ":" in addr:
        host, port = addr.split(":")
    else:
        host, port = name, addr
    PEERS[name] = f"{host}:{port}"

ALL_NODE_IDS: List[str] = sorted([NODE_ID] + list(PEERS.keys()))
N = len(ALL_NODE_IDS)                                 # = 5 in this deployment
PAXOS_QUORUM = N // 2 + 1                             # = 3
PBFT_F = (N - 1) // 3                                 # = 1
PBFT_QUORUM = 2 * PBFT_F + 1                          # = 3

LEDGER_PATH = os.path.join(DATA_DIR, f"ledger_{NODE_ID}.json")

logging.basicConfig(
    level=logging.INFO,
    format=f"%(asctime)s [{NODE_ID}] %(levelname)s %(message)s",
)
log = logging.getLogger(NODE_ID)


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------
@dataclass
class Transaction:
    client_id: str
    seq: int          # client-supplied sequence number (for dedup)
    payload: str
    submitted_at: float

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class LedgerEntry:
    """A committed slot in the replicated log."""
    slot: int
    transaction: dict
    committed_at: float

    def to_dict(self) -> dict:
        return asdict(self)


# ---------------------------------------------------------------------------
# Persistent + in-memory state
# ---------------------------------------------------------------------------
class Ledger:
    """Append-only on-disk log. Loaded on startup, fsynced on every append."""

    def __init__(self, path: str):
        self.path = path
        self.entries: List[LedgerEntry] = []
        self._load()

    def _load(self) -> None:
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        if os.path.exists(self.path):
            with open(self.path, "r") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        self.entries.append(LedgerEntry(**json.loads(line)))
            log.info(f"Loaded {len(self.entries)} ledger entries from disk")

    def next_slot(self) -> int:
        return self.entries[-1].slot + 1 if self.entries else 0

    def has_slot(self, slot: int) -> bool:
        return any(e.slot == slot for e in self.entries)

    def append(self, slot: int, transaction: dict) -> None:
        if self.has_slot(slot):
            return  # idempotent
        entry = LedgerEntry(slot=slot, transaction=transaction,
                            committed_at=time.time())
        self.entries.append(entry)
        # Durable write: append a line and fsync.
        with open(self.path, "a") as f:
            f.write(json.dumps(entry.to_dict()) + "\n")
            f.flush()
            os.fsync(f.fileno())
        log.info(f"COMMITTED slot={slot} tx={transaction.get('payload')!r}")


# ---------------------------------------------------------------------------
# The node
# ---------------------------------------------------------------------------
class ConsensusNode:
    def __init__(self):
        # Crypto: generate our keys, then build the keyring.
        generate_and_store_keys(NODE_ID)
        self.keyring = KeyRing(NODE_ID)

        # Persistent ledger.
        self.ledger = Ledger(LEDGER_PATH)

        # Leader-election state (Raft-style).
        self.current_term: int = 0
        self.voted_for: Optional[str] = None
        self.leader_id: Optional[str] = None
        self.last_heartbeat: float = time.time()
        self.role: str = "FOLLOWER"   # FOLLOWER | CANDIDATE | LEADER
        self.election_timeout: float = self._new_election_timeout()

        # Paxos per-slot state.
        # paxos_acceptor[slot] = {"promised": n, "accepted_n": n, "accepted_v": v}
        self.paxos_acceptor: Dict[int, dict] = {}

        # PBFT per-(view, seq) state.
        self.pbft_view: int = 0       # initial primary = ALL_NODE_IDS[0]
        self.pbft_next_seq: int = 0
        self.pbft_log: Dict[int, dict] = {}  # seq -> {pre_prepare, prepares, commits, committed}

        # HTTP client session, created on startup.
        self.session: Optional[ClientSession] = None

        # Lock to serialise leader-driven proposals.
        self.propose_lock = asyncio.Lock()

    # -----------------------------------------------------------------------
    # Generic helpers
    # -----------------------------------------------------------------------
    def _new_election_timeout(self) -> float:
        # Randomised 1.5–3.0 s — wide spread prevents simultaneous candidacies.
        return random.uniform(1.5, 3.0)

    async def _post(self, peer_addr: str, path: str, payload: dict) -> Optional[dict]:
        """POST to a peer, swallowing network errors."""
        if self.session is None:
            return None
        url = f"[{peer_addr}{path}](http://{peer_addr}{path})"
        try:
            async with self.session.post(url, json=payload) as resp:
                if resp.status == 200:
                    return await resp.json()
        except Exception as e:
            log.debug(f"POST {url} failed: {e}")
        return None

    async def _broadcast(self, path: str, payload: dict) -> List[dict]:
        """Fan out to all peers in parallel; return successful responses."""
        tasks = [self._post(addr, path, payload) for addr in PEERS.values()]
        results = await asyncio.gather(*tasks)
        return [r for r in results if r is not None]

    # =======================================================================
    # 1. LEADER ELECTION (Raft-style heartbeats + term numbers)
    # =======================================================================
    #
    # Why Raft-style rather than Bully?
    #  * Term numbers give us a monotonic logical clock that PREVENTS
    #    split-brain: any node receiving a message with a higher term
    #    immediately steps down. Two leaders in the same term is impossible
    #    because winning requires a majority and a node only votes once
    #    per term.
    #  * Randomised election timeouts prevent repeated split votes.
    # =======================================================================

    async def _election_loop(self):
        """Continuously monitor leader liveness; start election on timeout."""
        while True:
            await asyncio.sleep(0.1)
            if self.role == "LEADER":
                continue
            if time.time() - self.last_heartbeat > self.election_timeout:
                await self._start_election()

    async def _start_election(self):
        self.current_term += 1
        self.role = "CANDIDATE"
        self.voted_for = NODE_ID
        self.election_timeout = self._new_election_timeout()
        self.last_heartbeat = time.time()
        term = self.current_term

        log.info(f"Starting election for term {term}")

        payload = {"term": term, "candidate_id": NODE_ID}
        votes = 1  # self-vote
        responses = await self._broadcast("/raft/request_vote", payload)
        for r in responses:
            # Stale-term check: if anyone returned a higher term, step down.
            if r.get("term", 0) > self.current_term:
                self._step_down(r["term"])
                return
            if r.get("vote_granted"):
                votes += 1

        # Confirm we're still a candidate in the same term.
        if self.role == "CANDIDATE" and self.current_term == term \
                and votes >= PAXOS_QUORUM:
            self.role = "LEADER"
            self.leader_id = NODE_ID
            log.info(f"WON election term={term} votes={votes}/{N}")
            # Immediately assert leadership.
            asyncio.create_task(self._heartbeat_loop())
        else:
            log.info(f"Election failed term={term} votes={votes}/{N}")

    async def _heartbeat_loop(self):
        """Leaders send heartbeats every 500 ms to suppress elections."""
        while self.role == "LEADER":
            payload = {
                "term": self.current_term,
                "leader_id": NODE_ID,
            }
            await self._broadcast("/raft/heartbeat", payload)
            await asyncio.sleep(0.5)

    def _step_down(self, new_term: int):
        if new_term > self.current_term:
            self.current_term = new_term
            self.voted_for = None
        self.role = "FOLLOWER"
        self.leader_id = None
        self.last_heartbeat = time.time()

    # -- HTTP handlers --

    async def handle_request_vote(self, request: web.Request) -> web.Response:
        body = await request.json()
        term = body["term"]
        candidate = body["candidate_id"]

        # Reject votes from stale terms.
        if term < self.current_term:
            return web.json_response(
                {"term": self.current_term, "vote_granted": False})

        # Newer term seen → step down before deciding the vote.
        if term > self.current_term:
            self._step_down(term)

        # Grant vote at most once per term.
        grant = self.voted_for in (None, candidate)
        if grant:
            self.voted_for = candidate
            self.last_heartbeat = time.time()  # don't immediately challenge
            log.info(f"Granted vote to {candidate} for term {term}")

        return web.json_response(
            {"term": self.current_term, "vote_granted": grant})

    async def handle_heartbeat(self, request: web.Request) -> web.Response:
        body = await request.json()
        term = body["term"]
        leader = body["leader_id"]

        if term < self.current_term:
            return web.json_response(
                {"term": self.current_term, "success": False})

        # Accept this leader.
        if term > self.current_term:
            self._step_down(term)
        self.role = "FOLLOWER"
        self.leader_id = leader
        self.last_heartbeat = time.time()
        return web.json_response({"term": self.current_term, "success": True})

    # =======================================================================
    # 2. PAXOS (per-slot Basic Paxos, leader = distinguished proposer)
    # =======================================================================
    #
    # The leader chooses the next free slot and runs Paxos for it:
    #   Phase 1: Prepare(n)            ──▶ acceptors
    #            Promise(n, accepted)  ◀── acceptors
    #   Phase 2: Accept(n, v)          ──▶ acceptors
    #            Accepted(n)           ◀── acceptors
    # On majority, the value is decided and appended to the ledger.
    #
    # Safety property maintained: if any acceptor in the prepare quorum
    # reports an already-accepted value, the proposer MUST adopt the value
    # with the highest accepted proposal number — this is what prevents
    # two different values being decided for the same slot.
    # =======================================================================

    def _next_proposal_number(self) -> int:
        """
        Globally unique, monotonically increasing proposal numbers.
        Encoding: (round << 8) | node_index — guarantees uniqueness across
        nodes even if two leaders propose concurrently.
        """
        my_idx = ALL_NODE_IDS.index(NODE_ID)
        round_num = int(time.time() * 1000)
        return (round_num << 8) | my_idx

    async def paxos_propose(self, transaction: dict) -> bool:
        async with self.propose_lock:
            slot = self.ledger.next_slot()
            n = self._next_proposal_number()
            log.info(f"PAXOS propose slot={slot} n={n}")

            # ---- Phase 1: PREPARE ----
            prepare_payload = {"slot": slot, "n": n}
            promises = [await self._self_prepare(slot, n)]
            promises += await self._broadcast("/paxos/prepare", prepare_payload)
            promises = [p for p in promises if p and p.get("promised")]

            if len(promises) < PAXOS_QUORUM:
                log.warning(f"PAXOS prepare failed for slot={slot}: "
                            f"{len(promises)}/{PAXOS_QUORUM} promises")
                return False

            # Safety: adopt highest previously-accepted value, if any.
            value_to_propose = transaction
            highest_n = -1
            for p in promises:
                if p.get("accepted_n", -1) > highest_n and p.get("accepted_v"):
                    highest_n = p["accepted_n"]
                    value_to_propose = p["accepted_v"]
            if value_to_propose is not transaction:
                log.info(f"PAXOS adopting earlier accepted value for slot={slot}")

            # ---- Phase 2: ACCEPT ----
            accept_payload = {"slot": slot, "n": n, "value": value_to_propose}
            accepts = [await self._self_accept(slot, n, value_to_propose)]
            accepts += await self._broadcast("/paxos/accept", accept_payload)
            accepts = [a for a in accepts if a and a.get("accepted")]

            if len(accepts) < PAXOS_QUORUM:
                log.warning(f"PAXOS accept failed for slot={slot}: "
                            f"{len(accepts)}/{PAXOS_QUORUM} accepts")
                return False

            # ---- Decided! Commit locally and tell peers to commit. ----
            self.ledger.append(slot, value_to_propose)
            asyncio.create_task(self._broadcast("/paxos/commit", {
                "slot": slot, "value": value_to_propose,
            }))
            return value_to_propose is transaction  # True iff OUR tx was chosen

    async def _self_prepare(self, slot: int, n: int) -> dict:
        """The leader acts as an acceptor too."""
        state = self.paxos_acceptor.setdefault(
            slot, {"promised": -1, "accepted_n": -1, "accepted_v": None})
        if n > state["promised"]:
            state["promised"] = n
            return {"promised": True,
                    "accepted_n": state["accepted_n"],
                    "accepted_v": state["accepted_v"]}
        return {"promised": False}

    async def _self_accept(self, slot: int, n: int, value: dict) -> dict:
        state = self.paxos_acceptor.setdefault(
            slot, {"promised": -1, "accepted_n": -1, "accepted_v": None})
        if n >= state["promised"]:
            state["promised"] = n
            state["accepted_n"] = n
            state["accepted_v"] = value
            return {"accepted": True}
        return {"accepted": False}

    # -- HTTP handlers (acceptor role) --

    async def handle_paxos_prepare(self, request: web.Request) -> web.Response:
        body = await request.json()
        slot, n = body["slot"], body["n"]
        result = await self._self_prepare(slot, n)
        return web.json_response(result)

    async def handle_paxos_accept(self, request: web.Request) -> web.Response:
        body = await request.json()
        result = await self._self_accept(body["slot"], body["n"], body["value"])
        return web.json_response(result)

    async def handle_paxos_commit(self, request: web.Request) -> web.Response:
        """
        The leader tells learners about a decided slot. We append to the
        ledger only if we haven't already.
        """
        body = await request.json()
        self.ledger.append(body["slot"], body["value"])
        return web.json_response({"ok": True})

    # =======================================================================
    # 3. PBFT (Pre-prepare → Prepare → Commit)
    # =======================================================================
    #
    # Every message is signed with the sender's Ed25519 key. Receivers
    # verify the signature and reject anything that fails — this is how
    # PBFT defeats forgery and equivocation attempts by Byzantine nodes.
    #
    # Quorums (N=5, f=1):
    #   * 2f = 2 matching prepare messages from OTHERS → "prepared"
    #   * 2f+1 = 3 matching commit  messages (including self) → "committed"
    # =======================================================================

    def _pbft_primary(self) -> str:
        return ALL_NODE_IDS[self.pbft_view % N]

    def _pbft_is_primary(self) -> bool:
        return self._pbft_primary() == NODE_ID

    def _pbft_slot(self, seq: int) -> dict:
        """Lazy-init per-sequence state record."""
        return self.pbft_log.setdefault(seq, {
            "pre_prepare": None,
            "prepares": {},   # sender_id -> digest
            "commits": {},    # sender_id -> digest
            "prepared": False,
            "committed": False,
        })

    async def pbft_propose(self, transaction: dict) -> bool:
        if not self._pbft_is_primary():
            return False
        seq = self.pbft_next_seq
        self.pbft_next_seq += 1
        view = self.pbft_view
        digest = self._digest(transaction)

        # Build, sign, and broadcast PRE-PREPARE.
        payload = {
            "type": "pre-prepare",
            "view": view, "seq": seq, "digest": digest,
            "request": transaction, "sender": NODE_ID,
        }
        payload["signature"] = self.keyring.sign(self._unsigned(payload))

        # Record locally first, then broadcast.
        slot = self._pbft_slot(seq)
        slot["pre_prepare"] = payload
        # Primary implicitly prepares its own pre-prepare.
        slot["prepares"][NODE_ID] = digest

        await self._broadcast("/pbft/pre_prepare", payload)
        # Also send our own PREPARE so backups can collect 2f from non-primaries.
        await self._send_prepare(view, seq, digest)
        return True

    async def _send_prepare(self, view: int, seq: int, digest: str):
        payload = {
            "type": "prepare",
            "view": view, "seq": seq, "digest": digest,
            "sender": NODE_ID,
        }
        payload["signature"] = self.keyring.sign(self._unsigned(payload))
        await self._broadcast("/pbft/prepare", payload)

    async def _send_commit(self, view: int, seq: int, digest: str):
        payload = {
            "type": "commit",
            "view": view, "seq": seq, "digest": digest,
            "sender": NODE_ID,
        }
        payload["signature"] = self.keyring.sign(self._unsigned(payload))
        # Record our own commit immediately.
        self._pbft_slot(seq)["commits"][NODE_ID] = digest
        await self._broadcast("/pbft/commit", payload)

    @staticmethod
    def _unsigned(msg: dict) -> dict:
        return {k: v for k, v in msg.items() if k != "signature"}

    @staticmethod
    def _digest(transaction: dict) -> str:
        import hashlib
        return hashlib.sha256(canonical(transaction)).hexdigest()

    def _verify(self, body: dict) -> bool:
        """Verify a signed PBFT message. False on any failure."""
        sender = body.get("sender")
        sig = body.get("signature")
        if not sender or not sig:
            return False
        return self.keyring.verify(sender, self._unsigned(body), sig)

    # -- HTTP handlers --

    async def handle_pbft_pre_prepare(self, request: web.Request) -> web.Response:
        body = await request.json()
        if not self._verify(body):
            log.warning("PBFT pre-prepare: bad signature, rejecting")
            return web.json_response({"ok": False, "reason": "bad_sig"})

        view, seq, digest = body["view"], body["seq"], body["digest"]

        # Must come from the current view's primary.
        if body["sender"] != self._pbft_primary() or view != self.pbft_view:
            log.warning("PBFT pre-prepare: not from primary or wrong view")
            return web.json_response({"ok": False, "reason": "not_primary"})

        # Recompute the digest from the request to detect equivocation
        # where the primary signs a digest that doesn't match the payload.
        if digest != self._digest(body["request"]):
            log.warning("PBFT pre-prepare: digest mismatch — primary lying")
            return web.json_response({"ok": False, "reason": "digest_mismatch"})

        slot = self._pbft_slot(seq)
        # If we already accepted a DIFFERENT pre-prepare for (view, seq),
        # the primary is equivocating — refuse.
        if slot["pre_prepare"] and slot["pre_prepare"]["digest"] != digest:
            log.warning(f"PBFT pre-prepare equivocation detected at seq={seq}")
            return web.json_response({"ok": False, "reason": "equivocation"})

        slot["pre_prepare"] = body
        # Broadcast our PREPARE.
        await self._send_prepare(view, seq, digest)
        return web.json_response({"ok": True})

    async def handle_pbft_prepare(self, request: web.Request) -> web.Response:
        body = await request.json()
        if not self._verify(body):
            return web.json_response({"ok": False, "reason": "bad_sig"})

        view, seq, digest, sender = (
            body["view"], body["seq"], body["digest"], body["sender"])
        slot = self._pbft_slot(seq)
        slot["prepares"][sender] = digest

        # "Prepared" iff we have a matching pre-prepare AND 2f prepares
        # with the matching digest from OTHER nodes (≠ ourselves).
        if (slot["pre_prepare"]
                and slot["pre_prepare"]["digest"] == digest
                and not slot["prepared"]):
            matching = sum(1 for s, d in slot["prepares"].items()
                           if d == digest and s != NODE_ID)
            if matching >= 2 * PBFT_F:
                slot["prepared"] = True
                log.info(f"PBFT prepared seq={seq}")
                await self._send_commit(view, seq, digest)

        await self._maybe_commit(seq)
        return web.json_response({"ok": True})

    async def handle_pbft_commit(self, request: web.Request) -> web.Response:
        body = await request.json()
        if not self._verify(body):
            return web.json_response({"ok": False, "reason": "bad_sig"})

        seq, digest, sender = body["seq"], body["digest"], body["sender"]
        slot = self._pbft_slot(seq)
        slot["commits"][sender] = digest
        await self._maybe_commit(seq)
        return web.json_response({"ok": True})

    async def _maybe_commit(self, seq: int):
        """If 2f+1 matching commits, append the request to the ledger."""
        slot = self._pbft_slot(seq)
        if slot["committed"] or not slot["pre_prepare"]:
            return
        digest = slot["pre_prepare"]["digest"]
        matching = sum(1 for d in slot["commits"].values() if d == digest)
        if matching >= PBFT_QUORUM:
            slot["committed"] = True
            self.ledger.append(seq, slot["pre_prepare"]["request"])

    # =======================================================================
    # Client-facing & introspection endpoints
    # =======================================================================

    async def handle_submit(self, request: web.Request) -> web.Response:
        """
        Client transaction endpoint.
        In PAXOS mode: only the leader proposes; followers redirect.
        In PBFT  mode: only the primary proposes; followers redirect.
        """
        body = await request.json()
        tx = {
            "client_id": body.get("client_id", "anon"),
            "seq": body.get("seq", 0),
            "payload": body.get("payload", ""),
            "submitted_at": time.time(),
        }

        if MODE == "PAXOS":
            if self.role != "LEADER":
                return web.json_response(
                    {"ok": False, "redirect": self.leader_id}, status=421)
            ok = await self.paxos_propose(tx)
            return web.json_response({"ok": ok, "mode": "PAXOS"})

        # PBFT
        if not self._pbft_is_primary():
            return web.json_response(
                {"ok": False, "redirect": self._pbft_primary()}, status=421)
        ok = await self.pbft_propose(tx)
        # PBFT commit is async — wait briefly to confirm.
        for _ in range(20):
            await asyncio.sleep(0.1)
            if self.ledger.has_slot(self.pbft_next_seq - 1):
                return web.json_response({"ok": True, "mode": "PBFT"})
        return web.json_response({"ok": False, "mode": "PBFT",
                                  "reason": "no_quorum"})

    async def handle_status(self, request: web.Request) -> web.Response:
        return web.json_response({
            "node_id": NODE_ID, "mode": MODE, "role": self.role,
            "term": self.current_term, "leader": self.leader_id,
            "pbft_view": self.pbft_view, "pbft_primary": self._pbft_primary(),
            "ledger_length": len(self.ledger.entries),
        })

    async def handle_ledger(self, request: web.Request) -> web.Response:
        return web.json_response({
            "node_id": NODE_ID,
            "length": len(self.ledger.entries),
            "entries": [e.to_dict() for e in self.ledger.entries],
        })

    # -----------------------------------------------------------------------
    # Lifecycle
    # -----------------------------------------------------------------------
    async def on_startup(self, app: web.Application):
        timeout = ClientTimeout(total=4)
        self.session = ClientSession(timeout=timeout)
        # Give peers time to write their public keys to the shared volume.
        await asyncio.sleep(2)
        asyncio.create_task(self._election_loop())
        log.info(f"Started: mode={MODE} N={N} peers={list(PEERS)} "
                 f"PAXOS_QUORUM={PAXOS_QUORUM} PBFT_F={PBFT_F}")

    async def on_cleanup(self, app: web.Application):
        if self.session:
            await self.session.close()


def build_app() -> web.Application:
    node = ConsensusNode()
    app = web.Application()
    app.on_startup.append(node.on_startup)
    app.on_cleanup.append(node.on_cleanup)

    # Raft-style leader election
    app.router.add_post("/raft/request_vote", node.handle_request_vote)
    app.router.add_post("/raft/heartbeat",    node.handle_heartbeat)

    # Paxos
    app.router.add_post("/paxos/prepare", node.handle_paxos_prepare)
    app.router.add_post("/paxos/accept",  node.handle_paxos_accept)
    app.router.add_post("/paxos/commit",  node.handle_paxos_commit)

    # PBFT
    app.router.add_post("/pbft/pre_prepare", node.handle_pbft_pre_prepare)
    app.router.add_post("/pbft/prepare",     node.handle_pbft_prepare)
    app.router.add_post("/pbft/commit",      node.handle_pbft_commit)

    # Client + introspection
    app.router.add_post("/submit",  node.handle_submit)
    app.router.add_get ("/status",  node.handle_status)
    app.router.add_get ("/ledger",  node.handle_ledger)
    return app


if __name__ == "__main__":
    web.run_app(build_app(), host="0.0.0.0", port=PORT)
