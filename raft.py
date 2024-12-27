import random
import sys
import time
import asyncio
from asyncio import Lock
from contextlib import asynccontextmanager
from enum import Enum
from statistics import median_low
from typing import Any

from fastapi import FastAPI, Request
import httpx
from fastapi import HTTPException
from pydantic import BaseModel
import uvicorn


class VoteRequest(BaseModel):
    term: int
    candidate_id: int


class Entry(BaseModel):
    key: str
    value: Any
    term: int


class AppendEntriesRequest(BaseModel):
    leader_id: int
    term: int
    prev_log_index: int
    prev_log_term: int
    entries: list[Entry]
    leader_commit: int


class AppendEntriesResponse(BaseModel):
    success: bool
    term: int
    last_index: int


class KeyValue(BaseModel):
    key: str
    value: Any


class ListKeyValue(BaseModel):
    store: dict[str, Any]


class VoteResponse(BaseModel):
    term: int
    vote_granted: bool


class LockRequest(BaseModel):
    key: str
    current_value: str
    next_value: str
    version: int
    ttl: int


class UnlockRequest(BaseModel):
    key: str
    current_value: str
    version: int


class LockEntry(BaseModel):
    value: str
    version: int
    end_of_ttl: float | None = None


class NodeState(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


class IsLocked(BaseModel):
    is_locked: bool
    value: str
    remain: float


class Node:
    def __init__(self, port,  id, all_nodes):
        self.app = FastAPI(lifespan=self.lifespan)
        self.port = port
        self.id = int(id)
        self.state = NodeState.FOLLOWER
        self.current_term = 0
        self.voted_for = None
        self.election_timeout_time = lambda: random.uniform(1.5, 1.7)
        self.heartbeat_received = False
        self.votes = 0
        self.all_nodes = all_nodes
        self.log = [Entry(key="init", value="0", term=0)]
        self.data_store = {"lock_keys": []}
        self.next_index = {}
        self.match_index = {}
        self.last_applied = 0
        self.commit_index = 0
        self.leader_id = None

        @self.app.post("/request_vote")
        async def request_vote(vote_request: VoteRequest):
            return await self.request_vote_handle(vote_request)

        @self.app.post("/")
        async def add_key_value(request: KeyValue):
            key = request.key
            value = request.value

            if self.state != NodeState.LEADER:
                raise HTTPException(status_code=403, detail="Только лидер может добавлять данные")
            entry = Entry(key=key, value=value, term=self.current_term)
            self.log.append(entry)
            last_log_index = len(self.log) - 1

            await self.wait_apply(last_log_index)

            return KeyValue(key=key, value=value)

        @self.app.post("/replicate")
        async def replicate_data(request: AppendEntriesRequest):
            return await self.received_entries(request)

        @self.app.get("/{key}")
        async def get_key(key: str):
            if key not in self.data_store:
                raise HTTPException(status_code=404, detail="Ключ не найден")
            return KeyValue(key=key, value=self.data_store[key])

        @self.app.get("/")
        async def get():
            return ListKeyValue(store=self.data_store)

        @self.app.post("/lock")
        async def lock(lock_request: LockRequest):
            async with Lock():
                if self.state != NodeState.LEADER:
                    raise HTTPException(status_code=403, detail="Только лидер может обрабатывать блокировку")

                if lock_request.key not in self.data_store:
                    if len(lock_request.current_value) == 0:
                        lock_keys = self.data_store["lock_keys"]
                        if lock_request.key not in lock_keys:
                            lock_keys.append(lock_request.key)
                        entry = Entry(key="lock_keys", value=lock_keys, term=self.current_term)
                        self.log.append(entry)

                        lock_entry = LockEntry(value=lock_request.next_value, version=lock_request.version, end_of_ttl=time.time() + lock_request.ttl)
                        entry = Entry(key=lock_request.key, value=lock_entry.model_dump(), term=self.current_term)
                        self.log.append(entry)
                        last_log_index = len(self.log) - 1

                        await self.wait_apply(last_log_index)

                        return True
                    raise HTTPException(status_code=400, detail="Неверное значение для current value")

                found = self.data_store[lock_request.key]
                if found['value'] == lock_request.current_value and found['version'] == lock_request.version:
                    lock_entry = LockEntry(value=lock_request.next_value, version=lock_request.version, end_of_ttl=time.time() + lock_request.ttl)
                    entry = Entry(key=lock_request.key, value=lock_entry.model_dump(), term=self.current_term)
                    self.log.append(entry)
                    last_log_index = len(self.log) - 1

                    await self.wait_apply(last_log_index)

                    return True
                raise HTTPException(status_code=400, detail="Неверное значение для current value или version")

        @self.app.post("/unlock")
        async def unlock(unlock_request: UnlockRequest):
            async with Lock():
                if self.state != NodeState.LEADER:
                    raise HTTPException(status_code=403, detail="Только лидер может обрабатывать блокировку")

                if unlock_request.key not in self.data_store:
                    raise HTTPException(status_code=400, detail="Нет такого ключа в хранилище")

                found = self.data_store[unlock_request.key]
                if found['end_of_ttl'] is None:
                    raise HTTPException(status_code=400, detail="Блокировка уже снята")

                if found['value'] == unlock_request.current_value and found['version'] == unlock_request.version:
                    lock_entry = LockEntry(value="", version=unlock_request.version, end_of_ttl=None)
                    entry = Entry(key=unlock_request.key, value=lock_entry.model_dump(), term=self.current_term)
                    self.log.append(entry)
                    last_log_index = len(self.log) - 1

                    await self.wait_apply(last_log_index)

                    return True
                raise HTTPException(status_code=400, detail="Неверное значение для current value или version")

        @self.app.get("/is-locked/{key}")
        async def is_locked(key: str):
            if key not in self.data_store:
                raise HTTPException(status_code=400, detail="Нет такого ключа в хранилище")

            found = self.data_store[key]
            if found['end_of_ttl'] is None:
                return IsLocked(is_locked=False, remain=0.0, value=found['value'])
            #if found['end_of_ttl'] < time.time():
            #    return IsLocked(is_locked=False, remain=0.0, value=found['value'])
            else:
                return IsLocked(is_locked=True, remain=found['end_of_ttl'] - time.time(), value=found['value'])

    async def update_ttl(self):
        while self.state == NodeState.LEADER:
            for key in self.data_store["lock_keys"]:
                if key not in self.data_store:
                    continue
                lock_entry = self.data_store['key']
                if lock_entry['end_of_ttl'] is None:
                    continue
                if lock_entry['end_of_ttl'] < time.time():
                    lock_entry = LockEntry(value="", version=lock_entry['version'], end_of_ttl=None)
                    entry = Entry(key=key, value=lock_entry.model_dump(), term=self.current_term)
                    self.log.append(entry)

            last_log_index = len(self.log) - 1

            await self.wait_apply(last_log_index)

            await asyncio.sleep(1)

    async def wait_apply(self, last_log_index: int):
        while self.last_applied < last_log_index:
            await asyncio.sleep(1)

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        self.client = httpx.AsyncClient()
        self.task = asyncio.create_task(self.election_timeout())
        try:
            yield
        finally:
            self.task.cancel()
            await self.client.aclose()

    async def send_entries(self):
        for node in self.all_nodes:
            if node.endswith(str(self.id)):
                continue
            prev_log_index = min(len(self.log) - 1, self.next_index[node] - 1)
            entries = self.log[self.next_index[node]:]
            prev_log_term = self.log[prev_log_index].term

            append_entries = AppendEntriesRequest(
                leader_id=self.id,
                term=self.current_term,
                prev_log_index=prev_log_index,
                prev_log_term=prev_log_term,
                entries=entries,
                leader_commit=self.commit_index
            )

            try:
                response = await self.client.post(f"{node}/replicate", json=append_entries.model_dump(), timeout=0.05)
            except httpx.RequestError:
                continue

            if response.json().get("term") > self.current_term:
                self.current_term = response.json().get("term")
                self.state = NodeState.FOLLOWER
                return

            if response.json().get('success'):
                self.match_index[node] = response.json().get('last_index')
                self.next_index[node] = response.json().get('last_index') + 1
                self.match_index["http://localhost:800" + str(self.id)] = len(self.log) - 1
                self.next_index["http://localhost:800" + str(self.id)] = len(self.log)
                majority_index = median_low(self.match_index.values())
                await self.commit_entries(majority_index)
            else:
                #self.next_index[node] = max(0, self.next_index[node] - 1)
                self.next_index[node] = response.json().get('last_index') + 1

    async def commit_entries(self, leader_commit):
        #print("self.last_applied " + str(self.last_applied))
        if leader_commit <= self.commit_index:
            return
        self.commit_index = min(leader_commit, len(self.log) - 1)
        not_applied_entries = self.log[self.last_applied + 1:self.commit_index + 1]
        for entry in not_applied_entries:
            self.last_applied += 1
            self.data_store[entry.key] = entry.value

    async def received_entries(self, request: AppendEntriesRequest):
        print(f"My term: {self.current_term}. Leader term: {request.term}.")
        if request.term > self.current_term:
            self.current_term = request.term
            self.state = NodeState.FOLLOWER
            self.voted_for = request.leader_id
            self.heartbeat_received = True
            self.leader_id = request.leader_id
        elif request.term == self.current_term:
            self.heartbeat_received = True
            self.leader_id = request.leader_id
        else:
            return AppendEntriesResponse(success=False, term=self.current_term, last_index=len(self.log)-1)
        success = len(self.log) - 1 >= request.prev_log_index and self.log[request.prev_log_index].term == request.prev_log_term
        if success:
            if len(self.log) - 1 > request.prev_log_index:
                self.log = self.log[:request.prev_log_index] + request.entries
            else:
                self.log += request.entries

            await self.commit_entries(request.leader_commit)
        return AppendEntriesResponse(success=success, term=self.current_term, last_index=len(self.log)-1)

    async def request_vote_handle(self, vote_request: VoteRequest):
        term = vote_request.term
        candidate_id = vote_request.candidate_id

        vote_granted = False
        if term > self.current_term:
            self.current_term = term
            self.state = NodeState.FOLLOWER
            self.voted_for = candidate_id
            vote_granted = True
            self.heartbeat_received = True
        elif term == self.current_term and (self.voted_for is None or self.voted_for == candidate_id):
            self.voted_for = candidate_id
            self.state = NodeState.FOLLOWER
            vote_granted = True
            self.heartbeat_received = True
        return VoteResponse(vote_granted=vote_granted, term=self.current_term)

    async def start_election(self):
        self.current_term += 1
        self.voted_for = self.id
        self.votes = 1
        self.state = NodeState.CANDIDATE

        term_on_election = self.current_term

        print(f"Node {self.id} начинает выборы в термине {self.current_term}")

        for node_url in self.all_nodes:
            if node_url.endswith(str(self.id)):
                continue
            if self.state == NodeState.FOLLOWER or self.current_term > term_on_election:
                break
            try:
                vote_request = VoteRequest(term=self.current_term, candidate_id=self.id)
                response = await self.client.post(f"{node_url}/request_vote", json=vote_request.model_dump(),
                                             timeout=0.05)
                if response.json().get("term") > self.current_term:
                    self.current_term = response.json().get("term")
                    self.state = NodeState.FOLLOWER
                    break
                if response.json().get("vote_granted"):
                    self.votes += 1
            except httpx.RequestError as e:
                pass

        if self.state == NodeState.FOLLOWER or self.current_term > term_on_election:
            return

        if self.votes > len(self.all_nodes) // 2:
            self.state = NodeState.LEADER
            print(f"Node {self.id} стал лидером в термине {self.current_term}")
            for node in self.all_nodes:
                self.next_index[node] = len(self.log)
            for node in self.all_nodes:
                self.match_index[node] = 0
            asyncio.create_task(self.send_heartbeats())
            ###
            asyncio.create_task(self.update_ttl())
            ###
        else:
            self.state = NodeState.CANDIDATE

    async def send_heartbeats(self):
        while self.state == NodeState.LEADER:
            await self.send_entries()
            if self.state != NodeState.LEADER:
                return
            await asyncio.sleep(0.5)

    async def election_timeout(self):
        while True:
            await asyncio.sleep(self.election_timeout_time())
            if self.state == NodeState.FOLLOWER and not self.heartbeat_received:
                await self.start_election()
                while self.state == NodeState.CANDIDATE:
                    await asyncio.sleep(self.election_timeout_time())
                    if self.heartbeat_received:
                        break
                    await self.start_election()
            self.heartbeat_received = False

    async def start(self):
        config = uvicorn.Config(self.app, host="localhost", port=self.port, log_level="critical")
        server = uvicorn.Server(config)
        await server.serve()


if __name__ == "__main__":
    port = sys.argv[1]

    cluster = [
        "http://localhost:8000",
        "http://localhost:8001",
        "http://localhost:8002",
        "http://localhost:8003",
        "http://localhost:8004"
    ]

    node = Node(port, port[-1], cluster)
    asyncio.run(node.start())
