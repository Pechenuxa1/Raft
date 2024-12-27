import sys
import time
import httpx
import asyncio
from fastapi import HTTPException

from raft import LockRequest, IsLocked

cluster = [
        "http://localhost:8000",
        "http://localhost:8001",
        "http://localhost:8002",
        "http://localhost:8003",
        "http://localhost:8004"
    ]


async def main():
    key = args[0]
    name = args[1]
    ttl = int(args[2])

    lock_request = LockRequest(key=key,
                               current_value="",
                               next_value=name,
                               version=1,
                               ttl=ttl
                               )

    async with httpx.AsyncClient() as client:
        for node_url in cluster:
            try:
                response = await client.get(node_url + "/is-locked/" + key, timeout=0.05)
            except httpx.ConnectTimeout:
                continue

            print(response.json())
            break

        if response.status_code == 200 and response.json().get('is_locked'):
            while response.json().get('is_locked'):
                await asyncio.sleep(1)

                for node_url in cluster:
                    try:
                        response = await client.get(node_url + "/is-locked/" + key, timeout=0.05)
                    except Exception:
                        continue
                    if response.status_code == 200:
                        print(response.json())
                        break
            print(f"Unlocked!")

        for node_url in cluster:
            try:
                response = await client.post(node_url + "/lock", json=lock_request.model_dump(), timeout=2)
                if response.status_code == 403:
                    continue
            except httpx.ConnectTimeout:
                continue
            print(f"I locked: {response.json()}")
            break


if __name__ == "__main__":
    args = sys.argv[1:]
    asyncio.run(main())