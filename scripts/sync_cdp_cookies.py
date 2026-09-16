#!/usr/bin/env python3
"""Atomically export live Gemini auth cookies from a Chrome CDP relay."""

import argparse
import asyncio
import json
import os
import tempfile
import urllib.request

import websockets

REQUIRED = ("__Secure-1PSID", "__Secure-1PSIDTS")
OPTIONAL = ("__Secure-1PSIDCC",)


def discover_page(cdp_http: str) -> dict:
    with urllib.request.urlopen(f"{cdp_http.rstrip('/')}/json/list", timeout=10) as response:
        pages = json.load(response)
    for page in pages:
        if page.get("type") == "page" and "gemini.google.com" in page.get("url", ""):
            return page
    raise RuntimeError("no open gemini.google.com page found in CDP")


async def read_cookies(page: dict) -> dict[str, str]:
    async with websockets.connect(page["webSocketDebuggerUrl"], max_size=20_000_000) as socket:
        await socket.send(
            json.dumps(
                {
                    "id": 1,
                    "method": "Network.getCookies",
                    "params": {"urls": ["https://gemini.google.com", "https://google.com"]},
                }
            )
        )
        while True:
            message = json.loads(await socket.recv())
            if message.get("id") == 1:
                break
    if "error" in message:
        raise RuntimeError(f"CDP Network.getCookies failed: {message['error']}")
    values = {cookie["name"]: cookie["value"] for cookie in message["result"]["cookies"]}
    missing = [name for name in REQUIRED if not values.get(name)]
    if missing:
        raise RuntimeError(f"missing required cookies: {', '.join(missing)}")
    return {name: values[name] for name in (*REQUIRED, *OPTIONAL) if values.get(name)}


def atomic_write(path: str, values: dict[str, str], gid: int | None = None) -> None:
    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, mode=0o700, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=".browser-cookies-", dir=directory, text=True)
    try:
        os.fchmod(fd, 0o640 if gid is not None else 0o600)
        if gid is not None:
            os.fchown(fd, -1, gid)
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(values, stream, ensure_ascii=False, separators=(",", ":"))
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    except BaseException:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cdp", default="http://127.0.0.1:9224")
    parser.add_argument("--output", default="/opt/gemini-web-spb/api/cookies/browser-cookies.json")
    parser.add_argument("--gid", type=int, help="group allowed to read the cookie file")
    args = parser.parse_args()
    page = discover_page(args.cdp)
    values = await read_cookies(page)
    atomic_write(args.output, values, args.gid)
    print(f"updated {args.output}")


if __name__ == "__main__":
    asyncio.run(main())
