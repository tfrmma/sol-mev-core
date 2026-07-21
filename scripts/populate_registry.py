#!/usr/bin/env python3
"""
Populates registry.json with real mainnet pools, decoded directly from on-chain
account data via your own Solana RPC. Same byte offsets already trusted and used
in src/monitor.rs (decode_raydium_pool / decode_whirlpool_pool), reimplemented
here so this script has zero dependency on any third-party REST API's JSON shape.

Usage:
    pip install requests base58
    python3 populate_registry.py <YOUR_RPC_URL> > registry.json

Add more pool addresses to POOL_IDS below. Good places to find them:
  - https://solscan.io  (search a token, look at its "Markets" tab)
  - https://dexscreener.com/solana (sort by volume for a pair)
  - https://raydium.io/liquidity-pools/ or https://www.orca.so/pools (UI, copy pool address from URL)
"""
import sys
import json
import base64
import base58
import requests

RAYDIUM_AMM_V4_PROGRAM = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"
ORCA_WHIRLPOOL_PROGRAM = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"

# Verified against multiple independent sources (GeckoTerminal, Solscan, the
# official raydium-sdk-v1 README example). Add more pool_ids here as you find them.
POOL_IDS = {
    "raydium": [
        "58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUWBkwMihLYQo2",  # SOL/USDC
    ],
    "whirlpool": [
        # add whirlpool pool addresses here, e.g. from orca.so/pools -> click a
        # pool -> the address is in the URL. decode logic below handles them.
    ],
}


def rpc_get_account(rpc_url: str, pubkey: str) -> bytes | None:
    resp = requests.post(rpc_url, json={
        "jsonrpc": "2.0", "id": 1, "method": "getAccountInfo",
        "params": [pubkey, {"encoding": "base64"}],
    }, timeout=10)
    resp.raise_for_status()
    value = resp.json().get("result", {}).get("value")
    if value is None:
        print(f"  ! account not found: {pubkey}", file=sys.stderr)
        return None
    return base64.b64decode(value["data"][0])


def pk(data: bytes, offset: int) -> str:
    return base58.b58encode(data[offset:offset + 32]).decode()


def decode_raydium_pool(pool_id: str, data: bytes) -> dict | None:
    # discriminant check, same as RAYDIUM_POOL_DISC in monitor.rs
    if data[:8] != bytes([0x01, 0x00, 0x00, 0x00, 0x06, 0x00, 0x00, 0x00]):
        print(f"  ! {pool_id}: discriminant mismatch, not a live raydium AMM v4 pool", file=sys.stderr)
        return None

    coin_mint_offset = 0xB8
    return {
        "pool_id": pool_id,
        "program_id": RAYDIUM_AMM_V4_PROGRAM,
        "token_a_mint": pk(data, coin_mint_offset),
        "token_b_mint": pk(data, coin_mint_offset + 32),
        # baseVault/quoteVault sit immediately before baseMint/quoteMint in
        # LIQUIDITY_STATE_LAYOUT_V4 (raydium-sdk/src/liquidity/layout.ts), no gap.
        "token_a_vault": pk(data, coin_mint_offset - 64),
        "token_b_vault": pk(data, coin_mint_offset - 32),
        "fee_bps": 25,
        "extra_accounts": [],
        "dex": "amm_raydium",
    }


def decode_whirlpool_pool(pool_id: str, data: bytes) -> dict | None:
    disc = bytes([0x3f, 0x95, 0xd1, 0x0c, 0xe1, 0x80, 0x63, 0x09])
    if data[:8] != disc:
        print(f"  ! {pool_id}: discriminant mismatch, not a whirlpool account", file=sys.stderr)
        return None

    return {
        "pool_id": pool_id,
        "program_id": ORCA_WHIRLPOOL_PROGRAM,
        "token_a_mint": pk(data, 101),
        "token_b_mint": pk(data, 181),
        "token_a_vault": pk(data, 133),
        "token_b_vault": pk(data, 213),
        "fee_bps": int.from_bytes(data[45:47], "little") // 100,
        "extra_accounts": [],
        "dex": "amm_orca_whirlpool",
    }


def main():
    if len(sys.argv) != 2:
        print("usage: python3 populate_registry.py <RPC_URL>", file=sys.stderr)
        sys.exit(1)
    rpc_url = sys.argv[1]

    pools = []
    for pool_id in POOL_IDS["raydium"]:
        print(f"fetching raydium pool {pool_id}...", file=sys.stderr)
        data = rpc_get_account(rpc_url, pool_id)
        if data:
            decoded = decode_raydium_pool(pool_id, data)
            if decoded:
                pools.append(decoded)

    for pool_id in POOL_IDS["whirlpool"]:
        print(f"fetching whirlpool {pool_id}...", file=sys.stderr)
        data = rpc_get_account(rpc_url, pool_id)
        if data:
            decoded = decode_whirlpool_pool(pool_id, data)
            if decoded:
                pools.append(decoded)

    registry = {
        "programs": [
            {"program_id": RAYDIUM_AMM_V4_PROGRAM, "label": "Raydium AMM v4", "kind": "amm_raydium", "version": 4, "enabled": True},
            {"program_id": ORCA_WHIRLPOOL_PROGRAM, "label": "Orca Whirlpool", "kind": "amm_orca_whirlpool", "version": 1, "enabled": True},
        ],
        "pools": pools,
    }
    print(json.dumps(registry, indent=2))
    print(f"\ndone, {len(pools)} pool(s) decoded. redirect stdout to registry.json:", file=sys.stderr)
    print(f"  python3 populate_registry.py <RPC_URL> > registry.json", file=sys.stderr)


if __name__ == "__main__":
    main()
