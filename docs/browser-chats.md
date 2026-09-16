# Browser Chats & Real Model Selection

How to continue `gemini.google.com` chats through the API and select the real backend model.

## Continue a browser chat

Chats created manually in the browser are exposed via the Web extension. Disabled by default; enable with `GEMINI_BROWSER_CHATS_ENABLED=true` and `GEMINI_BROWSER_CHATS_API_KEY` (min 32 chars). Every request must carry the key as `Authorization: Bearer <key>` or `?key=<key>`.

```bash
KEY="$GEMINI_BROWSER_CHATS_API_KEY"

# List browser chats (paginated)
curl "http://localhost:4981/gemini/web/chats?limit=10" -H "Authorization: Bearer $KEY"

# Read one chat (turns include rid/rcid)
curl "http://localhost:4981/gemini/web/chats/c_EXAMPLE?limit=20" -H "Authorization: Bearer $KEY"

# Append a message to the same browser-visible branch
curl -X POST 'http://localhost:4981/gemini/web/chats/c_EXAMPLE/messages' \
  -H 'Content-Type: application/json' -H "Authorization: Bearer $KEY" \
  -d '{"prompt":"Continue from the browser context","model":"gemini-3.8-flash"}'
```

The reply returns the persisted `chat_id`, `request_id` (`r_...`) and `candidate_id` (`rc_...`). A read-back of the chat shows the new turn in the same branch with matching IDs.

The OpenAI-compatible endpoint accepts the same extension (only the newest user message is appended, browser history is not duplicated):

```bash
curl -X POST 'http://localhost:4981/openai/v1/chat/completions' \
  -H 'Content-Type: application/json' \
  -d '{
    "model":"gemini-3.8-flash",
    "chat_id":"c_EXAMPLE",
    "messages":[{"role":"user","content":"Continue this browser chat"}]
  }'
```

`POST /gemini/v1beta/models/{model}:generateContent` also returns `chat_id`/`request_id`/`candidate_id` for newly created chats.

## Real model selection (x-goog-ext header)

The browser selects the model through the `x-goog-ext-525001261-jspb` HTTP header, not the payload field. The bridge now sets it with the real internal model hash:

```
x-goog-ext-525001261-jspb:
[1,null,null,null,"<model_hash>",null,null,<mode>,[4,5,6,8,4,5,6,8],null,null,2,null,null,<selector>,<surface>,"<uuid>"]

x-goog-ext-73010989-jspb: [0]
x-goog-ext-73010990-jspb: [0,0,0]
```

Verified model hashes (boq_assistant-bard-web-server_20260910.05_p2):

| Model | Internal hash | Selector |
|---|---|---|
| `gemini-3.8-flash` | `56fdd199312815e2` | 1 |
| `gemini-3-flash` | `fbb127bbb056c959` | 1 |
| `gemini-3-flash-thinking` | `5bf011840784117a` | 2 |
| `gemini-3.1-pro` | `e6fa609c3fa255c0` | 3 |
| `gemini-3-pro` | `9d8ca3786ebdfbea` | 3 |
| `gemini-3.5-flash-lite` | `8c46e95b1a07cecc` | 6 |
| `gemini-3-flash-thinking-plus` | `e051ce1aa80aa576` | 2 |
| `gemini-3-pro-plus` / `-advanced` | `e6fa609c3fa255c0` | 3 |

Models with a known hash bypass the account catalog and are accepted even when the (stale) catalog does not list them.

### Behavior by tier

- **Flash family** (`3.8-flash`, `3-flash`, `3.5-flash-lite`, `3-flash-thinking`): answers synchronously and self-identifies correctly.
- **Pro family** (`3.1-pro`, `3-pro`, `gemini-advanced`, selector 3): switches the server into **Deep Think** agentic mode and immediately returns a placeholder (`I'm on it...` + `agentic_processing_chip` link). The final answer must be polled later — Deep Think result polling is not implemented yet; until then prefer Flash models for synchronous calls.

## Live cookie sync

Run `scripts/sync_cdp_cookies.py` against a logged-in Chrome CDP endpoint; point `GEMINI_COOKIE_SYNC_FILE` at its output. The file is re-read before every RPC, so browser-driven cookie rotation is picked up without restarts.

```bash
python3 scripts/sync_cdp_cookies.py \
  --cdp http://127.0.0.1:9224 \
  --output /opt/gemini-web-spb/api/cookies/browser-cookies.json \
  --gid 101   # optional: group allowed to read
```

On the host this runs from a systemd timer (`gemini-web-cookie-sync.timer`, 60s).

## Limitations

- Web RPC is unofficial; Google may change positional payload fields.
- The browser "regenerate" arrow has no dedicated RPC here; resending the same prompt creates a new turn (both stay in history).
- Chat history does not expose which model answered a past turn.
- Pro-tier Deep Think requires result polling (TODO).
- A nonexistent `chat_id` may surface as 502 instead of 404 (TODO).
