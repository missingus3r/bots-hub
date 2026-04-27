#!/usr/bin/env bash
# bots-hub CLI helper for Friday.
#
#   hub.sh health
#   hub.sh publish incoming <chat_id> <msg_id> <sender_id> <sender_name> <is_bot> "<text>"
#   hub.sh publish outgoing <chat_id> <msg_id> "<text>"
#   hub.sh messages [chat_id] [since_iso] [limit]
#   hub.sh recent   [chat_id] [minutes]        # messages from the last N min (default 15)
#   hub.sh mentions [bot_id]  [minutes] [chat_id]
#                                               # other bots that tagged this bot
#
# Reads the Friday token from ~/bots-hub/tokens.json.

set -eu

HUB="${BOTS_HUB_URL:-http://127.0.0.1:7788}"
TOKEN=$(python3 -c 'import json,sys; print(json.load(open("/home/br1/bots-hub/tokens.json"))["friday"])')

cmd="${1:-help}"
shift || true

case "$cmd" in
  health)
    curl -sS "$HUB/health" ;;

  publish)
    kind="${1:-incoming}"; shift
    chat_id="$1"; msg_id="$2"; shift 2
    if [ "$kind" = "outgoing" ]; then
      text="$1"
      payload=$(jq -n --arg chat_id "$chat_id" --argjson msg_id "$msg_id" \
                     --arg text "$text" --arg kind "outgoing" \
                     --arg sender_id "friday_bot" --arg sender_name "Friday" \
                     '{chat_id:$chat_id, msg_id:$msg_id, sender_id:$sender_id,
                       sender_name:$sender_name, is_bot:true, text:$text, kind:$kind}')
    else
      sender_id="$1"; sender_name="$2"; is_bot="$3"; text="$4"
      payload=$(jq -n --arg chat_id "$chat_id" --argjson msg_id "$msg_id" \
                     --arg sender_id "$sender_id" --arg sender_name "$sender_name" \
                     --argjson is_bot "$is_bot" --arg text "$text" --arg kind "incoming" \
                     '{chat_id:$chat_id, msg_id:$msg_id, sender_id:$sender_id,
                       sender_name:$sender_name, is_bot:$is_bot, text:$text, kind:$kind}')
    fi
    curl -sS -X POST "$HUB/ingest" \
      -H "Content-Type: application/json" \
      -H "X-Hub-Token: $TOKEN" \
      -d "$payload"
    ;;

  messages)
    chat_id="${1:-}"; since="${2:-}"; limit="${3:-50}"
    q="limit=$limit"
    [ -n "$chat_id" ] && q="$q&chat_id=$chat_id"
    [ -n "$since" ]   && q="$q&since=$since"
    curl -sS "$HUB/messages?$q"
    ;;

  recent)
    chat_id="${1:--1003904510322}"; minutes="${2:-15}"
    since=$(python3 -c "from datetime import datetime,timezone,timedelta; print((datetime.now(timezone.utc)-timedelta(minutes=$minutes)).isoformat())")
    curl -sS "$HUB/messages?chat_id=$chat_id&since=$since&limit=100"
    ;;

  mentions)
    # v0.2: which messages from OTHER bots tagged me (this bot)?
    # usage: hub.sh mentions [bot_id] [minutes] [chat_id]
    bot_id="${1:-friday}"; minutes="${2:-60}"; chat_id="${3:-}"
    since=$(python3 -c "from datetime import datetime,timezone,timedelta; print((datetime.now(timezone.utc)-timedelta(minutes=$minutes)).isoformat())")
    q="bot_id=$bot_id&since=$since&limit=50"
    [ -n "$chat_id" ] && q="$q&chat_id=$chat_id"
    curl -sS "$HUB/mentions?$q"
    ;;

  *)
    sed -n '2,10p' "$0"
    exit 1 ;;
esac
echo
