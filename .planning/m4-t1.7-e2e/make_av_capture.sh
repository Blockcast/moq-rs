#!/usr/bin/env bash
# Regenerate the audio+video MMTP capture fixture (BLO-8706) for the T1.7 audio E2E.
#
# Requires the custom Blockcast ffmpeg with the moq_mmt muxer. The system
# /usr/bin/ffmpeg does NOT have it; override FFMPEG to point at the fork build:
#   FFMPEG=~/src/pim-multicast-gateway/FFmpeg/build-native/ffmpeg ./make_av_capture.sh
#
# Output: a {packets_hex:[...]} list of MMTP datagrams. stream0=video ->
# packet_id 1, stream1=audio -> packet_id 2
# (moqenc_mmt.c: stream->packet_id = stream_index + 1). The generator joins a
# local multicast group; serve.py later replays the fixture as UDP unicast.
set -euo pipefail
# Resolve the Blockcast FFmpeg (moq_mmt muxer). Prefer the CI convention the pmg
# workflows use ($FFMPEG_PATH/build-native — they set FFMPEG_PATH=<workspace>/FFmpeg
# from the Blockcast/FFmpeg submodule), then the in-tree submodule build, then a
# devbox checkout. System /usr/bin/ffmpeg has NO moq_mmt muxer — never use it.
SELF_DIR="$(cd "$(dirname "$0")" && pwd)"
if [ -n "${FFMPEG:-}" ]; then :
elif [ -n "${FFMPEG_PATH:-}" ] && [ -x "$FFMPEG_PATH/build-native/ffmpeg" ]; then FFMPEG="$FFMPEG_PATH/build-native/ffmpeg"
elif [ -x "$SELF_DIR/../../../FFmpeg/build-native/ffmpeg" ]; then FFMPEG="$(cd "$SELF_DIR/../../.." && pwd)/FFmpeg/build-native/ffmpeg"
else FFMPEG="$HOME/src/pim-multicast-gateway/FFmpeg/build-native/ffmpeg"; fi
AUDIO_CODEC="${AUDIO_CODEC:-aac}"
case "$AUDIO_CODEC" in
  aac)
    AUDIO_ENCODER=aac
    AUDIO_BITRATE=128k
    DEFAULT_OUT="$SELF_DIR/moq_mmt_capture_av.json"
    ;;
  opus)
    AUDIO_ENCODER=libopus
    AUDIO_BITRATE=96k
    DEFAULT_OUT="$SELF_DIR/moq_mmt_capture_opus.json"
    ;;
  *)
    echo "unsupported AUDIO_CODEC=$AUDIO_CODEC (expected aac or opus)"
    exit 1
    ;;
esac
OUT="${1:-$DEFAULT_OUT}"
PORT="${PORT:-5000}"; DUR="${DUR:-4}"
MCAST_ADDR="${MCAST_ADDR:-239.1.1.1}"
MCAST_LOCAL_ADDR="${MCAST_LOCAL_ADDR:-127.0.0.1}"
[ -x "$FFMPEG" ] || { echo "custom ffmpeg with moq_mmt not found: $FFMPEG"; exit 1; }
"$FFMPEG" -hide_banner -h muxer=moq_mmt 2>/dev/null | grep -q moq_mmt \
  || { echo "$FFMPEG has no moq_mmt muxer"; exit 1; }

python3 - "$PORT" "$OUT" "$((DUR+10))" "$AUDIO_CODEC" \
  "$MCAST_ADDR" "$MCAST_LOCAL_ADDR" <<'PY' &
import socket,sys,json,time
from collections import Counter
port=int(sys.argv[1]); out=sys.argv[2]; dur=float(sys.argv[3]); codec=sys.argv[4]
group=sys.argv[5]; local=sys.argv[6]
s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
s.bind(("",port))
s.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP,
             socket.inet_aton(group) + socket.inet_aton(local))
s.settimeout(0.5)
pk=[]; idle=None; end=time.time()+dur
while time.time()<end:
    if pk and idle and time.time()-idle>2: break
    try:
        d,_=s.recvfrom(65535); pk.append(d.hex()); idle=time.time()
    except socket.timeout: pass
s.close()
json.dump({"_comment":f"audio+video moq_mmt multicast MMTP capture ({codec}); regen via make_av_capture.sh",
           "source":f"ffmpeg moq_mmt -moq_enabled 0 -multicast_enabled 1 -mcast_container mmtp (v=pid1, {codec}=pid2)",
           "packets_hex":pk}, open(out,"w"))
inv=Counter(int.from_bytes(bytes.fromhex(h)[2:4],"big") for h in pk)
print("captured",len(pk),"packets; packet_id inventory:",dict(sorted(inv.items())))
PY
CAP=$!; sleep 1
"$FFMPEG" -hide_banner -loglevel error \
  -f lavfi -i "testsrc2=size=320x240:rate=15" \
  -f lavfi -i "sine=frequency=440:sample_rate=48000" \
  -map 0:v -map 1:a -t "$DUR" \
  -c:v libx264 -g 15 -pix_fmt yuv420p -profile:v baseline -preset ultrafast \
  -c:a "$AUDIO_ENCODER" -ar 48000 -ac 2 -b:a "$AUDIO_BITRATE" \
  -f moq_mmt -moq_enabled 0 -multicast_enabled 1 -mcast_container mmtp \
  -mcast_addr "$MCAST_ADDR" -mcast_port "$PORT" \
  -mcast_local_addr "$MCAST_LOCAL_ADDR" /dev/null
wait $CAP
echo "wrote $OUT"
