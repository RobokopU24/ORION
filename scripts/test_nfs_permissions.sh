#!/bin/bash
# test_nfs_permissions.sh
# Diagnoses NFS uid squashing and attribute caching for ORION log files.
# Run inside an ORION pod (buildMode=false) against the NFS-backed ORION_LOGS directory.
#
# Usage:
#   ORION_LOGS=/ORION_logs bash test_nfs_permissions.sh
#   or simply run with ORION_LOGS already set in the environment.

LOGS_DIR="${ORION_LOGS:-/ORION_logs}"

echo "============================================================"
echo " ORION NFS Diagnostic — $(date)"
echo "============================================================"
echo ""

# ── 1. Identity ──────────────────────────────────────────────────
echo "[ 1 ] Pod identity"
echo "      uid=$(id -u)  gid=$(id -g)  user=$(id -un 2>/dev/null || echo unknown)"
echo "      ORION_LOGS=$LOGS_DIR"
echo ""

# ── 2. NFS effective UID ─────────────────────────────────────────
# The file we create will be owned by whatever uid the NFS server assigns.
# If uid squashing is active, it will differ from id -u.
echo "[ 2 ] NFS effective UID (uid squashing check)"
PROBE="$LOGS_DIR/.uid_probe_$$"
if touch "$PROBE" 2>/dev/null; then
    NFS_UID=$(stat -c '%u' "$PROBE")
    NFS_GID=$(stat -c '%g' "$PROBE")
    POD_UID=$(id -u)
    rm -f "$PROBE"
    echo "      Created probe file — NFS server assigned owner: uid=$NFS_UID gid=$NFS_GID"
    if [ "$NFS_UID" = "$POD_UID" ]; then
        echo "      RESULT: UID passes through correctly (no squashing)"
    else
        echo "      RESULT: UID SQUASHED — pod uid=$POD_UID → NFS uid=$NFS_UID"
        echo "      This means the pod cannot write files it didn't create in this session."
    fi
else
    echo "      ERROR: cannot even create a probe file in $LOGS_DIR"
    echo "      Directory permissions:"
    ls -la "$LOGS_DIR" 2>&1 | head -5
fi
echo ""

# ── 3. Writability of existing log files ─────────────────────────
echo "[ 3 ] Existing log file writability"
printf "      %-38s  %-14s  %-6s  %s\n" "File" "Owner(uid:gid)" "Perms" "Writable?"
printf "      %-38s  %-14s  %-6s  %s\n" "----" "--------------" "-----" "---------"
UNWRITABLE=0
for f in "$LOGS_DIR"/*.log "$LOGS_DIR"/*.cache "$LOGS_DIR"/*.txt; do
    [ -f "$f" ] || continue
    fname=$(basename "$f")
    owner=$(stat -c '%u:%g' "$f")
    perms=$(stat -c '%a' "$f")
    if echo "" >> "$f" 2>/dev/null; then
        result="YES"
    else
        result="NO  ← stale ownership mismatch"
        UNWRITABLE=$((UNWRITABLE + 1))
    fi
    printf "      %-38s  %-14s  %-6s  %s\n" "$fname" "$owner" "$perms" "$result"
done
echo ""
if [ "$UNWRITABLE" -gt 0 ]; then
    echo "      $UNWRITABLE file(s) cannot be written. These were created by a different"
    echo "      effective uid than the current pod uid on the NFS server."
    echo "      Admin fix: chown nobody:nogroup $LOGS_DIR/*.log"
    echo "              or: chmod 666 $LOGS_DIR/*.log"
fi
echo ""

# ── 4. Attribute cache timing ─────────────────────────────────────
# Create a file, delete it, then poll stat() to see how long the NFS
# client continues to report it as existing (stale attribute cache).
echo "[ 4 ] NFS attribute cache — stale-entry timing"
STALE="$LOGS_DIR/.stale_test_$$"
echo "probe" > "$STALE"
sleep 0.2
rm -f "$STALE"
echo "      Deleted $STALE — polling stat() every 0.5s to see how long it stays visible..."
STALE_SECS=0
for i in $(seq 1 60); do
    if stat "$STALE" > /dev/null 2>&1; then
        STALE_SECS=$i
        printf "."
        sleep 0.5
    else
        break
    fi
done
echo ""
if [ "$STALE_SECS" -gt 0 ]; then
    echo "      File was still visible for ~$((STALE_SECS / 2))s after deletion (attribute cache active)"
    echo "      Admin fix: add 'noac' to NFS PV mountOptions (requires cluster-admin PV)"
else
    echo "      File disappeared immediately — attribute cache not observed from this client"
    echo "      (Cache effects are most visible across separate pods/nodes, not within the same process)"
fi
echo ""

# ── 5. Mount options ──────────────────────────────────────────────
echo "[ 5 ] NFS mount options for $LOGS_DIR"
mount | grep -i "$(echo "$LOGS_DIR" | cut -d/ -f1-3)" 2>/dev/null \
    || mount | grep -i "nfs" 2>/dev/null \
    || echo "      (mount output not available — run 'mount' on the Kubernetes node instead)"
echo ""

# ── Summary ───────────────────────────────────────────────────────
echo "============================================================"
echo " Summary for admin"
echo "============================================================"
echo " NFS path : $LOGS_DIR"
echo " Pod uid  : $(id -u) → NFS uid: $NFS_UID ($([ "${NFS_UID:-x}" = "$(id -u)" ] && echo 'no squash' || echo 'SQUASHED'))"
echo " Unwritable log files: $UNWRITABLE"
echo ""
echo " Admin checklist:"
echo "   1. Check NFS export:  cat /etc/exports  (look for all_squash / anonuid / anongid)"
echo "   2. Check node mount:  ssh <node>  mount | grep Data_services"
echo "   3. Fix existing files: chown nobody:nogroup $LOGS_DIR/*.log"
echo "                       or: chmod 666 $LOGS_DIR/*.log"
echo "   4. Prevent recurrence: export NFS path with no_all_squash and consistent uid mapping"
echo "                       or: add 'noac' mountOption to the Kubernetes PV (requires cluster-admin)"
echo "============================================================"
