#!/usr/bin/env bash
# Helper behind the Makefile. Usage: scripts/fn.sh <list|check|deploy|describe> [function-name]
#
# A function is "known" when a deploy.sh one or two folders below the repo root has a line
# FUNCTION=<name> at column 0. That deploy.sh is the single source of truth for its deploy config.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

deploy_scripts() { ls -1 */deploy.sh */*/deploy.sh 2>/dev/null | sort; }
header() { sed -n "s/^$1=//p" "$2" | head -1; }

known_names() { for f in $(deploy_scripts); do header FUNCTION "$f"; done | sort; }

die() { echo "error: $*" >&2; exit 1; }

resolve() {
  local fn="$1" matches
  [ -n "$fn" ] || die "missing function name. Usage: make $CMD <function-name>
known functions:
$(known_names | sed 's/^/  /')"
  matches=$(grep -lx "FUNCTION=$fn" $(deploy_scripts) 2>/dev/null || true)
  local n
  n=$(printf '%s' "$matches" | grep -c . || true)
  if [ "$n" -eq 0 ]; then
    die "no deploy.sh declares FUNCTION=$fn
known functions:
$(known_names | sed 's/^/  /')"
  elif [ "$n" -gt 1 ]; then
    die "FUNCTION=$fn is declared in more than one deploy.sh:
$(printf '%s\n' "$matches" | sed 's/^/  /')"
  fi
  SCRIPT="$matches"
  DIR="$(dirname "$SCRIPT")"
  REGION="$(header REGION "$SCRIPT")"
  PROJECT="$(header PROJECT "$SCRIPT")"
  [ -n "$REGION" ] && [ -n "$PROJECT" ] || die "$SCRIPT must define REGION= and PROJECT= at column 0"
}

CMD="${1:-}"
FN="${2:-}"

case "$CMD" in
  list)
    printf '%-24s %-13s %s\n' FUNCTION REGION FOLDER
    for f in $(deploy_scripts); do
      name=$(header FUNCTION "$f"); [ -n "$name" ] || continue
      printf '%-24s %-13s %s\n' "$name" "$(header REGION "$f")" "$(dirname "$f")"
    done
    ;;
  describe)
    resolve "$FN"
    gcloud functions describe "$FN" --project "$PROJECT" --region "$REGION" --format=yaml
    ;;
  check)
    resolve "$FN"
    exec python3 scripts/fn_check.py "$FN" "$DIR"
    ;;
  deploy)
    resolve "$FN"
    echo "function: $FN"
    echo "folder:   $DIR"
    echo "project:  $PROJECT"
    echo "region:   $REGION"
    changes=$(git status --short -- "$DIR")
    if [ -n "$changes" ]; then
      echo "WARNING: uncommitted changes in $DIR (the working tree is what gets deployed):"
      printf '%s\n' "$changes" | sed 's/^/  /'
    fi
    if [ "${DRY_RUN:-}" = "1" ]; then
      echo "DRY_RUN=1: command that would run:"
      GCLOUD="echo gcloud" bash "$SCRIPT"
      exit 0
    fi
    if [ "${YES:-}" != "1" ]; then
      printf 'Deploy %s? [y/N] ' "$FN"
      read -r answer || answer=""
      case "$answer" in
        y|Y|yes|YES) ;;
        *) echo "aborted, nothing deployed"; exit 0 ;;
      esac
    fi
    exec bash "$SCRIPT"
    ;;
  *)
    die "usage: scripts/fn.sh <list|check|deploy|describe> [function-name]"
    ;;
esac
