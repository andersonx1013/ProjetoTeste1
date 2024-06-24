#Arquivo db/wait-for-it.sh
#!/bin/sh

TIMEOUT=15
QUIET=0
HOST=
PORT=
RESULT=1

while [ $# -gt 0 ]; do
    case "$1" in
        -q)
        QUIET=1
        shift
        ;;
        -t)
        TIMEOUT="$2"
        if [ -z "$TIMEOUT" ]; then break; fi
        shift 2
        ;;
        --timeout=*)
        TIMEOUT="${1#*=}"
        shift
        ;;
        --host=*)
        HOST="${1#*=}"
        shift
        ;;
        --port=*)
        PORT="${1#*=}"
        shift
        ;;
        *)
        echo "Unknown argument: $1"
        exit 1
        ;;
    esac
done

if [ -z "$HOST" ] || [ -z "$PORT" ]; then
    echo "Usage: $0 --host=HOST --port=PORT [--timeout=SECONDS]"
    exit 1
fi

for i in $(seq $TIMEOUT); do
    nc -z "$HOST" "$PORT" > /dev/null 2>&1
    RESULT=$?
    if [ $RESULT -eq 0 ]; then
        if [ $QUIET -ne 1 ]; then echo "$HOST:$PORT is available after $i seconds"; fi
        break
    fi
    sleep 1
done

exit $RESULT
