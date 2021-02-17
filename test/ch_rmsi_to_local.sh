#!/bin/sh
#set -x
#
HST="clickhouse-omsk.rmsi.srv.loc"
REMOTE_SOCKET="${HST}:8123"
REMOTE_SOCKET9000="${HST}:9000"
REMOTE_SOCKET9440="${HST}:9004"

pgrep -f "${HST}:" -a && kill $(pgrep -f "${HST}:")
echo "Starting ssh forwarding to ${REMOTE_SOCKET}"
###

#ssh -f -N nn01 -L ${LOCAL_PORT}:${REMOTE_SOCKET}
ssh -f -N nn03 -L 8123:${REMOTE_SOCKET}
ssh -f -N nn03 -L 9000:${REMOTE_SOCKET9000}
ssh -f -N nn03 -L 9004:${REMOTE_SOCKET9440}
# go though Aida: 
# run on Napa> ssh -f -N -R 8123:localhost:8123 aidb
# run localy>  ssh -f -N aidb -L 8123:localhost:8123

echo ""
# проверка, должен вернуть "OK"
wget -nv http://localhost:8123 && cat ./index.html && rm ./index.html
###
echo "If you see OK ^ you can connect to 127.0.0.1/XXXX or 9000, 9004 ports"
