#!/usr/bin/env bash
# Generates a local CA and a wildcard TLS cert for the Neon proxy, then loads
# them into the cluster as a TLS secret.
#
# The CN *must* be the wildcard domain: the proxy derives its set of valid SNI
# "common names" from certificate CNs (mkcert won't work — it doesn't put the
# domain in CN).
set -euo pipefail
DIR="$(cd "$(dirname "$0")/.." && pwd)/deploy/certs"
mkdir -p "$DIR"
cd "$DIR"
DOMAIN="${DOMAIN:-db.127-0-0-1.sslip.io}"

if [[ ! -f ca.key ]]; then
  openssl genrsa -out ca.key 2048
  openssl req -new -x509 -days 3650 -key ca.key -subj "/CN=firth-pgsql-dev-ca" -out ca.crt
fi

openssl genrsa -out proxy.key 2048
openssl req -new -key proxy.key -subj "/CN=*.${DOMAIN}" -out proxy.csr
printf "subjectAltName=DNS:*.%s,DNS:%s\n" "$DOMAIN" "$DOMAIN" > ext.cnf
openssl x509 -req -days 3650 -in proxy.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -extfile ext.cnf -out proxy.crt

kubectl -n firth-pgsql create secret tls proxy-tls \
  --cert=proxy.crt --key=proxy.key \
  --dry-run=client -o yaml | kubectl apply -f -

echo "CA cert: $DIR/ca.crt (pass as sslrootcert for verify-full)"
