#!/bin/bash
# generate_certs.sh - Generate local-only mTLS certificates for an Ayder HA cluster
# Usage: ./generate_certs.sh [3|5|7]
#   Default: 3 nodes

# Number of nodes (default: 3)
NUM_NODES=${1:-3}
umask 077
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CERT_DIR="${SCRIPT_DIR}/certs"

# Validate input
if [[ ! "$NUM_NODES" =~ ^(3|5|7)$ ]]; then
    echo "❌ Invalid number of nodes. Must be 3, 5, or 7"
    echo "Usage: $0 [3|5|7]"
    exit 1
fi

echo "🔐 Generating certificates for $NUM_NODES-node cluster..."

mkdir -p "$CERT_DIR"
cd "$CERT_DIR"

# ═══════════════════════════════════════════════════════════════════════════════
# 1. Create Certificate Authority (CA)
# ═══════════════════════════════════════════════════════════════════════════════
if [ ! -f ca.key ]; then
    echo "📋 Creating CA..."
    openssl genrsa -out ca.key 4096
    openssl req -new -x509 -days 3650 -key ca.key -out ca.crt \
        -subj "/CN=Ayder-Local-Development-CA/O=Ayder"
    echo "✅ CA created (ca.crt, ca.key)"
else
    echo "ℹ️  CA already exists, skipping..."
fi

# ═══════════════════════════════════════════════════════════════════════════════
# 2. Create Node Certificates
# ═══════════════════════════════════════════════════════════════════════════════
echo ""
echo "📋 Generating node certificates..."

for i in $(seq 1 "$NUM_NODES"); do
    node="node$i"

    # Skip if cert already exists
    if [ -f "${node}.crt" ] && [ -f "${node}.key" ]; then
        echo "ℹ️  ${node} certificates already exist, skipping..."
        continue
    fi

    # Generate private key
    openssl genrsa -out "${node}.key" 2048

    # Create CSR with Subject Alternative Names (SAN)
    cat > "${node}.cnf" << EOF
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no

[req_distinguished_name]
CN = ${node}
O = Ayder
OU = HA-Node

[v3_req]
subjectAltName = @alt_names
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth, clientAuth

[alt_names]
DNS.1 = ${node}
DNS.2 = localhost
IP.1 = 127.0.0.1
EOF

    # Create Certificate Signing Request (CSR)
    openssl req -new -key "${node}.key" -out "${node}.csr" -config "${node}.cnf"

    # Sign certificate with CA
    openssl x509 -req -days 365 -in "${node}.csr" \
        -CA ca.crt -CAkey ca.key -CAcreateserial \
        -out "${node}.crt" -extensions v3_req -extfile "${node}.cnf"

    # Cleanup temporary files
    rm "${node}.csr" "${node}.cnf"

    echo "✅ Generated ${node}.key and ${node}.crt"
done

# ═══════════════════════════════════════════════════════════════════════════════
# 3. Summary & Verification
# ═══════════════════════════════════════════════════════════════════════════════
echo ""
echo "🎉 Certificate generation complete!"
echo ""
echo "📁 Generated files in ./certs/:"
echo "   - ca.crt, ca.key          (Certificate Authority)"

for i in $(seq 1 "$NUM_NODES"); do
    echo "   - node${i}.crt, node${i}.key   (Node ${i} certificate)"
done

echo ""
echo "🔍 Verify certificates:"
echo "   openssl x509 -in certs/node1.crt -text -noout | grep -A2 'Subject Alternative Name'"
echo ""
echo "🚀 Ready to start ${NUM_NODES}-node HA cluster with mTLS!"
echo "⚠️  These local certificates are test fixtures. Never reuse them in production."
