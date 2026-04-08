#!/bin/bash
# Generate Python bindings for Swift proto files
#
# Option 1 (recommended): Copy pre-generated pb2 files from ha3_install
#   bash generate_proto.sh --from-ha3install
#
# Option 2: Regenerate using protoc (requires protoc 3.x)
#   bash generate_proto.sh
#
# Protobuf runtime path in ha3_install:
#   /home/xijie/havenask_120/ha3_install/usr/local/lib/python/site-packages
# Set before use:
#   export PYTHONPATH=/home/xijie/havenask_120/ha3_install/usr/local/lib/python/site-packages:$PYTHONPATH

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROTO_SRC="${SCRIPT_DIR}/../java_client/proto_files"
PROTO_OUT="${SCRIPT_DIR}/swift/proto"

mkdir -p "${PROTO_OUT}"

# Add syntax = "proto2" declaration to proto files (original files lack it but use proto2 features)
TMP_DIR=$(mktemp -d)
trap "rm -rf ${TMP_DIR}" EXIT

for proto_file in "${PROTO_SRC}/swift/protocol/"*.proto; do
    fname=$(basename "${proto_file}")
    tmp_file="${TMP_DIR}/${fname}"
    # Insert syntax declaration at the top of the file
    if ! grep -q "^syntax" "${proto_file}"; then
        echo 'syntax = "proto2";' > "${tmp_file}"
        cat "${proto_file}" >> "${tmp_file}"
    else
        cp "${proto_file}" "${tmp_file}"
    fi
done

echo "Generating Python proto files..."
protoc \
    --proto_path="${TMP_DIR}" \
    --python_out="${PROTO_OUT}" \
    "${TMP_DIR}"/*.proto

# Fix import paths in generated files (make relative imports work correctly)
for py_file in "${PROTO_OUT}"/*_pb2.py; do
    if [[ -f "${py_file}" ]]; then
        # Replace "import xxx_pb2" with "from . import xxx_pb2" (Python 3 relative imports)
        sed -i 's/^import \([a-z_]*_pb2\)/from . import \1/' "${py_file}"
        echo "Processed: $(basename ${py_file})"
    fi
done

echo "Proto files generated to: ${PROTO_OUT}"
echo "Generated files:"
ls -la "${PROTO_OUT}"/*.py 2>/dev/null || echo "  (No .py files found, please check if protoc is installed correctly)"
