#!/bin/bash
# 生成 Swift proto 文件的 Python 绑定
#
# 方式 1（推荐）：直接从 ha3_install 复制已生成的 pb2 文件
#   bash generate_proto.sh --from-ha3install
#
# 方式 2：使用 protoc 重新生成（需要 protoc 3.x）
#   bash generate_proto.sh
#
# ha3_install 中的 protobuf 运行时路径：
#   /home/xijie/havenask_120/ha3_install/usr/local/lib/python/site-packages
# 使用时需要设置：
#   export PYTHONPATH=/home/xijie/havenask_120/ha3_install/usr/local/lib/python/site-packages:$PYTHONPATH

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROTO_SRC="${SCRIPT_DIR}/../java_client/proto_files"
PROTO_OUT="${SCRIPT_DIR}/swift/proto"

mkdir -p "${PROTO_OUT}"

# 为 proto 文件添加 syntax = "proto2" 声明（原文件无此声明但使用 proto2 特性）
TMP_DIR=$(mktemp -d)
trap "rm -rf ${TMP_DIR}" EXIT

for proto_file in "${PROTO_SRC}/swift/protocol/"*.proto; do
    fname=$(basename "${proto_file}")
    tmp_file="${TMP_DIR}/${fname}"
    # 在文件头插入 syntax 声明
    if ! grep -q "^syntax" "${proto_file}"; then
        echo 'syntax = "proto2";' > "${tmp_file}"
        cat "${proto_file}" >> "${tmp_file}"
    else
        cp "${proto_file}" "${tmp_file}"
    fi
done

echo "生成 Python proto 文件..."
protoc \
    --proto_path="${TMP_DIR}" \
    --python_out="${PROTO_OUT}" \
    "${TMP_DIR}"/*.proto

# 修正生成文件中的 import 路径（使相对导入正常工作）
for py_file in "${PROTO_OUT}"/*_pb2.py; do
    if [[ -f "${py_file}" ]]; then
        # 将 "import xxx_pb2" 替换为 "from . import xxx_pb2"（Python 3 相对导入）
        sed -i 's/^import \([a-z_]*_pb2\)/from . import \1/' "${py_file}"
        echo "已处理: $(basename ${py_file})"
    fi
done

echo "Proto 文件已生成到: ${PROTO_OUT}"
echo "生成的文件:"
ls -la "${PROTO_OUT}"/*.py 2>/dev/null || echo "  (无 .py 文件，请检查 protoc 是否正确安装)"
