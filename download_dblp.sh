#!/usr/bin/env bash

set -euo pipefail

BASE_URL="https://dblp.org/xml"
FILES=(
    "dblp.dtd"
    "dblp.xml.gz"
)

safe_wget() {
    local url="$1"
    local filename="$2"
    local tmpfile="${filename}.tmp"

    echo "⬇️  下载: ${filename}"

    if wget --progress=bar:force -O "$tmpfile" "$url"; then
        mv "$tmpfile" "$filename"
        echo "✅ 完成: ${filename}"
    else
        rm -f "$tmpfile"
        echo "❌ 失败: ${filename}" >&2
        return 1
    fi
}

for file in "${FILES[@]}"; do
    safe_wget "${BASE_URL}/${file}" "${file}"
done

echo "🎉 全部下载完成"
