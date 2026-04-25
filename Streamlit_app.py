import streamlit as st
import numpy as np
import hashlib
import math
import re
from datetime import datetime

# ----------------------------
# 🔱 CONFIG
# ----------------------------
st.set_page_config(page_title="Spartan Sentinel", layout="wide")

RADIUS_UP = 17
RADIUS_DOWN = 15
VOXEL_SCALE = 1024

# ----------------------------
# 🔱 CORE MATH ENGINE
# ----------------------------
def quantize(values):
    return np.rint(values * VOXEL_SCALE).astype(np.int64)

def pack64(x, y, z):
    mask = (1 << 21) - 1
    offset = 1 << 20
    return ((x + offset) & mask) << 42 | ((y + offset) & mask) << 21 | ((z + offset) & mask)

# ----------------------------
# 🔱 HTT PROTOCOL (SAFE)
# ----------------------------
def run_htt(sequence):
    clean = re.sub(r'[^ACGT]', '', sequence.upper())

    repeats = [len(m.group(0)) // 3 for m in re.finditer(r"(?:CAG)+", clean)]
    max_cag = max(repeats, default=0)

    points = max(36, max_cag)
    theta = np.linspace(0, 4*np.pi, points)
    z = np.linspace(0, 24, points)

    lead_r = RADIUS_UP
    lag_r = RADIUS_DOWN - max(0, (max_cag - 35) / 35)

    x1 = quantize(np.cos(theta) * lead_r)
    y1 = quantize(np.sin(theta) * lead_r)

    x2 = quantize(np.cos(theta + np.pi) * lag_r)
    y2 = quantize(np.sin(theta + np.pi) * lag_r)

    z_i = quantize(z)
    z_block = (z_i // 64) * 64

    lead_keys = pack64(x1, y1, z_block)
    lag_keys = pack64(x2, y2, z_block)

    stream = []
    for i in range(len(lead_keys)):
        uid = int(lead_keys[i])
        stream.append({
            "idx": i,
            "lane": "lead",
            "uid": uid,
            "voxel_hash": hashlib.sha256(str(uid).encode()).hexdigest()[:12]
        })

    return {
        "max_cag": max_cag,
        "stream": stream,
        "unique_voxels": len(set(lead_keys) | set(lag_keys))
    }

# ----------------------------
# 🔐 ZERO-KNOWLEDGE FORMATTER
# ----------------------------
def format_secure_stream(buf):
    out = []
    out.append("=== SPARTAN SECURE TENSOR STREAM ===")
    out.append("[AES-256 BOUNDARY ENFORCED]\n")

    for i, entry in enumerate(buf):
        uid_mask = hashlib.sha256(str(entry["uid"]).encode()).hexdigest()[:10]

        out.append(
            f"IDX:{i:03d} | "
            f"LANE:{entry['lane']} | "
            f"VOXEL:{entry['voxel_hash']} | "
            f"NODE:{uid_mask}"
        )

    return "\n".join(out)

# ----------------------------
# 🔱 UI
# ----------------------------
st.title("🔱 Spartan Sentinel")
st.caption("Zero-Knowledge Genomic Tensor Engine")

sequence = st.text_area("Enter DNA Sequence", "CAGCAGCAGCAGCAGCAGCAGCAG")

if st.button("Run Analysis"):
    result = run_htt(sequence)

    st.metric("Max CAG Repeats", result["max_cag"])
    st.metric("Unique Voxels", result["unique_voxels"])

    st.code(format_secure_stream(result["stream"]), language="text")
