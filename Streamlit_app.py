import sys
import os
from pathlib import Path

# Force the root of the repository into the Python path
# On Streamlit Cloud, the root is typically /mount/src/spartanbio/
repo_root = Path(__file__).parent.absolute()
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

# NOW the engine can find your shared modules
from packages.shared.spartan_shared.engagement_engine import engage_protocols, parse_protocol_endpoints

import hashlib
import importlib.util
import json
import math
import sys
import os
from pathlib import Path

# Force the current directory into the Python path
# This ensures 'packages' is visible to the interpreter
current_dir = Path(__file__).parent.absolute()
if str(current_dir) not in sys.path:
    sys.path.insert(0, str(current_dir))
    
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import google.generativeai as genai
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import requests
import streamlit as st

from packages.shared.spartan_shared.engagement_engine import engage_protocols, parse_protocol_endpoints
from packages.shared.spartan_shared.bounty_contracts_engine import build_bounty_contract_report
from packages.shared.spartan_shared.conversation_engine import build_conversation_envelopes
from packages.shared.spartan_shared.sequence_intel_engine import build_sequence_intel_report
from packages.shared.spartan_shared.prospector_engine import build_income_prospector_report
from packages.shared.spartan_shared.repo_hunter_engine import build_repo_hunt_report

# ----------------------------
# 🔱 PHASE 1: FAIL-SAFE AI BRAIN
# ----------------------------
st.set_page_config(page_title="🔱 Spartan Sentinel V15.0", layout="wide")

# Runtime configuration
RADIUS_UP = int(os.getenv("RADIUS_UP", 17))
RADIUS_DOWN = int(os.getenv("RADIUS_DOWN", 15))
OVERSEER = os.getenv(
    "OVERSEER_ADDRESS",
    "agent1qvn0x2q3dgw6hwvl6cvmty3htn46x60qa6dz987kyyukv988w6emvqlzhee",
)
API_KEY = os.getenv("GOOGLE_API_KEY")
CLOUDFLARE_AGENT_URL = os.getenv("CLOUDFLARE_AGENT_URL", "")
RAILWAY_MESH_URL = os.getenv("RAILWAY_MESH_URL", "https://mineking-production.up.railway.app")
SEQUENCE_MINER_URL = os.getenv("SEQUENCE_MINER_URL", os.getenv("MINER_URL", RAILWAY_MESH_URL))
VERCEL_CONTROL_URL = os.getenv("VERCEL_CONTROL_URL", "")
SKYFIRE_API_KEY = os.getenv("SKYFIRE_API_KEY", "")
SKYFIRE_PAYMENT_URL = os.getenv("SKYFIRE_PAYMENT_URL", "")
AI_MODEL_PREFERENCE = os.getenv("AI_MODEL_PREFERENCE", "gemini-2.5-flash,gemini-2.0-flash,gemini-1.5-flash")
GXP_STRICT_MODE = os.getenv("GXP_STRICT_MODE", "True")
VOXEL_SCALE = int(os.getenv("VOXEL_SCALE", "1024"))
ACTIONS_AUDIT_PATH = os.getenv("ACTIONS_AUDIT_PATH", "data/sentinel_audit_log.jsonl")
PROSPECTOR_REPORT_PATH = os.getenv("PROSPECTOR_REPORT_PATH", "data/prospector_report.json")
REPO_HUNT_REPORT_PATH = os.getenv("REPO_HUNT_REPORT_PATH", "data/repo_hunt_report.json")
BOUNTY_CONTRACTS_PATH = os.getenv("BOUNTY_CONTRACTS_PATH", "data/bounty_contracts.json")
SEQUENCE_INTEL_REPORT_PATH = os.getenv("SEQUENCE_INTEL_REPORT_PATH", "data/sequence_intel_report.json")
AUTOMATION_LOG_PATH = os.getenv("AUTOMATION_LOG_PATH", "data/automation_cycles.jsonl")
SALES_LOG_PATH = os.getenv("SALES_LOG_PATH", "data/sales_events.jsonl")
OUTREACH_LOG_PATH = os.getenv("OUTREACH_LOG_PATH", "data/outreach_events.jsonl")
PAYMENT_PROVIDER = os.getenv("PAYMENT_PROVIDER", "Stripe Payment Links")
BOOKING_URL = os.getenv("BOOKING_URL", "")
OFFER_SCOUTING_URL = os.getenv("OFFER_SCOUTING_URL", "")
OFFER_AUTOMATION_URL = os.getenv("OFFER_AUTOMATION_URL", "")
OFFER_ENTERPRISE_URL = os.getenv("OFFER_ENTERPRISE_URL", "")
HUNT_PROTOCOL_ENDPOINTS = os.getenv("HUNT_PROTOCOL_ENDPOINTS", "")
HUNT_PROTOCOL_API_KEY = os.getenv("HUNT_PROTOCOL_API_KEY", "")
OUTREACH_PROTOCOL_ENDPOINTS = os.getenv("OUTREACH_PROTOCOL_ENDPOINTS", "")
OUTREACH_PROTOCOL_API_KEY = os.getenv("OUTREACH_PROTOCOL_API_KEY", HUNT_PROTOCOL_API_KEY)
BOUNTY_PROTOCOL_ENDPOINTS = os.getenv("BOUNTY_PROTOCOL_ENDPOINTS", OUTREACH_PROTOCOL_ENDPOINTS)
BOUNTY_PROTOCOL_API_KEY = os.getenv("BOUNTY_PROTOCOL_API_KEY", OUTREACH_PROTOCOL_API_KEY)
CONVERSATION_PROTOCOL_ENDPOINTS = os.getenv("CONVERSATION_PROTOCOL_ENDPOINTS", OUTREACH_PROTOCOL_ENDPOINTS)
CONVERSATION_PROTOCOL_API_KEY = os.getenv("CONVERSATION_PROTOCOL_API_KEY", OUTREACH_PROTOCOL_API_KEY)
BOUNTY_ACTION = os.getenv("BOUNTY_ACTION", "bounty_contract_consolidation")
CONVERSATION_ACTION = os.getenv("CONVERSATION_ACTION", "conversation_envelope_dispatch")
BOUNTY_CONTRACT_MAX_RESULTS = int(os.getenv("BOUNTY_CONTRACT_MAX_RESULTS", "25"))
CONVERSATION_MAX_TARGETS = int(os.getenv("CONVERSATION_MAX_TARGETS", "20"))
PIPELINE_VALUE_SPARTAN_USD = float(os.getenv("PIPELINE_VALUE_SPARTAN_USD", "29"))
PIPELINE_VALUE_PRO_USD = float(os.getenv("PIPELINE_VALUE_PRO_USD", "99"))
PIPELINE_VALUE_ENTERPRISE_USD = float(os.getenv("PIPELINE_VALUE_ENTERPRISE_USD", "499"))
PIPELINE_CLOSE_RATE_BOUNTY = float(os.getenv("PIPELINE_CLOSE_RATE_BOUNTY", "0.08"))
PIPELINE_CLOSE_RATE_REPO = float(os.getenv("PIPELINE_CLOSE_RATE_REPO", "0.03"))
PIPELINE_ENVELOPE_VALUE_USD = float(os.getenv("PIPELINE_ENVELOPE_VALUE_USD", "29"))
ORCHID_ID = os.getenv("ORCHID_ID", "spartan-orchid-01")
SEQUENCE_INPUT_PATHS = os.getenv("SEQUENCE_INPUT_PATHS", "")
SEQUENCE_ACCESSIONS = os.getenv("SEQUENCE_ACCESSIONS", "")
DEFAULT_SEQUENCE_ACCESSIONS = os.getenv("DEFAULT_SEQUENCE_ACCESSIONS", "NM_002111,NM_000492,NM_004006,NM_007294")
REFERENCE_GENOME_ID = os.getenv("REFERENCE_GENOME_ID", "GRCh38")
NCBI_EMAIL = os.getenv("NCBI_EMAIL", "")
NCBI_API_KEY = os.getenv("NCBI_API_KEY", "")
SEQUENCE_MAX_RECORDS = int(os.getenv("SEQUENCE_MAX_RECORDS", "100"))
AISTUDIO_HTT_ANALYZE_SOURCE = os.getenv(
    "AISTUDIO_HTT_ANALYZE_SOURCE",
    "https://raw.githubusercontent.com/Joedaddy66/Spartan-RD-HTT-Inference/main/analyze.py",
)
AISTUDIO_CRISPR_ANALYZE_SOURCE = os.getenv(
    "AISTUDIO_CRISPR_ANALYZE_SOURCE",
    "https://raw.githubusercontent.com/Joedaddy66/integer-resonance-crispr/main/analyze.py",
)
AISTUDIO_HTT_ACTIVE_LEADS_SOURCE = os.getenv(
    "AISTUDIO_HTT_ACTIVE_LEADS_SOURCE",
    "https://raw.githubusercontent.com/Joedaddy66/Spartan-RD-HTT-Inference/main/active_leads.json",
)
AISTUDIO_HTT_PAYOUT_SIGNAL_SOURCE = os.getenv(
    "AISTUDIO_HTT_PAYOUT_SIGNAL_SOURCE",
    "https://raw.githubusercontent.com/Joedaddy66/Spartan-RD-HTT-Inference/main/payout_signal.json",
)
AISTUDIO_HTT_SMOKE_DATA_SOURCE = os.getenv(
    "AISTUDIO_HTT_SMOKE_DATA_SOURCE",
    "https://raw.githubusercontent.com/Joedaddy66/Spartan-RD-HTT-Inference/main/data/ci_smoke_test_data.csv",
)
AISTUDIO_CRISPR_SMOKE_DATA_SOURCE = os.getenv(
    "AISTUDIO_CRISPR_SMOKE_DATA_SOURCE",
    "https://raw.githubusercontent.com/Joedaddy66/integer-resonance-crispr/main/data/ci_smoke_test_data.csv",
)
HTT_CAG_CARRIER_THRESHOLD = int(os.getenv("HTT_CAG_CARRIER_THRESHOLD", "36"))
HTT_PRECISION_APERTURE = int(os.getenv("HTT_PRECISION_APERTURE", "875000000"))
HTT_SETTLEMENT_AMOUNT_USD = float(os.getenv("HTT_SETTLEMENT_AMOUNT_USD", "250"))
DEAL_AGENT_ADDRESS = os.getenv("DEAL_AGENT_ADDRESS", "")
SKYFIRE_SELLER_GATEWAY_ACTION = os.getenv("SKYFIRE_SELLER_GATEWAY_ACTION", "seller_bounty_claim")
SKYFIRE_BOUNTY_UNITS = float(os.getenv("SKYFIRE_BOUNTY_UNITS", "250"))
SKYFIRE_TENSION_TRIGGER = float(os.getenv("SKYFIRE_TENSION_TRIGGER", "2.0"))
RAILWAY_OVERHEAD_USD = float(os.getenv("RAILWAY_OVERHEAD_USD", "0"))
AGENTVERSE_OVERHEAD_USD = float(os.getenv("AGENTVERSE_OVERHEAD_USD", "0"))
AUTONOMOUS_AGENT_MODE = os.getenv("AUTONOMOUS_AGENT_MODE", "true").lower() == "true"
AUTONOMOUS_LEAD_VOLUME = int(os.getenv("AUTONOMOUS_LEAD_VOLUME", "20"))
AUTONOMOUS_CYCLE_INTERVAL_SECONDS = int(os.getenv("AUTONOMOUS_CYCLE_INTERVAL_SECONDS", "900"))
AUTONOMOUS_GOLDEN_LIMIT = int(os.getenv("AUTONOMOUS_GOLDEN_LIMIT", "20"))

ENFORCED_RADIUS_UP = RADIUS_UP
ENFORCED_RADIUS_DOWN = RADIUS_DOWN
SKYFIRE_PING_BYTES = int(os.getenv("SKYFIRE_PING_BYTES", "1000000"))

# ----------------------------
# 🔱 SEQUENCE TRACKING ENGINE
# ----------------------------
SEQUENCE_TRACKER_PATH = os.getenv("SEQUENCE_TRACKER_PATH", "data/sequence_tracker.json")
SEQUENCE_VALUE_PER_RECORD = float(os.getenv("SEQUENCE_VALUE_PER_RECORD", PIPELINE_ENVELOPE_VALUE_USD))


def initialize_sequence_tracker():
    """Initialize or load sequence tracking data."""
    tracker_path = Path(SEQUENCE_TRACKER_PATH)
    tracker_path.parent.mkdir(parents=True, exist_ok=True)
    
    if tracker_path.exists():
        with open(tracker_path, 'r') as f:
            return json.load(f)
    
    return {
        "total_sequences_processed": 0,
        "total_value_usd": 0.0,
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "runs": []
    }


def update_sequence_tracker(sequence_count: int, run_id: str = None) -> dict:
    """Update sequence tracker with new run data."""
    tracker = initialize_sequence_tracker()
    
    value_added = sequence_count * SEQUENCE_VALUE_PER_RECORD
    tracker["total_sequences_processed"] += sequence_count
    tracker["total_value_usd"] += value_added
    tracker["last_updated"] = datetime.now(timezone.utc).isoformat()
    
    run_record = {
        "run_id": run_id or hashlib.sha256(tracker["last_updated"].encode()).hexdigest()[:8],
        "sequences": sequence_count,
        "value_usd": value_added,
        "timestamp": tracker["last_updated"]
    }
    tracker["runs"].append(run_record)
    
    tracker_path = Path(SEQUENCE_TRACKER_PATH)
    tracker_path.parent.mkdir(parents=True, exist_ok=True)
    with open(tracker_path, 'w') as f:
        json.dump(tracker, f, indent=2)
    
    return tracker


def get_sequence_tracker() -> dict:
    """Get current sequence tracking data."""
    return initialize_sequence_tracker()


class SpartanMonetizer:
    PRICING_TIERS = {
        "Spartan": 29.0,
        "Pro": 99.0,
        "Enterprise": 499.0,
    }

    def __init__(self, api_key: str, payment_url: str):
        self.api_key = api_key
        self.payment_url = payment_url

    def process_payment(self, tier: str, customer_ref: str, pay_token: str = "") -> dict[str, Any]:
        if tier not in self.PRICING_TIERS:
            raise ValueError(f"Unknown tier '{tier}'.")
        if not self.api_key:
            raise ValueError("Missing SKYFIRE_API_KEY.")
        if not self.payment_url:
            raise ValueError("Missing SKYFIRE_PAYMENT_URL.")

        payload = {
            "action": "verify_payment",
            "tier": tier,
            "amount_usd": self.PRICING_TIERS[tier],
            "customer_ref": customer_ref,
            "token": pay_token,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        response = requests.post(
            self.payment_url,
            headers={"content-type": "application/json", "x-skyfire-key": self.api_key},
            data=json.dumps(payload),
            timeout=20,
        )
        result = response.json() if response.text else {}
        if not response.ok:
            error_text = str(result.get("error") or result.get("detail") or response.text[:300]).strip()
            raise ValueError(f"Payment verification failed ({response.status_code}): {error_text}")
        verified = bool(result.get("paid") or result.get("verified") or result.get("status") == "paid")
        return {
            "verified": verified,
            "tier": tier,
            "amount_usd": self.PRICING_TIERS[tier],
            "customer_ref": customer_ref,
            "provider_response": result,
        }


class AutonomousMesh:
    CLASS_NAME = "AutonomousMesh"

    def __init__(self, identities: dict[str, str]):
        self.identities = self._normalize_identities(identities)

    @staticmethod
    def _normalize_identities(identities: dict[str, str]) -> dict[str, str]:
        normalized: dict[str, str] = {}
        for name, address in identities.items():
            label = (name or "").strip()
            wallet = (address or "").strip()
            if not label:
                raise ValueError("Mesh identity label cannot be empty.")
            if not wallet.startswith("agent1"):
                raise ValueError(f"Invalid mesh identity for '{label}': {wallet}")
            normalized[label] = wallet
        return normalized

    def registry(self) -> dict[str, str]:
        return dict(self.identities)

    def count(self) -> int:
        return len(self.identities)


@st.cache_resource
def init_spartan_brain(api_key: str | None):
    if not api_key:
        return None
    try:
        genai.configure(api_key=api_key)
        supported_models = [
            model
            for model in genai.list_models()
            if "generateContent" in getattr(model, "supported_generation_methods", [])
        ]
        if not supported_models:
            raise ValueError("No generateContent models available for this API key.")
        preferred_tokens = [token.strip() for token in AI_MODEL_PREFERENCE.split(",") if token.strip()]
        for token in preferred_tokens:
            for model in supported_models:
                if token in model.name:
                    return genai.GenerativeModel(model.name)
        return genai.GenerativeModel(supported_models[0].name)
    except Exception as err:  # API/library-specific handshake failures
        st.sidebar.error(f"AI Handshake Error: {str(err)[:160]}")
        return None


spartan_brain = init_spartan_brain(API_KEY)

# ----------------------------
# 🔱 PHASE 2: 12-AGENT REGISTRY
# ----------------------------
AUTONOMOUS_MESH_IDENTITIES = {
    "SPARTANMESH": "agent1qdpml9vhyeq85rjdve3t7vfwmcqpwussqvhf8fyh7ln0gz4rce62uza0y4t",
    "SPARTAN02": "agent1qv260ufhyz2nrjqejh9sx5r9e4kmu8qswuclkfp5ueqvp3uafzn07aevumf",
    "Spartan03": "agent1qgmy67q3w6t3r8fc98e94gztnekj5sjd4n3f84nahpmhx3kd3vgcytqqgv2",
    "SPOPS": "agent1qd6gnsqpj4wutgvdz5krjn3fvvdvkkg8a933s4zsek790al8uy5aylak8ac",
    "CLOUT": "agent1qf0jjt6w7rhfjsdv2z6epevsx3q6eljy6h7f9a3kqat4eu3r0sxux9ucevm",
    "genomic-db": "agent1q03aj6rz8f8tkze9t6rhjkfm93txndqh97ysww457lyzeu53u02xx5jpuq2",
    "nexus-mcp": "agent1qgaqmm4s69tf5ey0vfu828rxfx7ylvjfq9r2v9uc3e8zw25cfgkpxy0w5cm",
    "handler": "agent1qfzm0xml74s0gmh7s32qvjqspu6llr3zczz34k3rsd2yrkcfyzh02y6wg83",
    "watchdog": "agent1q0sr6pu3fz9yenwyrjek5zxdus5yu6n66a7npy7j7w5xgulchsnz5lygpxa",
    "spartan-db": "agent1qd3522v8tau8ufl5xpaxdna4l4uuqptxz07v525y5ldq2zksxqpjctu5340",
    "spartanmodelb": "agent1q2n2kygd8jstna7l3p50wervmh4dvnv5d8kjqgmuk7slc4rc250h6c4jdq0",
    "spartanmodela": "agent1qwc2scwgmucu32hk8z5tuf7mwnarhtpamnlevknqnetpyn59yxvry46vcrg",
}
AUTONOMOUS_MESH = AutonomousMesh(AUTONOMOUS_MESH_IDENTITIES)
AGENT_REGISTRY = AUTONOMOUS_MESH.registry()


# ----------------------------
# 🔱 PHASE 3: VOLUMETRIC HELIX ENGINE
# ----------------------------
def quantize_int64(values: np.ndarray, scale: int) -> np.ndarray:
    return np.rint(values * scale).astype(np.int64)


def pack_voxel_key64(x: np.ndarray, y: np.ndarray, z: np.ndarray) -> np.ndarray:
    # 21-bit signed lanes packed into one uint64: | x(21) | y(21) | z(21) |
    lane_mask = np.uint64((1 << 21) - 1)
    lane_offset = np.int64(1 << 20)
    ux = (x + lane_offset).astype(np.uint64) & lane_mask
    uy = (y + lane_offset).astype(np.uint64) & lane_mask
    uz = (z + lane_offset).astype(np.uint64) & lane_mask
    return (ux << np.uint64(42)) | (uy << np.uint64(21)) | uz


def deterministic_jitter(points: int) -> np.ndarray:
    seed = hashlib.sha256(f"{ENFORCED_RADIUS_UP}:{ENFORCED_RADIUS_DOWN}:{points}".encode("utf-8")).digest()
    repeated = (seed * ((points // len(seed)) + 1))[:points]
    raw = np.frombuffer(repeated, dtype=np.uint8).astype(np.int64)
    return (raw % 3) - 1  # [-1, 0, 1]


def build_symmetry_metrics(
    x1: np.ndarray, y1: np.ndarray, x2: np.ndarray, y2: np.ndarray
) -> dict[str, float]:
    r1 = np.sqrt(x1**2 + y1**2)
    r2 = np.sqrt(x2**2 + y2**2)
    phase1 = np.arctan2(y1, x1)
    phase2 = np.arctan2(y2, x2)
    phase_delta = np.angle(np.exp(1j * (phase2 - phase1)))
    return {
        "lead_radius_mae": float(np.mean(np.abs(r1 - ENFORCED_RADIUS_UP))),
        "lag_radius_mae": float(np.mean(np.abs(r2 - ENFORCED_RADIUS_DOWN))),
        "target_gap": float(ENFORCED_RADIUS_UP - ENFORCED_RADIUS_DOWN),
        "actual_gap_mean": float(np.mean(r1 - r2)),
        "phase_pi_error_mean": float(np.mean(np.abs(np.abs(phase_delta) - np.pi))),
    }


def build_financial_pulse(points: int, transaction_volume: float) -> np.ndarray:
    theta = np.linspace(0, 4 * np.pi, points)
    amplitude = np.clip(transaction_volume / 1000.0, 0.1, 3.0)
    return np.sin(theta) * amplitude + amplitude


def _add_voxel_cube_mesh(
    fig: go.Figure,
    center: tuple[float, float, float],
    size: float,
    color: str,
    name: str,
    opacity: float,
    showlegend: bool,
) -> None:
    cx, cy, cz = center
    half = size / 2.0
    x = np.array([cx - half, cx + half, cx + half, cx - half, cx - half, cx + half, cx + half, cx - half])
    y = np.array([cy - half, cy - half, cy + half, cy + half, cy - half, cy - half, cy + half, cy + half])
    z = np.array([cz - half, cz - half, cz - half, cz - half, cz + half, cz + half, cz + half, cz + half])
    i = [0, 0, 0, 1, 1, 2, 4, 4, 5, 5, 6, 6]
    j = [1, 2, 4, 2, 5, 3, 5, 6, 6, 1, 7, 2]
    k = [2, 4, 1, 5, 2, 7, 6, 7, 1, 4, 2, 3]
    fig.add_trace(
        go.Mesh3d(
            x=x,
            y=y,
            z=z,
            i=i,
            j=j,
            k=k,
            color=color,
            opacity=opacity,
            flatshading=True,
            showscale=False,
            name=name,
            showlegend=showlegend,
            hovertemplate="Voxel cube<extra></extra>",
        )
    )


def create_voxel_helix(
    points: int = 200, transaction_volume: float = 0.0, tension_marker: dict[str, Any] | None = None
) -> tuple[go.Figure, dict[str, float]]:
    theta = np.linspace(0, 8 * np.pi, points)
    z = np.linspace(0, 50, points)

    x1_f = np.cos(theta) * ENFORCED_RADIUS_UP
    y1_f = np.sin(theta) * ENFORCED_RADIUS_UP

    jitter = deterministic_jitter(points)
    x2_f = np.cos(theta + np.pi) * ENFORCED_RADIUS_DOWN + (jitter / VOXEL_SCALE)
    y2_f = np.sin(theta + np.pi) * ENFORCED_RADIUS_DOWN

    x1_i = quantize_int64(x1_f, VOXEL_SCALE)
    y1_i = quantize_int64(y1_f, VOXEL_SCALE)
    x2_i = quantize_int64(x2_f, VOXEL_SCALE)
    y2_i = quantize_int64(y2_f, VOXEL_SCALE)
    z_i = quantize_int64(z, VOXEL_SCALE)
    z_block_i = (z_i // np.int64(64)) * np.int64(64)

    lead_keys = pack_voxel_key64(x1_i, y1_i, z_block_i)
    lag_keys = pack_voxel_key64(x2_i, y2_i, z_block_i)
    metrics = build_symmetry_metrics(
        x1_i / VOXEL_SCALE,
        y1_i / VOXEL_SCALE,
        x2_i / VOXEL_SCALE,
        y2_i / VOXEL_SCALE,
    )
    metrics["voxel_points"] = float(points * 2)
    metrics["unique_voxels"] = float(np.unique(np.concatenate([lead_keys, lag_keys])).size)
    metrics["z_block_size"] = 64.0
    metrics["financial_volume"] = float(transaction_volume)

    fig = go.Figure()
    fig.add_trace(
        go.Scatter3d(
            x=x1_i / VOXEL_SCALE,
            y=y1_i / VOXEL_SCALE,
            z=z_block_i / VOXEL_SCALE,
            mode="lines",
            line=dict(color="#00ff00", width=8),
            name=f"Lead ({ENFORCED_RADIUS_UP})",
        )
    )
    fig.add_trace(
        go.Scatter3d(
            x=x2_i / VOXEL_SCALE,
            y=y2_i / VOXEL_SCALE,
            z=z_block_i / VOXEL_SCALE,
            mode="lines",
            line=dict(color="#57236f", width=6),
            name=f"Lag ({ENFORCED_RADIUS_DOWN}) Base",
        )
    )
    fig.add_trace(
        go.Scatter3d(
            x=x2_i / VOXEL_SCALE,
            y=y2_i / VOXEL_SCALE,
            z=z_block_i / VOXEL_SCALE,
            mode="markers",
            marker=dict(
                size=4,
                color=build_financial_pulse(points, transaction_volume),
                colorscale="Turbo",
                showscale=True,
                colorbar=dict(title="Financial Pulse"),
            ),
            name=f"Lag ({ENFORCED_RADIUS_DOWN}) Financial Pulse",
        )
    )

    # Render stacked voxel cubes per z-block so cube alignment is visible in 3D.
    unique_z_blocks = np.unique(z_block_i)
    max_layers = min(12, len(unique_z_blocks))
    layer_indices = np.linspace(0, len(unique_z_blocks) - 1, max_layers, dtype=int)
    sampled_layers = unique_z_blocks[layer_indices]
    alignment_distances: list[float] = []
    cube_size = 64.0 / VOXEL_SCALE
    lead_legend = True
    lag_legend = True
    for layer in sampled_layers:
        layer_mask = z_block_i == layer
        layer_pos = np.where(layer_mask)[0]
        if layer_pos.size == 0:
            continue
        idx = int(layer_pos[len(layer_pos) // 2])
        lead_center = (float(x1_i[idx] / VOXEL_SCALE), float(y1_i[idx] / VOXEL_SCALE), float(layer / VOXEL_SCALE))
        lag_center = (float(x2_i[idx] / VOXEL_SCALE), float(y2_i[idx] / VOXEL_SCALE), float(layer / VOXEL_SCALE))
        alignment_distances.append(float(np.sqrt((lead_center[0] - lag_center[0]) ** 2 + (lead_center[1] - lag_center[1]) ** 2)))
        _add_voxel_cube_mesh(
            fig=fig,
            center=lead_center,
            size=cube_size,
            color="#25d366",
            name="Lead voxel cubes",
            opacity=0.22,
            showlegend=lead_legend,
        )
        _add_voxel_cube_mesh(
            fig=fig,
            center=lag_center,
            size=cube_size,
            color="#9c6cff",
            name="Lag voxel cubes",
            opacity=0.22,
            showlegend=lag_legend,
        )
        lead_legend = False
        lag_legend = False

    if alignment_distances:
        metrics["stack_layers_visualized"] = float(len(alignment_distances))
        metrics["stack_alignment_mean"] = float(np.mean(alignment_distances))
        metrics["stack_alignment_max"] = float(np.max(alignment_distances))

    agent_z = np.linspace(5, 45, len(AGENT_REGISTRY))
    fig.add_trace(
        go.Scatter3d(
            x=np.cos(agent_z) * 16,
            y=np.sin(agent_z) * 16,
            z=agent_z,
            mode="markers+text",
            text=list(AGENT_REGISTRY.keys()),
            marker=dict(size=8, color="#ffffff", symbol="diamond"),
            name="Active Agents",
        )
    )
    if tension_marker:
        fig.add_trace(
            go.Scatter3d(
                x=[float(tension_marker["x"])],
                y=[float(tension_marker["y"])],
                z=[float(tension_marker["z"])],
                mode="markers+text",
                text=["🔱 HIGH-TENSION"],
                textposition="top center",
                marker=dict(size=12, color="#ff2d2d", symbol="x"),
                name="High-Tension Alert",
            )
        )

    fig.update_layout(
        template="plotly_dark",
        margin=dict(l=0, r=0, b=0, t=0),
        scene=dict(
            aspectmode="data",
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            zaxis=dict(showgrid=True, title="Voxel Depth (int64 quantized)"),
        ),
    )
    return fig, metrics


def append_audit_record(action: str, payload: dict[str, Any], status: str, response: dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(ACTIONS_AUDIT_PATH), exist_ok=True)
    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "action": action,
        "status": status,
        "operator": os.getenv("OPERATOR_ID", "unknown"),
        "alcoa_plus": "Compliant" if GXP_STRICT_MODE.lower() == "true" else "Not-Enforced",
        "payload_hash": hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest(),
        "response_hash": hashlib.sha256(json.dumps(response, sort_keys=True).encode("utf-8")).hexdigest(),
    }
    with open(ACTIONS_AUDIT_PATH, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(record) + "\n")


def dispatch_control_event(base_url: str, action: str, payload: dict[str, Any]) -> dict[str, Any]:
    resolved_base_url = normalize_endpoint_url(base_url)
    if not resolved_base_url:
        raise ValueError(f"Missing endpoint for action '{action}'.")
    relay_path = "/skyfire/relay"
    resolved_no_slash = resolved_base_url.rstrip("/")
    relay_url = resolved_no_slash if resolved_no_slash.endswith(relay_path) else f"{resolved_no_slash}{relay_path}"
    headers = {"Content-Type": "application/json"}
    if SKYFIRE_API_KEY:
        headers["X-SKYFIRE-KEY"] = SKYFIRE_API_KEY
    response = requests.post(
        relay_url,
        headers=headers,
        data=json.dumps({"action": action, "payload": payload}),
        timeout=15,
    )
    response.raise_for_status()
    try:
        return response.json()
    except ValueError:
        return {"raw_response": response.text[:500]}


def persist_prospector_report(report: dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(PROSPECTOR_REPORT_PATH), exist_ok=True)
    with open(PROSPECTOR_REPORT_PATH, "w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2)


def load_prospector_report() -> dict[str, Any] | None:
    if not os.path.exists(PROSPECTOR_REPORT_PATH):
        return None
    with open(PROSPECTOR_REPORT_PATH, "r", encoding="utf-8") as handle:
        return json.load(handle)


def persist_repo_hunt_report(report: dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(REPO_HUNT_REPORT_PATH), exist_ok=True)
    with open(REPO_HUNT_REPORT_PATH, "w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2)


def load_repo_hunt_report() -> dict[str, Any] | None:
    if not os.path.exists(REPO_HUNT_REPORT_PATH):
        return None
    with open(REPO_HUNT_REPORT_PATH, "r", encoding="utf-8") as handle:
        return json.load(handle)


def persist_bounty_contract_report(report: dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(BOUNTY_CONTRACTS_PATH), exist_ok=True)
    with open(BOUNTY_CONTRACTS_PATH, "w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2)


def load_bounty_contract_report() -> dict[str, Any] | None:
    if not os.path.exists(BOUNTY_CONTRACTS_PATH):
        return None
    with open(BOUNTY_CONTRACTS_PATH, "r", encoding="utf-8") as handle:
        return json.load(handle)


def persist_sequence_intel_report(report: dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(SEQUENCE_INTEL_REPORT_PATH), exist_ok=True)
    with open(SEQUENCE_INTEL_REPORT_PATH, "w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2)


def load_sequence_intel_report() -> dict[str, Any] | None:
    if not os.path.exists(SEQUENCE_INTEL_REPORT_PATH):
        return None
    with open(SEQUENCE_INTEL_REPORT_PATH, "r", encoding="utf-8") as handle:
        return json.load(handle)


def ensure_sequence_intel_report(force_refresh: bool = False) -> dict[str, Any]:
    cached = load_sequence_intel_report()
    if not force_refresh and cached and int(cached.get("totals", {}).get("sequence_records") or 0) > 0:
        return cached
    fresh = build_sequence_intel_report(
        orchid_id=ORCHID_ID,
        raw_input_paths=SEQUENCE_INPUT_PATHS,
        raw_accessions=configured_sequence_accessions(),
        ncbi_email=NCBI_EMAIL,
        ncbi_api_key=NCBI_API_KEY,
        max_records=SEQUENCE_MAX_RECORDS,
    )
    persist_sequence_intel_report(fresh)
    return fresh


def is_htt_record(record: dict[str, Any]) -> bool:
    description = str(record.get("description", "")).lower()
    return "huntingtin" in description or " htt" in f" {description}" or "chromosome 4" in description


def build_htt_stream_snapshot(sequence_report: dict[str, Any] | None) -> dict[str, Any]:
    if not sequence_report:
        return {"chromosome": 4, "records": [], "record_count": 0, "raw_nucleotide_count": 0}
    records = [rec for rec in sequence_report.get("records", []) if is_htt_record(rec)]
    raw_nucleotide_count = sum(int(rec.get("length") or 0) for rec in records)
    return {
        "chromosome": 4,
        "records": records,
        "record_count": len(records),
        "raw_nucleotide_count": raw_nucleotide_count,
    }


def is_htt_remote_record(record: dict[str, Any]) -> bool:
    sequence = str(record.get("sequence", "")).upper()
    region = str(record.get("region", "")).lower()
    return "CAG" in sequence or "htt" in region or "huntingtin" in region


def fetch_sequence_miner_snapshot(base_url: str, limit: int = 100) -> dict[str, Any]:
    endpoint = normalize_endpoint_url(base_url).rstrip("/")
    if not endpoint:
        return {"online": False, "error": "SEQUENCE_MINER_URL is not configured."}
    try:
        stats_resp = requests.get(f"{endpoint}/stats", timeout=12)
        stats_resp.raise_for_status()
        stats_payload = stats_resp.json()

        recent_resp = requests.get(f"{endpoint}/sequences/recent?limit={max(1, min(200, limit))}", timeout=12)
        recent_resp.raise_for_status()
        recent_payload = recent_resp.json()
        records = recent_payload.get("records", []) if isinstance(recent_payload, dict) else []
        htt_records = [record for record in records if is_htt_remote_record(record)]
        raw_nucleotide_count = sum(len(str(record.get("sequence", "")).strip()) for record in records)
        return {
            "online": True,
            "endpoint": endpoint,
            "stats": stats_payload if isinstance(stats_payload, dict) else {},
            "records": records,
            "htt_stream_snapshot": {
                "chromosome": 4,
                "records": htt_records,
                "record_count": len(htt_records),
                "raw_nucleotide_count": raw_nucleotide_count,
            },
        }
    except requests.RequestException as err:
        return {"online": False, "endpoint": endpoint, "error": str(err)}


def append_automation_cycle(cycle: dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(AUTOMATION_LOG_PATH), exist_ok=True)
    with open(AUTOMATION_LOG_PATH, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(cycle) + "\n")


def load_automation_cycles(limit: int = 25) -> list[dict[str, Any]]:
    if not os.path.exists(AUTOMATION_LOG_PATH):
        return []
    with open(AUTOMATION_LOG_PATH, "r", encoding="utf-8") as handle:
        lines = [line.strip() for line in handle.readlines() if line.strip()]
    return [json.loads(line) for line in lines[-limit:]][::-1]


def append_outreach_event(event: dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(OUTREACH_LOG_PATH), exist_ok=True)
    with open(OUTREACH_LOG_PATH, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(event) + "\n")


def load_outreach_events(limit: int = 100) -> list[dict[str, Any]]:
    if not os.path.exists(OUTREACH_LOG_PATH):
        return []
    with open(OUTREACH_LOG_PATH, "r", encoding="utf-8") as handle:
        lines = [line.strip() for line in handle.readlines() if line.strip()]
    return [json.loads(line) for line in lines[-limit:]][::-1]


def append_sales_event(event: dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(SALES_LOG_PATH), exist_ok=True)
    with open(SALES_LOG_PATH, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(event) + "\n")


def load_sales_events(limit: int = 50) -> list[dict[str, Any]]:
    if not os.path.exists(SALES_LOG_PATH):
        return []
    with open(SALES_LOG_PATH, "r", encoding="utf-8") as handle:
        lines = [line.strip() for line in handle.readlines() if line.strip()]
    return [json.loads(line) for line in lines[-limit:]][::-1]


def calculate_financial_volume(limit: int = 500) -> float:
    events = load_sales_events(limit=limit)
    total = 0.0
    for event in events:
        if event.get("event_type") in {
            "payment_received",
            "skyfire_payment_verified",
            "skyfire_bounty_settled",
            "diagnostic_payout_settled",
        }:
            total += float(event.get("amount_usd") or 0.0)
    return total


def build_outreach_targets(bounty_contract_report: dict[str, Any]) -> list[dict[str, Any]]:
    targets: list[dict[str, Any]] = []
    for contract in bounty_contract_report.get("contracts", [])[:20]:
        targets.append(
            {
                "type": contract.get("source_type"),
                "title": contract.get("title"),
                "url": contract.get("url"),
                "repo": contract.get("repo"),
                "language": contract.get("language"),
                "score": float(contract.get("score") or 0.0),
                "offer_tier": contract.get("offer_tier"),
                "offer_value_usd": float(contract.get("contract_value_usd") or 0.0),
                "expected_value_usd": float(contract.get("expected_value_usd") or 0.0),
            }
        )
    return targets


def estimate_delivered_envelopes(event: dict[str, Any]) -> int:
    explicit = event.get("delivered_envelopes")
    if explicit is not None:
        return int(explicit or 0)
    target_count = int(event.get("target_count") or 0)
    result = event.get("result") or {}
    attempted = int(result.get("attempted") or 0)
    successful = int(result.get("successful") or 0)
    if attempted <= 0:
        return 0
    ratio = max(0.0, min(1.0, successful / attempted))
    return int(round(target_count * ratio))


def run_money_machine_cycle(max_results: int, *, has_checkout: bool) -> dict[str, Any]:
    report = build_income_prospector_report(max_results=max_results)
    persist_prospector_report(report)
    repo_hunt_report = build_repo_hunt_report(max_results=max_results)
    persist_repo_hunt_report(repo_hunt_report)
    bounty_contract_report = build_bounty_contract_report(
        report,
        repo_hunt_report,
        max_contracts=BOUNTY_CONTRACT_MAX_RESULTS,
        value_spartan_usd=PIPELINE_VALUE_SPARTAN_USD,
        value_pro_usd=PIPELINE_VALUE_PRO_USD,
        value_enterprise_usd=PIPELINE_VALUE_ENTERPRISE_USD,
        close_rate_bounty=PIPELINE_CLOSE_RATE_BOUNTY,
        close_rate_repo=PIPELINE_CLOSE_RATE_REPO,
    )
    persist_bounty_contract_report(bounty_contract_report)
    bounty_dispatch_result = engage_protocols(
        action=BOUNTY_ACTION,
        payload={
            "contracts": bounty_contract_report["contracts"],
            "totals": bounty_contract_report["totals"],
            "offers": ["Spartan", "Pro", "Enterprise"],
        },
        endpoints=parse_protocol_endpoints(BOUNTY_PROTOCOL_ENDPOINTS),
        api_key=BOUNTY_PROTOCOL_API_KEY,
    )
    outreach_targets = build_outreach_targets(bounty_contract_report)
    conversation_envelopes = build_conversation_envelopes(outreach_targets, max_targets=CONVERSATION_MAX_TARGETS)
    conversation_result = engage_protocols(
        action=CONVERSATION_ACTION,
        payload={"envelopes": conversation_envelopes, "count": len(conversation_envelopes)},
        endpoints=parse_protocol_endpoints(CONVERSATION_PROTOCOL_ENDPOINTS),
        api_key=CONVERSATION_PROTOCOL_API_KEY,
    )
    outreach_result = engage_protocols(
        action="outreach_sequence_dispatch",
        payload={"targets": outreach_targets, "offers": ["Spartan", "Pro", "Enterprise"], "booking_url": BOOKING_URL},
        endpoints=parse_protocol_endpoints(OUTREACH_PROTOCOL_ENDPOINTS),
        api_key=OUTREACH_PROTOCOL_API_KEY,
    )
    append_outreach_event(
        {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "event_type": "bounty_contract_dispatch",
            "contract_count": int(bounty_contract_report["totals"]["contract_count"]),
            "contract_value_usd": float(bounty_contract_report["totals"]["contract_value_usd"]),
            "expected_value_usd": float(bounty_contract_report["totals"]["expected_value_usd"]),
            "result": bounty_dispatch_result,
        }
    )
    target_count = len(outreach_targets)
    delivered_envelopes = int(
        round(
            target_count
            * (
                max(
                    0.0,
                    min(
                        1.0,
                        int(outreach_result.get("successful") or 0) / max(int(outreach_result.get("attempted") or 1), 1),
                    ),
                )
            )
        )
    )
    append_outreach_event(
        {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "event_type": "conversation_dispatch",
            "target_count": len(outreach_targets),
            "envelope_count": len(conversation_envelopes),
            "result": conversation_result,
        }
    )
    append_outreach_event(
        {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "event_type": "outreach_dispatch",
            "sequence_runs": 1,
            "target_count": target_count,
            "delivered_envelopes": delivered_envelopes,
            "sequence_potential_value_usd": round(target_count * PIPELINE_ENVELOPE_VALUE_USD, 2),
            "sequence_realized_value_usd": round(delivered_envelopes * PIPELINE_ENVELOPE_VALUE_USD, 2),
            "pipeline_contract_value_usd": round(
                sum(float(target.get("offer_value_usd") or 0.0) for target in outreach_targets), 2
            ),
            "pipeline_expected_value_usd": round(
                sum(float(target.get("expected_value_usd") or 0.0) for target in outreach_targets), 2
            ),
            "result": outreach_result,
        }
    )
    event = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event_type": "money_machine_engaged",
        "qualified_leads": report["totals"]["combined_leads"],
        "checkout_links_online": has_checkout,
        "booking_online": bool(BOOKING_URL),
        "bounty_contract_count": int(bounty_contract_report["totals"]["contract_count"]),
        "conversation_envelope_count": len(conversation_envelopes),
        "outreach_successful": outreach_result.get("successful", 0),
    }
    append_sales_event(event)
    append_audit_record("money_machine_engaged", {"max_results": max_results, "payment_provider": PAYMENT_PROVIDER}, "success", event)
    return {
        "combined_leads": int(report["totals"]["combined_leads"]),
        "target_count": target_count,
        "delivered_envelopes": delivered_envelopes,
    }


def build_revenue_scoreboard() -> dict[str, float]:
    sales_events = load_sales_events(limit=500)
    outreach_events = load_outreach_events(limit=500)
    sequence_runs = sum(int(event.get("sequence_runs") or 0) for event in outreach_events if event.get("event_type") == "outreach_dispatch")
    outreach_dispatches = [event for event in outreach_events if event.get("event_type") == "outreach_dispatch"]
    outreach_sent = sum(int(event.get("target_count") or 0) for event in outreach_dispatches)
    delivered_envelopes = sum(estimate_delivered_envelopes(event) for event in outreach_dispatches)
    sequence_potential_value = sum(float(event.get("sequence_potential_value_usd") or 0.0) for event in outreach_dispatches)
    sequence_realized_value = sum(float(event.get("sequence_realized_value_usd") or 0.0) for event in outreach_dispatches)
    if sequence_potential_value == 0.0:
        sequence_potential_value = float(outreach_sent) * PIPELINE_ENVELOPE_VALUE_USD
    if sequence_realized_value == 0.0:
        sequence_realized_value = float(delivered_envelopes) * PIPELINE_ENVELOPE_VALUE_USD
    pipeline_contract_value = sum(
        float(event.get("pipeline_contract_value_usd") or 0.0)
        for event in outreach_events
        if event.get("event_type") == "outreach_dispatch"
    )
    pipeline_expected_value = sum(
        float(event.get("pipeline_expected_value_usd") or 0.0)
        for event in outreach_events
        if event.get("event_type") == "outreach_dispatch"
    )
    bounty_dispatch_events = [event for event in outreach_events if event.get("event_type") == "bounty_contract_dispatch"]
    bounty_contract_count = sum(int(event.get("contract_count") or 0) for event in bounty_dispatch_events)
    bounty_contract_value = sum(float(event.get("contract_value_usd") or 0.0) for event in bounty_dispatch_events)
    bounty_expected_value = sum(float(event.get("expected_value_usd") or 0.0) for event in bounty_dispatch_events)
    conversation_events = [event for event in outreach_events if event.get("event_type") == "conversation_dispatch"]
    conversation_envelopes = sum(int(event.get("envelope_count") or 0) for event in conversation_events)
    reply_events = sum(1 for event in outreach_events if event.get("event_type") == "outreach_reply")
    booking_events = sum(1 for event in outreach_events if event.get("event_type") == "booking_created")
    paid_events = [
        event
        for event in sales_events
        if event.get("event_type") in {"payment_received", "skyfire_bounty_settled", "diagnostic_payout_settled"}
    ]
    paid_count = len(paid_events)
    cash_revenue = sum(float(event.get("amount_usd") or 0.0) for event in paid_events)
    sequence_leakage = max(sequence_potential_value - sequence_realized_value, 0.0)
    expected_leakage = max(pipeline_expected_value - sequence_realized_value, 0.0)
    contract_gap = max(pipeline_contract_value - sequence_realized_value, 0.0)
    return {
        "sequence_runs": float(sequence_runs),
        "outreach_sent": float(outreach_sent),
        "delivered_envelopes": float(delivered_envelopes),
        "replies": float(reply_events),
        "bookings": float(booking_events),
        "paid_conversions": float(paid_count),
        "revenue_usd": float(sequence_realized_value),
        "cash_revenue_usd": float(cash_revenue),
        "sequence_potential_value_usd": float(sequence_potential_value),
        "sequence_leakage_usd": float(sequence_leakage),
        "pipeline_contract_value_usd": float(pipeline_contract_value),
        "pipeline_expected_value_usd": float(pipeline_expected_value),
        "bounty_contract_count": float(bounty_contract_count),
        "bounty_contract_value_usd": float(bounty_contract_value),
        "bounty_expected_value_usd": float(bounty_expected_value),
        "conversation_envelopes": float(conversation_envelopes),
        "expected_leakage_usd": float(expected_leakage),
        "contract_gap_usd": float(contract_gap),
        "reply_rate_pct": float((reply_events / outreach_sent) * 100) if outreach_sent else 0.0,
        "booking_rate_pct": float((booking_events / outreach_sent) * 100) if outreach_sent else 0.0,
        "close_rate_pct": float((paid_count / booking_events) * 100) if booking_events else 0.0,
        "revenue_per_outreach": float(sequence_realized_value / outreach_sent) if outreach_sent else 0.0,
    }


def build_profit_and_loss() -> dict[str, float]:
    sales_events = load_sales_events(limit=1000)
    outreach_events = load_outreach_events(limit=1000)
    settled_bounty_events = [
        event
        for event in sales_events
        if event.get("event_type") in {"skyfire_bounty_settled", "diagnostic_payout_settled"}
    ]
    settled_bounty_total = sum(float(event.get("amount_usd") or 0.0) for event in settled_bounty_events)
    identified_carriers = [event for event in outreach_events if event.get("event_type") == "tier1_carrier_identified"]
    settled_by_lead = {
        str(event.get("lead_ref") or "").strip() for event in settled_bounty_events if str(event.get("lead_ref") or "").strip()
    }
    identified_with_ref = 0
    settled_from_identified = 0
    for event in identified_carriers:
        lead_ref = str(event.get("lead_ref") or "").strip()
        if lead_ref:
            identified_with_ref += 1
            if lead_ref in settled_by_lead:
                settled_from_identified += 1
    identified_without_ref = len(identified_carriers) - identified_with_ref
    settled_without_ref = max(0, len(settled_bounty_events) - settled_from_identified)
    pending_leads = max(0, (identified_with_ref - settled_from_identified) + (identified_without_ref - settled_without_ref))
    operational_cost = float(RAILWAY_OVERHEAD_USD + AGENTVERSE_OVERHEAD_USD)
    return {
        "total_revenue_usd": float(settled_bounty_total),
        "pending_leads": float(pending_leads),
        "operational_cost_usd": float(operational_cost),
    }


def claim_skyfire_seller_bounty(
    *,
    carrier_result: dict[str, Any],
    manifest_event: dict[str, Any] | None,
    agent: str,
) -> dict[str, Any]:
    lead_ref = str(
        (manifest_event or {}).get("lead_ref")
        or (manifest_event or {}).get("precision_fingerprint")
        or ((manifest_event or {}).get("stretch_voxel") or {}).get("uid64_hex")
        or ""
    ).strip()
    bounty_payload = {
        "event": "high_tension_bounty_claim",
        "trigger": {
            "lead_radius": ENFORCED_RADIUS_UP,
            "lag_radius": ENFORCED_RADIUS_DOWN,
            "tension_threshold": SKYFIRE_TENSION_TRIGGER,
            "sync_variance_v": float(carrier_result.get("sync_variance_v") or 0.0),
        },
        "amount_units": SKYFIRE_BOUNTY_UNITS,
        "amount_usd": SKYFIRE_BOUNTY_UNITS,
        "agent": agent,
        "lead_ref": lead_ref,
        "manifest_event": manifest_event or {},
    }
    result = dispatch_control_event(RAILWAY_MESH_URL, SKYFIRE_SELLER_GATEWAY_ACTION, bounty_payload)
    status = str(result.get("status") or "").lower()
    settled = bool(result.get("settled") or result.get("paid") or status in {"paid", "settled", "completed", "success"})
    append_sales_event(
        {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "event_type": "skyfire_bounty_settled" if settled else "skyfire_bounty_claim_requested",
            "amount_units": SKYFIRE_BOUNTY_UNITS,
            "amount_usd": SKYFIRE_BOUNTY_UNITS,
            "agent": agent,
            "lead_ref": lead_ref,
            "tension_sync_variance_v": float(carrier_result.get("sync_variance_v") or 0.0),
            "result": result,
        }
    )
    append_audit_record("skyfire_seller_bounty_claim", bounty_payload, "success", result)
    return {"settled": settled, "result": result}


def build_manifest_v2(symmetry_metrics: dict[str, float], htt_protocol: dict[str, Any] | None = None) -> dict[str, Any]:
    gxp_timestamp = datetime.now(timezone.utc).isoformat()
    manifest = {
        "project": "Fission Pharma",
        "schema": "spartan-sentinel-v15",
        "overseer": OVERSEER,
        "timestamp": gxp_timestamp,
        "gxp_timestamp_utc": gxp_timestamp,
        "time_standard": "ISO-8601 UTC",
        "alcoa_plus": "Compliant" if GXP_STRICT_MODE.lower() == "true" else "Not-Enforced",
        "alcoa_plus_audit_markers": {
            "attributable": True,
            "legible": True,
            "contemporaneous": True,
            "original": True,
            "accurate": True,
            "complete": True,
            "consistent": True,
            "enduring": True,
            "available": True,
        },
        "radii": {"lead": ENFORCED_RADIUS_UP, "lag": ENFORCED_RADIUS_DOWN},
        "voxel_architecture": {"bits": 64, "scale": VOXEL_SCALE, "z_block_size": 64},
        "symmetry_metrics": symmetry_metrics,
        "execution_endpoints": {
            "cloudflare": CLOUDFLARE_AGENT_URL,
            "railway": RAILWAY_MESH_URL,
            "vercel": VERCEL_CONTROL_URL,
        },
        "agent_registry_size": len(AGENT_REGISTRY),
        "assigned_agents": count_assigned_agents(),
    }
    if htt_protocol:
        manifest["htt_carrier_protocol"] = htt_protocol
    return manifest


def count_assigned_agents() -> int:
    return len(AGENT_REGISTRY)


@st.cache_resource
def load_aistudio_analyzer(source: str):
    module_name = f"aistudio_{hashlib.sha256(source.encode('utf-8')).hexdigest()[:12]}"
    if source.startswith("http://") or source.startswith("https://"):
        response = requests.get(source, timeout=20)
        response.raise_for_status()
        namespace: dict[str, Any] = {"__name__": module_name}
        exec(compile(response.text, source, "exec"), namespace)
        analyzer = namespace.get("analyze_sequence_for_score")
    else:
        path = Path(source)
        if not path.exists():
            raise FileNotFoundError(f"Analyzer source not found: {source}")
        spec = importlib.util.spec_from_file_location(module_name, str(path))
        if spec is None or spec.loader is None:
            raise ImportError(f"Cannot load analyzer module at {source}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        analyzer = getattr(module, "analyze_sequence_for_score", None)

    if not callable(analyzer):
        raise AttributeError(f"'analyze_sequence_for_score' not found in {source}")
    return analyzer


def read_json_source(source: str) -> Any | None:
    if source.startswith("http://") or source.startswith("https://"):
        response = requests.get(source, timeout=20)
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json()
    path = Path(source)
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def read_smoke_rows(source: str) -> int:
    if source.startswith("http://") or source.startswith("https://"):
        response = requests.get(source, timeout=20)
        if response.status_code == 404:
            return 0
        response.raise_for_status()
        frame = pd.read_csv(StringIO(response.text))
        return int(len(frame))
    path = Path(source)
    if not path.exists():
        return 0
    frame = pd.read_csv(path)
    return int(len(frame))


def source_online(source: str) -> bool:
    if source.startswith("http://") or source.startswith("https://"):
        try:
            response = requests.get(source, timeout=10)
            return response.ok
        except requests.RequestException:
            return False
    return Path(source).exists()


def normalize_endpoint_url(raw: str) -> str:
    candidate = (raw or "").strip()
    if not candidate:
        return ""
    parsed = urlparse(candidate)
    if parsed.scheme in {"http", "https"}:
        return candidate
    if ".railway.internal" in candidate:
        return f"http://{candidate}"
    return f"https://{candidate}"


def configured_sequence_accessions() -> str:
    return SEQUENCE_ACCESSIONS.strip() or DEFAULT_SEQUENCE_ACCESSIONS


def sanitize_dna(sequence: str) -> str:
    return "".join(base for base in sequence.upper() if base in {"A", "C", "G", "T"})


def format_raw_nucleotide_pulse(external_buffer_stream: list[dict[str, Any]]) -> str:
    if not external_buffer_stream:
        return "Awaiting external_buffer stream..."
    pulse = "".join(str(entry.get("nucleotide", "")) for entry in external_buffer_stream)
    pulse_header = f"Pulse: {pulse}"
    hashed_lines = [
        (
            f"{str(entry.get('lane', 'n/a')).upper()}[{int(entry.get('index', 0)):04d}] "
            f"base={entry.get('nucleotide', '?')} "
            f"slot={int(entry.get('aperture_slot', 0))} "
            f"Voxel Hash={entry.get('voxel_hash', 'n/a')} "
            f"uid64={entry.get('uid64_hex', 'n/a')}"
        )
        for entry in external_buffer_stream
    ]
    return "\n".join([pulse_header, ""] + hashed_lines)


def build_voxel_stream_entries(
    lead_keys: np.ndarray,
    lag_keys: np.ndarray,
    x1_i: np.ndarray,
    y1_i: np.ndarray,
    x2_i: np.ndarray,
    y2_i: np.ndarray,
    z_block_i: np.ndarray,
) -> list[dict[str, Any]]:
    stream: list[dict[str, Any]] = []
    for lane, keys, x_vals, y_vals in (
        ("lead", lead_keys, x1_i, y1_i),
        ("lag", lag_keys, x2_i, y2_i),
    ):
        for idx in range(len(keys)):
            uid = int(keys[idx])
            uid64_hex = hex(uid)
            stream.append(
                {
                    "lane": lane,
                    "index": int(idx),
                    "x": float(x_vals[idx] / VOXEL_SCALE),
                    "y": float(y_vals[idx] / VOXEL_SCALE),
                    "z": float(z_block_i[idx] / VOXEL_SCALE),
                    "uid64": str(uid),
                    "uid64_hex": uid64_hex,
                    "aperture_slot": int(uid % HTT_PRECISION_APERTURE),
                    "voxel_hash": hashlib.sha256(f"{lane}:{idx}:{uid64_hex}".encode("utf-8")).hexdigest()[:16],
                }
            )
    return stream


def aistudio_dual_score(sequence: str) -> dict[str, float]:
    clean = sanitize_dna(sequence)
    if len(clean) < 23:
        raise ValueError("Sequence must contain at least 23 DNA bases (ACGT).")
    htt_analyzer = load_aistudio_analyzer(AISTUDIO_HTT_ANALYZE_SOURCE)
    crispr_analyzer = load_aistudio_analyzer(AISTUDIO_CRISPR_ANALYZE_SOURCE)
    htt_score = float(htt_analyzer(clean, 1, "GG"))
    crispr_score = float(crispr_analyzer(clean[:23], 1, "NGG"))
    return {"htt_lambda_score": htt_score, "crispr_lambda_score": crispr_score}


def run_htt_carrier_protocol(sequence: str) -> dict[str, Any]:
    clean = sanitize_dna(sequence)
    if len(clean) < 23:
        raise ValueError("Sequence must contain at least 23 DNA bases (ACGT).")

    repeat_matches = [len(match.group(0)) // 3 for match in re.finditer(r"(?:CAG)+", clean)]
    max_cag_repeats = max(repeat_matches, default=0)
    exceeds_threshold = max_cag_repeats >= HTT_CAG_CARRIER_THRESHOLD

    points = max(max_cag_repeats, HTT_CAG_CARRIER_THRESHOLD, 36)
    theta = np.linspace(0, 4 * np.pi, points)
    z = np.linspace(0, 24, points)
    jitter = deterministic_jitter(points) / VOXEL_SCALE
    drift_strength = max(0.0, (max_cag_repeats - (HTT_CAG_CARRIER_THRESHOLD - 1)) / max(HTT_CAG_CARRIER_THRESHOLD, 1))
    lag_drift = drift_strength * np.linspace(0.0, 1.0, points)
    lead_radius = np.full(points, float(ENFORCED_RADIUS_UP))
    lag_radius = np.full(points, float(ENFORCED_RADIUS_DOWN)) - lag_drift
    sync_variance = float(np.mean(np.abs(lead_radius - lag_radius)))
    baseline_differential_area = float(np.pi * ((ENFORCED_RADIUS_UP**2) - (ENFORCED_RADIUS_DOWN**2)))
    differential_area = float(np.mean(np.pi * ((lead_radius**2) - (lag_radius**2))))
    high_tension = sync_variance > 2.0 and differential_area > baseline_differential_area
    structural_drift = exceeds_threshold and high_tension

    x1_i = quantize_int64(np.cos(theta) * lead_radius, VOXEL_SCALE)
    y1_i = quantize_int64(np.sin(theta) * lead_radius, VOXEL_SCALE)
    x2_i = quantize_int64(np.cos(theta + np.pi) * lag_radius + jitter, VOXEL_SCALE)
    y2_i = quantize_int64(np.sin(theta + np.pi) * lag_radius, VOXEL_SCALE)
    z_i = quantize_int64(z, VOXEL_SCALE)
    z_block_i = (z_i // np.int64(64)) * np.int64(64)
    lead_keys = pack_voxel_key64(x1_i, y1_i, z_block_i)
    lag_keys = pack_voxel_key64(x2_i, y2_i, z_block_i)
    combined_keys = np.concatenate([lead_keys, lag_keys])
    voxel_stream = build_voxel_stream_entries(lead_keys, lag_keys, x1_i, y1_i, x2_i, y2_i, z_block_i)
    external_buffer_stream = []
    for stream_index, entry in enumerate(voxel_stream):
        stream_entry = dict(entry)
        stream_entry["nucleotide"] = clean[stream_index % len(clean)]
        external_buffer_stream.append(stream_entry)
    unique_voxels = int(np.unique(combined_keys).size)
    stretch_idx = int(np.argmax(np.abs(lead_radius - lag_radius)))
    stretch_voxel_uid = int(lag_keys[stretch_idx])
    stretch_voxel = {
        "x": float(x2_i[stretch_idx] / VOXEL_SCALE),
        "y": float(y2_i[stretch_idx] / VOXEL_SCALE),
        "z": float(z_block_i[stretch_idx] / VOXEL_SCALE),
        "uid64": str(stretch_voxel_uid),
        "uid64_hex": hex(stretch_voxel_uid),
    }

    fingerprint_source = combined_keys.tobytes() + max_cag_repeats.to_bytes(4, "big", signed=False)
    precision_fingerprint = int(hashlib.sha256(fingerprint_source).hexdigest()[:16], 16) % HTT_PRECISION_APERTURE
    processed_bytes = len(clean)
    processed_megabytes = processed_bytes / 1_000_000.0
    skyfire_ping_count = max(1, math.ceil(processed_bytes / max(SKYFIRE_PING_BYTES, 1)))
    watchdog_verified = structural_drift and unique_voxels >= max(64, points)

    return {
        "spops_agent": AGENT_REGISTRY.get("SPOPS"),
        "watchdog_agent": AGENT_REGISTRY.get("watchdog"),
        "threshold_repeats": HTT_CAG_CARRIER_THRESHOLD,
        "max_cag_repeats": int(max_cag_repeats),
        "chromosome": 4,
        "sync_variance_v": sync_variance,
        "baseline_differential_area": baseline_differential_area,
        "differential_area": differential_area,
        "high_tension": high_tension,
        "structural_drift": structural_drift,
        "voxel_bits": 64,
        "precision_aperture": HTT_PRECISION_APERTURE,
        "precision_fingerprint": int(precision_fingerprint),
        "unique_voxels": unique_voxels,
        "voxel_stream": voxel_stream,
        "voxel_stream_count": len(voxel_stream),
        "external_buffer_stream": external_buffer_stream,
        "stretch_voxel": stretch_voxel,
        "processed_bytes": int(processed_bytes),
        "processed_megabytes": float(processed_megabytes),
        "skyfire_ping_count": int(skyfire_ping_count),
        "watchdog_verified": watchdog_verified,
        "tier1_carrier_identified": bool(structural_drift and watchdog_verified),
    }


def enqueue_celery_task(task_name: str, *args: Any) -> str:
    automation_path = Path(__file__).resolve().parent / "apps" / "automation"
    if str(automation_path) not in sys.path:
        sys.path.insert(0, str(automation_path))
    try:
        import redis as redis_client
    except ImportError as err:
        raise RuntimeError("Celery Redis client is not installed in this runtime.") from err
    if not hasattr(redis_client, "Redis"):
        raise RuntimeError("Celery Redis transport is invalid in this runtime (redis.Redis missing).")
    from celery_tasks import run_conversation_task, run_cycle_task

    try:
        if task_name == "automation.run_cycle":
            task = run_cycle_task.delay()
        elif task_name == "automation.run_conversation":
            task = run_conversation_task.delay(*args)
        else:
            raise ValueError(f"Unsupported celery task: {task_name}")
    except AttributeError as err:
        raise RuntimeError("Celery broker transport failed to initialize. Check CELERY_BROKER_URL and Redis package.") from err
    return str(task.id)


def render_spartan_helm_theme() -> None:
    st.markdown(
        """
        <style>
          :root {
            --spartan-gold: #ffca6b;
            --spartan-steel: #d7dfef;
            --spartan-panel: rgba(13, 20, 32, 0.72);
            --spartan-panel-border: rgba(255, 202, 107, 0.2);
          }
          .stApp {
            background:
              radial-gradient(circle at 12% 12%, rgba(255, 80, 34, 0.22), transparent 35%),
              radial-gradient(circle at 88% 20%, rgba(255, 186, 74, 0.18), transparent 42%),
              radial-gradient(circle at 50% 100%, rgba(30, 115, 255, 0.15), transparent 45%),
              linear-gradient(135deg, #0a0f16 0%, #111826 45%, #0e1520 100%);
            color: #e9eef8;
          }
          .spartan-helm {
            border: 1px solid rgba(255, 186, 74, 0.35);
            background: linear-gradient(120deg, rgba(12, 17, 25, 0.9), rgba(24, 30, 45, 0.78));
            border-radius: 14px;
            padding: 1rem 1.2rem;
            margin: 0.3rem 0 0.55rem 0;
            box-shadow: 0 0 0 1px rgba(255, 255, 255, 0.04), 0 10px 30px rgba(0, 0, 0, 0.35);
          }
          .spartan-helm h1 {
            margin: 0;
            font-size: 1.55rem;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            color: #ffca6b;
          }
          .spartan-helm p {
            margin: 0.25rem 0 0;
            color: var(--spartan-steel);
            font-size: 0.95rem;
            letter-spacing: 0.05em;
          }
          .spartan-helm-badges {
            display: flex;
            flex-wrap: wrap;
            gap: 0.4rem;
            margin-top: 0.65rem;
          }
          .spartan-helm-badges span {
            display: inline-flex;
            align-items: center;
            border: 1px solid var(--spartan-panel-border);
            border-radius: 999px;
            padding: 0.2rem 0.55rem;
            font-size: 0.75rem;
            letter-spacing: 0.06em;
            color: #e6edf9;
            background: rgba(9, 14, 24, 0.55);
          }
          .spartan-helm-subtitle {
            margin-bottom: 0.9rem;
            color: #b9c6de;
            font-size: 0.93rem;
            letter-spacing: 0.05em;
          }
          section[data-testid="stSidebar"] > div:first-child {
            background: linear-gradient(180deg, rgba(15, 22, 34, 0.9) 0%, rgba(12, 18, 28, 0.82) 100%);
            border-right: 1px solid rgba(255, 255, 255, 0.06);
          }
          .stButton > button {
            border-radius: 10px;
            border: 1px solid rgba(255, 202, 107, 0.35);
            background: linear-gradient(120deg, rgba(43, 67, 104, 0.95), rgba(25, 48, 85, 0.95));
            color: #edf3ff;
            font-weight: 600;
          }
          .stButton > button:hover {
            border-color: rgba(255, 202, 107, 0.58);
            background: linear-gradient(120deg, rgba(55, 82, 124, 0.98), rgba(33, 60, 101, 0.98));
          }
          div[data-testid="stMetric"] {
            background: var(--spartan-panel);
            border: 1px solid var(--spartan-panel-border);
            border-radius: 12px;
            padding: 0.55rem 0.8rem;
          }
          div[data-baseweb="tab-list"] {
            gap: 0.35rem;
          }
          div[data-baseweb="tab"] {
            border-radius: 9px;
            border: 1px solid rgba(255, 255, 255, 0.08);
            background: rgba(9, 14, 22, 0.55);
          }
        </style>
        <div class="spartan-helm">
          <h1>🛡️ SROL SpartanR&amp;D HELM</h1>
          <p>Operational Command Surface • Multi-Agent Revenue Mesh</p>
          <div class="spartan-helm-badges">
            <span>Command Surface</span>
            <span>Revenue Mesh</span>
            <span>Sequence Intel</span>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


# ----------------------------
# 🔱 PHASE 4: UI & SENTINEL BRAIN
# ----------------------------
def main():
    render_spartan_helm_theme()
    st.markdown('<div class="spartan-helm-subtitle">🔱 Spartan Sentinel: Unified Multi-Agent Node</div>', unsafe_allow_html=True)
    monetizer = SpartanMonetizer(SKYFIRE_API_KEY, SKYFIRE_PAYMENT_URL)
    st.session_state.setdefault("payment_verified", False)
    st.session_state.setdefault("payment_tier", "")
    st.session_state.setdefault("latest_htt_carrier_event", None)
    st.session_state.setdefault("skyfire_status", "Idle")
    st.session_state.setdefault("mesh_mode", "External Buffer Mode")
    st.session_state.setdefault("latest_voxel_stream", [])
    st.session_state.setdefault("external_buffer_stream", [])
    st.session_state.setdefault("skyfire_ping_total", 0)
    
    # Initialize sequence tracking
    tracker = get_sequence_tracker()
    st.session_state.setdefault("sequence_tracker", tracker)
    
    sequence_report_cache = ensure_sequence_intel_report()
    htt_stream_snapshot = build_htt_stream_snapshot(sequence_report_cache)
    miner_snapshot = fetch_sequence_miner_snapshot(SEQUENCE_MINER_URL)
    using_remote_miner = bool(miner_snapshot.get("online"))
    if using_remote_miner:
        htt_stream_snapshot = miner_snapshot["htt_stream_snapshot"]

    with st.sidebar:
        st.caption(f"Reference Genome ID: `{REFERENCE_GENOME_ID}`")
        st.divider()
        
        # 📊 SEQUENCE TRACKER DISPLAY
        st.header("📊 Sequence Tracker")
        tracker = st.session_state.get("sequence_tracker", get_sequence_tracker())
        col1, col2 = st.columns(2)
        col1.metric(
            "✨ Total Sequences Run",
            f"{tracker['total_sequences_processed']:,}",
            delta=f"{len(tracker['runs'])} runs"
        )
        col2.metric(
            "💰 Total Value",
            f"${tracker['total_value_usd']:.2f}",
            delta=f"@ ${SEQUENCE_VALUE_PER_RECORD:.2f}/seq"
        )
        
        if tracker["runs"]:
            last_run = tracker["runs"][-1]
            st.caption(f"Last run: {last_run['sequences']} seqs (${last_run['value_usd']:.2f}) @ {last_run['timestamp'][:16]}")
        
        st.divider()
        st.header("💳 Skyfire Monetization")
        selected_tier = st.selectbox("Pricing tier", options=list(SpartanMonetizer.PRICING_TIERS.keys()), index=0)
        st.caption(f"{selected_tier}: ${SpartanMonetizer.PRICING_TIERS[selected_tier]:.0f}")
        customer_ref = st.text_input("Customer reference", key="customer_ref")
        pay_token = st.text_input("Skyfire pay token", key="skyfire_pay_token", type="password")
        if st.button("Verify Payment Status", use_container_width=True):
            try:
                payment_result = monetizer.process_payment(selected_tier, customer_ref.strip(), pay_token.strip())
                st.session_state["payment_verified"] = payment_result["verified"]
                st.session_state["payment_tier"] = selected_tier if payment_result["verified"] else ""
                append_sales_event(
                    {
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "event_type": "skyfire_payment_verified",
                        "verified": payment_result["verified"],
                        "tier": selected_tier,
                        "amount_usd": payment_result["amount_usd"],
                        "customer_ref": customer_ref.strip(),
                    }
                )
                if payment_result["verified"]:
                    st.success("Payment verified.")
                else:
                    st.warning("Payment not verified by Skyfire.")
            except (ValueError, requests.RequestException) as err:
                st.session_state["payment_verified"] = False
                st.error(f"Payment verification failed: {err}")
        st.caption(f"Golden Sequence access: {'Unlocked' if st.session_state['payment_verified'] else 'Locked'}")
        st.caption(f"Skyfire status: {st.session_state.get('skyfire_status', 'Idle')}")
        st.caption(f"Skyfire heartbeat pings: {int(st.session_state.get('skyfire_ping_total', 0))}")
        st.divider()
        st.header("🛰️ Sequence Stream")
        st.caption(f"Mode: {st.session_state.get('mesh_mode', 'External Buffer Mode')}")
        if using_remote_miner:
            st.caption(f"Source: Sequence_MINER (`{miner_snapshot.get('endpoint', '')}`)")
        else:
            st.caption("Source: local sequence intel cache")
        st.caption("Stream: Chromosome 4 (HTT region)")
        st.metric("Raw nucleotide count", int(htt_stream_snapshot["raw_nucleotide_count"]))
        st.metric("HTT-linked records", int(htt_stream_snapshot["record_count"]))
        if st.session_state.get("external_buffer_stream"):
            with st.expander("Raw Nucleotide Pulse", expanded=True):
                st.code(format_raw_nucleotide_pulse(st.session_state["external_buffer_stream"]), language="text")
        if st.session_state.get("latest_voxel_stream"):
            with st.expander("64-bit voxel aperture stream", expanded=False):
                voxel_df = pd.DataFrame(st.session_state["latest_voxel_stream"])
                st.dataframe(
                    voxel_df[["lane", "index", "uid64_hex", "aperture_slot", "voxel_hash"]],
                    use_container_width=True,
                )

        st.caption(f"Active radii: lead={ENFORCED_RADIUS_UP}, lag={ENFORCED_RADIUS_DOWN}")

        st.divider()
        st.header("🧬 Mesh Registry")
        st.caption(f"Class linked: `{AUTONOMOUS_MESH.CLASS_NAME}`")
        for name, addr in AGENT_REGISTRY.items():
            st.caption(f"🟢 {name}: {addr[:12]}...")

        st.divider()
        if spartan_brain:
            st.subheader("🤖 AI Sentinel Brain")
            st.success("Brain Linked to Railway Env")
            user_query = st.text_input("Ask the Mesh:", key="sentinel_query")
            if user_query:
                with st.spinner("AI Analysis..."):
                    try:
                        resp = spartan_brain.generate_content(
                            f"Context: Spartan Mesh {ENFORCED_RADIUS_UP}/{ENFORCED_RADIUS_DOWN}. "
                            f"Gap: {ENFORCED_RADIUS_UP - ENFORCED_RADIUS_DOWN} units. Logic: 64-bit. Query: {user_query}"
                        )
                        st.info(resp.text)
                    except Exception as err:  # model request failures
                        st.error(f"Brain Sync Failed: {str(err)[:50]}")
        else:
            st.warning("AI Sentinel Offline. Add GOOGLE_API_KEY to Railway Variables.")

    tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs(
        [
            "📊 Command Center",
            "🧬 3D Volumetric Map",
            "🔗 Manifest Audit",
            "💰 Income Prospector",
            "🧠 Automation Hub",
            "💵 Money Machine",
            "🧪 AI Studio Live",
        ]
    )
    tension_marker = None
    if st.session_state.get("latest_htt_carrier_event"):
        stretch_voxel = st.session_state["latest_htt_carrier_event"].get("stretch_voxel")
        if isinstance(stretch_voxel, dict):
            tension_marker = stretch_voxel
    helix_figure, symmetry_metrics = create_voxel_helix(
        transaction_volume=calculate_financial_volume(),
        tension_marker=tension_marker,
    )

    with tab1:
        st.subheader("🧬 Genetic Combing Swarm")
        st.caption("Primary mission: run swarm discovery, surface real sequences, and show carrier-relevant findings.")
        swarm_max_results = st.slider(
            "Swarm depth per source",
            min_value=5,
            max_value=50,
            value=20,
            step=5,
            key="swarm_max_results",
        )
        if st.button("Run Genetic Swarm Now", use_container_width=True):
            payload = {
                "max_results": swarm_max_results,
                "orchid_id": ORCHID_ID,
                "accessions": configured_sequence_accessions(),
            }
            try:
                report = build_income_prospector_report(max_results=swarm_max_results)
                persist_prospector_report(report)
                repo_hunt_report = build_repo_hunt_report(max_results=swarm_max_results)
                persist_repo_hunt_report(repo_hunt_report)
                sequence_intel_report = build_sequence_intel_report(
                    orchid_id=ORCHID_ID,
                    raw_input_paths=SEQUENCE_INPUT_PATHS,
                    raw_accessions=configured_sequence_accessions(),
                    ncbi_email=NCBI_EMAIL,
                    ncbi_api_key=NCBI_API_KEY,
                    max_records=SEQUENCE_MAX_RECORDS,
                )
                persist_sequence_intel_report(sequence_intel_report)
                
                sequence_count = int(sequence_intel_report["totals"]["sequence_records"])
                updated_tracker = update_sequence_tracker(sequence_count)
                st.session_state["sequence_tracker"] = updated_tracker
                
                append_audit_record(
                    "genetic_swarm_run",
                    payload,
                    "success",
                    {
                        "genetic_leads": report["totals"]["genetic_leads"],
                        "repo_targets": repo_hunt_report["totals"]["targets_found"],
                        "sequence_records": sequence_intel_report["totals"]["sequence_records"],
                    },
                )
                st.success(f"✨ Genetic swarm completed! Processed {sequence_count} sequences (${sequence_count * SEQUENCE_VALUE_PER_RECORD:.2f} value)")
            except requests.RequestException as err:
                append_audit_record("genetic_swarm_run", payload, "failed", {"error": str(err)})
                st.error(f"Genetic swarm failed: {err}")

        sequence_report = ensure_sequence_intel_report()
        sequence_records = miner_snapshot.get("records", []) if using_remote_miner else sequence_report.get("records", [])
        htt_records = (
            miner_snapshot.get("htt_stream_snapshot", {}).get("records", [])
            if using_remote_miner
            else build_htt_stream_snapshot(sequence_report).get("records", [])
        )
        prospector_report = load_prospector_report() or {"totals": {"genetic_leads": 0}, "genetic_leads": []}
        repo_hunt_report = load_repo_hunt_report() or {"totals": {"targets_found": 0}, "repo_targets": []}
        latest_htt_event = st.session_state.get("latest_htt_carrier_event") or {}
        g1, g2, g3, g4, g5 = st.columns(5)
        sequence_record_total = (
            int(miner_snapshot.get("stats", {}).get("total_sequences") or 0)
            if using_remote_miner
            else int(sequence_report.get("totals", {}).get("sequence_records") or 0)
        )
        g1.metric("Sequence records", sequence_record_total)
        g2.metric("HTT-linked records", len(htt_records))
        g3.metric("Genetic leads", int(prospector_report["totals"].get("genetic_leads") or 0))
        g4.metric("Repo targets", int(repo_hunt_report["totals"].get("targets_found") or 0))
        g5.metric("Carrier protocol", "Detected" if latest_htt_event.get("tier1_carrier_identified") else "Monitoring")
        st.metric("Discovery value signal", f"${sequence_record_total * PIPELINE_ENVELOPE_VALUE_USD:.2f}")
        
        # 💹 VALUE TICKER CHART
        st.divider()
        st.subheader("💹 Sequence Value Tracker")
        tracker = st.session_state.get("sequence_tracker", get_sequence_tracker())
        
        ticker_col1, ticker_col2 = st.columns([2, 1])
        with ticker_col1:
            if tracker["runs"]:
                runs_df = pd.DataFrame(tracker["runs"])
                runs_df["timestamp"] = pd.to_datetime(runs_df["timestamp"])
                runs_df = runs_df.sort_values("timestamp")
                
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=runs_df["timestamp"],
                    y=runs_df["value_usd"],
                    mode="lines+markers",
                    name="Run Value",
                    line=dict(color="#ffca6b", width=3),
                    marker=dict(size=8, color="#25d366"),
                    fill="tozeroy",
                    fillcolor="rgba(255, 202, 107, 0.1)"
                ))
                fig.update_layout(
                    title="Cumulative Value Over Runs",
                    xaxis_title="Timestamp",
                    yaxis_title="Value (USD)",
                    template="plotly_dark",
                    hovermode="x unified",
                    height=300
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No runs recorded yet. Execute swarms to generate data.")
        
        with ticker_col2:
            t1, t2 = st.columns(2)
            t1.metric(
                "Average Value/Run",
                f"${(tracker['total_value_usd'] / len(tracker['runs'])) if tracker['runs'] else 0:.2f}"
            )
        
        st.divider()

        st.markdown("### Discovered genomic sequences")
        if using_remote_miner:
            st.caption("Live feed from Sequence_MINER `/sequences/recent`.")
        st.dataframe(pd.DataFrame(sequence_records[:25]), use_container_width=True)
        st.markdown("### Live genetic leads")
        st.dataframe(pd.DataFrame(prospector_report.get("genetic_leads", [])[:25]), use_container_width=True)
        st.caption(
            "HTT carrier confirmation remains in AI Studio tab via HTT Carrier Protocol (drift + 64-bit voxel UID logging)."
        )

        st.divider()
        st.subheader("🚀 Operational Status")
        m1, m2, m3 = st.columns(3)
        m1.metric("Architecture", "64-Bit Voxel")
        m2.metric("Gap Variance", f"{ENFORCED_RADIUS_UP - ENFORCED_RADIUS_DOWN} Units")
        m3.metric("GxP Mode", GXP_STRICT_MODE)

        st.info(f"**Target Overseer:** `{OVERSEER[:16]}...`")
        st.caption(f"Mesh assigned: {count_assigned_agents()}/{AUTONOMOUS_MESH.count()} agents")
        cloudflare_url = normalize_endpoint_url(CLOUDFLARE_AGENT_URL)
        railway_url = normalize_endpoint_url(RAILWAY_MESH_URL)
        vercel_url = normalize_endpoint_url(VERCEL_CONTROL_URL)
        endpoint_col_1, endpoint_col_2, endpoint_col_3 = st.columns(3)
        endpoint_col_1.markdown(
            f"Cloudflare relay: {'🟢' if cloudflare_url and source_online(cloudflare_url) else '🔴'} "
            + (f"[{cloudflare_url}]({cloudflare_url})" if cloudflare_url else "Not configured")
        )
        endpoint_col_2.markdown(
            f"Railway mesh: {'🟢' if railway_url and source_online(railway_url) else '🔴'} "
            + (f"[{railway_url}]({railway_url})" if railway_url else "Not configured")
        )
        endpoint_col_3.markdown(
            f"Vercel control: {'🟢' if vercel_url and source_online(vercel_url) else '🔴'} "
            + (f"[{vercel_url}]({vercel_url})" if vercel_url else "Not configured")
        )

        st.divider()
        st.subheader("⚡ Skyfire Controls")
        action_col_1, action_col_2, action_col_3 = st.columns(3)

        if action_col_1.button("Relay Mesh Sync", use_container_width=True):
            payload = {
                "mesh": AGENT_REGISTRY,
                "radii": {"lead": ENFORCED_RADIUS_UP, "lag": ENFORCED_RADIUS_DOWN},
                "symmetry": symmetry_metrics,
            }
            try:
                result = dispatch_control_event(CLOUDFLARE_AGENT_URL, "mesh_sync", payload)
                append_audit_record("mesh_sync", payload, "success", result)
                st.success("Cloudflare relay completed.")
                st.json(result)
            except (ValueError, requests.RequestException) as err:
                append_audit_record("mesh_sync", payload, "failed", {"error": str(err)})
                st.error(f"Relay failed: {err}")

        if action_col_2.button("Run Revenue Cycle", use_container_width=True):
            payload = {
                "action": "run_cycle",
                "strict_mode": GXP_STRICT_MODE,
                "agent_count": count_assigned_agents(),
            }
            try:
                result = dispatch_control_event(RAILWAY_MESH_URL, "revenue_cycle", payload)
                append_audit_record("revenue_cycle", payload, "success", result)
                st.success("Railway revenue cycle dispatched.")
                st.json(result)
            except (ValueError, requests.RequestException) as err:
                append_audit_record("revenue_cycle", payload, "failed", {"error": str(err)})
                st.error(f"Revenue cycle failed: {err}")

        if action_col_3.button("Sync Vercel Edge", use_container_width=True):
            payload = {"status_probe": True, "timestamp": datetime.now(timezone.utc).isoformat()}
            try:
                result = dispatch_control_event(VERCEL_CONTROL_URL, "edge_sync", payload)
                append_audit_record("edge_sync", payload, "success", result)
                st.success("Vercel edge sync completed.")
                st.json(result)
            except (ValueError, requests.RequestException) as err:
                append_audit_record("edge_sync", payload, "failed", {"error": str(err)})
                st.error(f"Vercel sync failed: {err}")

    with tab2:
        st.plotly_chart(helix_figure, use_container_width=True)
        st.caption("Lead/Lag voxel cubes are rendered as stacked 64-bit blocks to show 3D alignment by z-layer.")
        latest_htt_event = st.session_state.get("latest_htt_carrier_event") or {}
        if latest_htt_event.get("high_tension"):
            st.error("🔱 HIGH-TENSION DRIFT DETECTED: HTT CARRIER CANDIDATE")
            stretch_voxel = latest_htt_event.get("stretch_voxel") or {}
            st.caption(
                f"Stretch voxel UID64: `{stretch_voxel.get('uid64_hex', 'n/a')}` at "
                f"({stretch_voxel.get('x', 'n/a')}, {stretch_voxel.get('y', 'n/a')}, {stretch_voxel.get('z', 'n/a')})"
            )
        st.subheader("64-bit Voxel Symmetry Metrics")
        c1, c2, c3 = st.columns(3)
        c1.metric("Lead Radius MAE", f"{symmetry_metrics['lead_radius_mae']:.6f}")
        c2.metric("Lag Radius MAE", f"{symmetry_metrics['lag_radius_mae']:.6f}")
        c3.metric("Phase π Error", f"{symmetry_metrics['phase_pi_error_mean']:.6f}")
        if "stack_layers_visualized" in symmetry_metrics:
            s1, s2, s3 = st.columns(3)
            s1.metric("Voxel stack layers", int(symmetry_metrics["stack_layers_visualized"]))
            s2.metric("Alignment mean", f"{symmetry_metrics['stack_alignment_mean']:.6f}")
            s3.metric("Alignment max", f"{symmetry_metrics['stack_alignment_max']:.6f}")
        st.json(symmetry_metrics)

    with tab3:
        st.subheader("📄 Manifest V2 Generation")
        manifest_v2 = build_manifest_v2(symmetry_metrics, st.session_state.get("latest_htt_carrier_event"))
        st.json(manifest_v2)
        st.download_button("📥 Sign & Download Manifest", json.dumps(manifest_v2), "manifest_v2.json")

    with tab4:
        st.subheader("🧲 Genetic + Bounty Prospecting")
        st.caption("Scans public genetic research feeds and open bounty signals for monetizable opportunities.")

        max_results = st.slider("Results per source", min_value=5, max_value=50, value=20, step=5)
        if st.button("Run Prospector Cycle", use_container_width=True):
            payload = {"max_results": max_results, "mode": "public_sources_only"}
            try:
                report = build_income_prospector_report(max_results=max_results)
                persist_prospector_report(report)
                sequence_intel_report = build_sequence_intel_report(
                    orchid_id=ORCHID_ID,
                    raw_input_paths=SEQUENCE_INPUT_PATHS,
                    raw_accessions=configured_sequence_accessions(),
                    ncbi_email=NCBI_EMAIL,
                    ncbi_api_key=NCBI_API_KEY,
                    max_records=SEQUENCE_MAX_RECORDS,
                )
                persist_sequence_intel_report(sequence_intel_report)
                append_audit_record("prospector_cycle", payload, "success", report["totals"])
                st.success("Prospector cycle complete.")
            except requests.RequestException as err:
                append_audit_record("prospector_cycle", payload, "failed", {"error": str(err)})
                st.error(f"Prospector cycle failed: {err}")

        report = load_prospector_report()
        if not report:
            st.info("No prospector report found yet. Run one cycle to generate leads.")
        else:
            st.metric("Combined leads", report["totals"]["combined_leads"])
            st.metric("Genetic leads", report["totals"]["genetic_leads"])
            st.metric("Bounty leads", report["totals"]["bounty_leads"])

            st.markdown("### Genetic intelligence leads")
            st.dataframe(pd.DataFrame(report["genetic_leads"]), use_container_width=True)

            st.markdown("### Open bounty leads")
            st.dataframe(pd.DataFrame(report["bounty_leads"]), use_container_width=True)

            repo_hunt_snapshot = load_repo_hunt_report()
            if repo_hunt_snapshot:
                bounty_contract_report = build_bounty_contract_report(
                    report,
                    repo_hunt_snapshot,
                    max_contracts=BOUNTY_CONTRACT_MAX_RESULTS,
                    value_spartan_usd=PIPELINE_VALUE_SPARTAN_USD,
                    value_pro_usd=PIPELINE_VALUE_PRO_USD,
                    value_enterprise_usd=PIPELINE_VALUE_ENTERPRISE_USD,
                    close_rate_bounty=PIPELINE_CLOSE_RATE_BOUNTY,
                    close_rate_repo=PIPELINE_CLOSE_RATE_REPO,
                )
                persist_bounty_contract_report(bounty_contract_report)
                st.markdown("### Consolidated bounty contracts")
                bc1, bc2, bc3 = st.columns(3)
                bc1.metric("Contracts", int(bounty_contract_report["totals"]["contract_count"]))
                bc2.metric("Contract value", f"${bounty_contract_report['totals']['contract_value_usd']:.2f}")
                bc3.metric("Expected value", f"${bounty_contract_report['totals']['expected_value_usd']:.2f}")
                st.dataframe(pd.DataFrame(bounty_contract_report["contracts"][:25]), use_container_width=True)

            st.download_button(
                "📥 Download Prospector Report",
                json.dumps(report, indent=2),
                "prospector_report.json",
                use_container_width=True,
            )

        st.markdown("### Orchid + Biopython sequence intelligence")
        st.caption("This powers monetizable genetic discovery lead generation.")
        if st.button("Run Sequence Discovery Now", use_container_width=True):
            payload = {
                "orchid_id": ORCHID_ID,
                "accessions": configured_sequence_accessions(),
                "max_records": SEQUENCE_MAX_RECORDS,
            }
            sequence_intel_report = ensure_sequence_intel_report(force_refresh=True)
            append_audit_record("sequence_discovery", payload, "success", sequence_intel_report["totals"])
            st.success("Sequence discovery updated.")

        sequence_report = ensure_sequence_intel_report()
        if not sequence_report:
            st.warning(
                "No sequence intel snapshot yet. Run Sequence Discovery to pull records (HTT defaults are auto-loaded when no env accessions are set)."
            )
        else:
            sr1, sr2, sr3, sr4, sr5 = st.columns(5)
            sr1.metric("Orchid ID", sequence_report.get("orchid_id", ""))
            sr2.metric("Sequence records", int(sequence_report["totals"]["sequence_records"]))
            sr3.metric("Avg length", float(sequence_report["totals"]["avg_length"]))
            sr4.metric("Avg GC%", float(sequence_report["totals"]["avg_gc_percent"]))
            sr5.metric(
                "Discovery value signal",
                f"${float(sequence_report['totals']['sequence_records']) * PIPELINE_ENVELOPE_VALUE_USD:.2f}",
            )
            st.caption(f"Accession source: `{configured_sequence_accessions()}`")
            st.dataframe(pd.DataFrame(sequence_report.get("records", [])[:25]), use_container_width=True)
            if int(sequence_report["totals"]["sequence_records"]) == 0:
                st.warning(
                    f"No sequence records found. Current accession source: `{configured_sequence_accessions()}`. "
                    "Set SEQUENCE_ACCESSIONS or SEQUENCE_INPUT_PATHS in Railway if you want custom targets."
                )
            if sequence_report.get("errors"):
                st.warning(f"Sequence intel warnings: {len(sequence_report['errors'])}")

    with tab5:
        st.subheader("🧠 Monorepo Automation Hub")
        st.caption("Central control for orchestrator + automation cycles from one Streamlit surface.")

        c1, c2, c3 = st.columns(3)
        c1.metric("Orchestrator App", "apps/orchestrator")
        c2.metric("Automation App", "apps/automation")
        c3.metric("Shared Package", "packages/shared")

        run_mode = st.selectbox("Automation run mode", options=["once", "daemon"], index=0)
        interval_seconds = st.number_input(
            "Daemon interval (seconds)",
            min_value=60,
            max_value=86400,
            value=3600,
            step=60,
        )
        max_results_auto = st.slider(
            "Automation max results per source", min_value=5, max_value=50, value=20, step=5, key="auto_results"
        )

        if st.button("Run Automation Cycle Now", use_container_width=True):
            payload = {
                "max_results": max_results_auto,
                "run_mode": run_mode,
                "interval_seconds": interval_seconds,
                "trigger": "streamlit_automation_hub",
            }
            try:
                report = build_income_prospector_report(max_results=max_results_auto)
                persist_prospector_report(report)
                repo_hunt_report = build_repo_hunt_report(max_results=max_results_auto)
                persist_repo_hunt_report(repo_hunt_report)
                sequence_intel_report = build_sequence_intel_report(
                    orchid_id=ORCHID_ID,
                    raw_input_paths=SEQUENCE_INPUT_PATHS,
                    raw_accessions=configured_sequence_accessions(),
                    ncbi_email=NCBI_EMAIL,
                    ncbi_api_key=NCBI_API_KEY,
                    max_records=SEQUENCE_MAX_RECORDS,
                )
                persist_sequence_intel_report(sequence_intel_report)
                bounty_contract_report = build_bounty_contract_report(
                    report,
                    repo_hunt_report,
                    max_contracts=BOUNTY_CONTRACT_MAX_RESULTS,
                    value_spartan_usd=PIPELINE_VALUE_SPARTAN_USD,
                    value_pro_usd=PIPELINE_VALUE_PRO_USD,
                    value_enterprise_usd=PIPELINE_VALUE_ENTERPRISE_USD,
                    close_rate_bounty=PIPELINE_CLOSE_RATE_BOUNTY,
                    close_rate_repo=PIPELINE_CLOSE_RATE_REPO,
                )
                persist_bounty_contract_report(bounty_contract_report)
                protocol_endpoints = parse_protocol_endpoints(HUNT_PROTOCOL_ENDPOINTS)
                engagement_result = engage_protocols(
                    action="active_hunt_cycle",
                    payload={
                        "prospector_totals": report["totals"],
                        "repo_hunt_totals": repo_hunt_report["totals"],
                        "sequence_intel_totals": sequence_intel_report["totals"],
                        "orchid_id": sequence_intel_report["orchid_id"],
                        "top_repo_targets": repo_hunt_report["repo_targets"][:10],
                        "top_bounty_targets": report["bounty_leads"][:10],
                        "top_sequence_records": sequence_intel_report["records"][:10],
                    },
                    endpoints=protocol_endpoints,
                    api_key=HUNT_PROTOCOL_API_KEY,
                )
                bounty_dispatch_result = engage_protocols(
                    action=BOUNTY_ACTION,
                    payload={
                        "contracts": bounty_contract_report["contracts"],
                        "totals": bounty_contract_report["totals"],
                        "offers": ["Spartan", "Pro", "Enterprise"],
                    },
                    endpoints=parse_protocol_endpoints(BOUNTY_PROTOCOL_ENDPOINTS),
                    api_key=BOUNTY_PROTOCOL_API_KEY,
                )
                outreach_targets = build_outreach_targets(bounty_contract_report)
                conversation_envelopes = build_conversation_envelopes(
                    outreach_targets, max_targets=CONVERSATION_MAX_TARGETS
                )
                conversation_result = engage_protocols(
                    action=CONVERSATION_ACTION,
                    payload={"envelopes": conversation_envelopes, "count": len(conversation_envelopes)},
                    endpoints=parse_protocol_endpoints(CONVERSATION_PROTOCOL_ENDPOINTS),
                    api_key=CONVERSATION_PROTOCOL_API_KEY,
                )
                outreach_result = engage_protocols(
                    action="outreach_sequence_dispatch",
                    payload={
                        "targets": outreach_targets,
                        "offers": ["Spartan", "Pro", "Enterprise"],
                        "booking_url": BOOKING_URL,
                    },
                    endpoints=parse_protocol_endpoints(OUTREACH_PROTOCOL_ENDPOINTS),
                    api_key=OUTREACH_PROTOCOL_API_KEY,
                )
                target_count = len(outreach_targets)
                delivered_envelopes = int(
                    round(
                        target_count
                        * (
                            max(
                                0.0,
                                min(
                                    1.0,
                                    int(outreach_result.get("successful") or 0)
                                    / max(int(outreach_result.get("attempted") or 1), 1),
                                ),
                            )
                        )
                    )
                )
                append_outreach_event(
                    {
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "event_type": "bounty_contract_dispatch",
                        "contract_count": int(bounty_contract_report["totals"]["contract_count"]),
                        "contract_value_usd": float(bounty_contract_report["totals"]["contract_value_usd"]),
                        "expected_value_usd": float(bounty_contract_report["totals"]["expected_value_usd"]),
                        "result": bounty_dispatch_result,
                    }
                )
                append_outreach_event(
                    {
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "event_type": "conversation_dispatch",
                        "target_count": len(outreach_targets),
                        "envelope_count": len(conversation_envelopes),
                        "result": conversation_result,
                    }
                )
                append_outreach_event(
                    {
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "event_type": "outreach_dispatch",
                        "sequence_runs": 1,
                        "target_count": target_count,
                        "delivered_envelopes": delivered_envelopes,
                        "sequence_potential_value_usd": round(target_count * PIPELINE_ENVELOPE_VALUE_USD, 2),
                        "sequence_realized_value_usd": round(delivered_envelopes * PIPELINE_ENVELOPE_VALUE_USD, 2),
                        "pipeline_contract_value_usd": round(
                            sum(float(target.get("offer_value_usd") or 0.0) for target in outreach_targets), 2
                        ),
                        "pipeline_expected_value_usd": round(
                            sum(float(target.get("expected_value_usd") or 0.0) for target in outreach_targets), 2
                        ),
                        "result": outreach_result,
                    }
                )
                cycle = {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "prospector_totals": report["totals"],
                    "repo_hunt_totals": repo_hunt_report["totals"],
                    "sequence_intel_totals": sequence_intel_report["totals"],
                    "orchid_id": sequence_intel_report["orchid_id"],
                    "bounty_contract_totals": bounty_contract_report["totals"],
                    "run_mode": run_mode,
                    "interval_seconds": int(interval_seconds),
                    "engagement": engagement_result,
                    "bounty_dispatch": bounty_dispatch_result,
                    "conversation_dispatch": conversation_result,
                    "conversation_envelope_count": len(conversation_envelopes),
                    "outreach": outreach_result,
                    "outreach_sequence_runs": 1,
                    "sequence_potential_value_usd": round(target_count * PIPELINE_ENVELOPE_VALUE_USD, 2),
                    "sequence_realized_value_usd": round(delivered_envelopes * PIPELINE_ENVELOPE_VALUE_USD, 2),
                    "pipeline_contract_value_usd": round(
                        sum(float(target.get("offer_value_usd") or 0.0) for target in outreach_targets), 2
                    ),
                    "pipeline_expected_value_usd": round(
                        sum(float(target.get("expected_value_usd") or 0.0) for target in outreach_targets), 2
                    ),
                }
                append_automation_cycle(cycle)
                append_audit_record("automation_cycle", payload, "success", cycle)
                st.success("Automation cycle completed and logged.")
            except requests.RequestException as err:
                append_audit_record("automation_cycle", payload, "failed", {"error": str(err)})
                st.error(f"Automation cycle failed: {err}")

        st.markdown("### Async execution (Celery)")
        async_col_1, async_col_2 = st.columns(2)
        if async_col_1.button("Queue Celery Automation Cycle", use_container_width=True):
            try:
                task_id = enqueue_celery_task("automation.run_cycle")
                st.success(f"Queued automation task: {task_id}")
            except (ImportError, ModuleNotFoundError, RuntimeError, ValueError) as err:
                st.error(f"Celery queue failed: {err}")
        if async_col_2.button("Queue Celery Conversation Run", use_container_width=True):
            try:
                task_id = enqueue_celery_task("automation.run_conversation", int(max_results_auto))
                st.success(f"Queued conversation task: {task_id}")
            except (ImportError, ModuleNotFoundError, RuntimeError, ValueError) as err:
                st.error(f"Celery queue failed: {err}")

        st.markdown("### Repo hunter status")
        repo_hunt_report = load_repo_hunt_report()
        if not repo_hunt_report:
            st.info("No repo hunt report found yet.")
        else:
            st.metric("Repo targets found", repo_hunt_report["totals"]["targets_found"])
            st.dataframe(pd.DataFrame(repo_hunt_report["repo_targets"][:20]), use_container_width=True)

        st.markdown("### Bounty contract consolidation")
        bounty_contract_report = load_bounty_contract_report()
        if not bounty_contract_report:
            st.info("No consolidated bounty contracts yet.")
        else:
            bcc1, bcc2, bcc3 = st.columns(3)
            bcc1.metric("Contracts", int(bounty_contract_report["totals"]["contract_count"]))
            bcc2.metric("Contract value", f"${bounty_contract_report['totals']['contract_value_usd']:.2f}")
            bcc3.metric("Expected value", f"${bounty_contract_report['totals']['expected_value_usd']:.2f}")
            st.dataframe(pd.DataFrame(bounty_contract_report["contracts"][:20]), use_container_width=True)

        st.markdown("### Recent outreach events")
        outreach_events = load_outreach_events(limit=25)
        if not outreach_events:
            st.info("No outreach events logged yet.")
        else:
            st.dataframe(pd.DataFrame(outreach_events), use_container_width=True)

        st.markdown("### Recent automation cycles")
        cycles = load_automation_cycles()
        if not cycles:
            st.info("No automation cycles logged yet.")
        else:
            st.dataframe(pd.DataFrame(cycles), use_container_width=True)

    with tab6:
        st.subheader("💵 MONEY MACHINE")
        st.caption("Turn detected opportunities into paid actions with direct checkout and sales tracking.")

        has_checkout = bool(OFFER_SCOUTING_URL or OFFER_AUTOMATION_URL or OFFER_ENTERPRISE_URL)
        status_col_1, status_col_2, status_col_3 = st.columns(3)
        status_col_1.metric("Payment Rail", PAYMENT_PROVIDER)
        status_col_2.metric("Checkout Links", "Online" if has_checkout else "Missing")
        status_col_3.metric("Booking Link", "Online" if BOOKING_URL else "Missing")

        st.markdown("### Productized Offers")
        offer_col_1, offer_col_2, offer_col_3 = st.columns(3)

        if OFFER_SCOUTING_URL:
            offer_col_1.link_button("Buy: Bio Prospecting Sprint", OFFER_SCOUTING_URL, use_container_width=True)
        else:
            offer_col_1.warning("Set OFFER_SCOUTING_URL")

        if OFFER_AUTOMATION_URL:
            offer_col_2.link_button("Buy: Automation Build Sprint", OFFER_AUTOMATION_URL, use_container_width=True)
        else:
            offer_col_2.warning("Set OFFER_AUTOMATION_URL")

        if OFFER_ENTERPRISE_URL:
            offer_col_3.link_button("Buy: Enterprise Agent System", OFFER_ENTERPRISE_URL, use_container_width=True)
        else:
            offer_col_3.warning("Set OFFER_ENTERPRISE_URL")

        if BOOKING_URL:
            st.link_button("Book Sales Call", BOOKING_URL, use_container_width=True)
        else:
            st.info("Set BOOKING_URL to enable call booking from this tab.")

        st.divider()
        st.markdown("### Engage Pipeline")
        engage_results = st.slider(
            "Engage cycle lead volume",
            min_value=5,
            max_value=50,
            value=max(5, min(50, AUTONOMOUS_LEAD_VOLUME)),
            step=5,
            key="mm_results",
        )
        st.session_state.setdefault("autonomous_cycle_last_ts", 0.0)
        now_ts = datetime.now(timezone.utc).timestamp()
        cycle_due = (now_ts - float(st.session_state.get("autonomous_cycle_last_ts", 0.0))) >= float(
            AUTONOMOUS_CYCLE_INTERVAL_SECONDS
        )
        if AUTONOMOUS_AGENT_MODE and cycle_due:
            with st.spinner("Autonomous agent cycle running..."):
                try:
                    auto_result = run_money_machine_cycle(engage_results, has_checkout=has_checkout)
                    st.session_state["autonomous_cycle_last_ts"] = now_ts
                    st.success(
                        "Autonomous cycle complete: "
                        f"{auto_result['combined_leads']} leads, {auto_result['delivered_envelopes']} delivered."
                    )
                except requests.RequestException as err:
                    append_audit_record(
                        "money_machine_engaged",
                        {"max_results": engage_results, "mode": "autonomous"},
                        "failed",
                        {"error": str(err)},
                    )
                    st.error(f"Autonomous cycle failed: {err}")
        if st.button("ENGAGE MONEY MACHINE", use_container_width=True):
            try:
                manual_result = run_money_machine_cycle(engage_results, has_checkout=has_checkout)
                st.success(
                    "Money Machine engaged. "
                    f"{manual_result['combined_leads']} leads, {manual_result['delivered_envelopes']} delivered."
                )
            except requests.RequestException as err:
                append_audit_record(
                    "money_machine_engaged",
                    {"max_results": engage_results, "mode": "manual"},
                    "failed",
                    {"error": str(err)},
                )
                st.error(f"Money Machine cycle failed: {err}")

        st.markdown("### Revenue Logging")
        st.info("Revenue is auto-ingested from agent settlement events (no manual payment entry required).")

        sales_events = load_sales_events()
        st.markdown("### Recent sales events")
        if not sales_events:
            st.info("No sales events logged yet.")
        else:
            st.dataframe(pd.DataFrame(sales_events), use_container_width=True)

        st.divider()
        st.markdown("### Revenue scoreboard")
        scoreboard = build_revenue_scoreboard()
        sb1, sb2, sb3, sb4 = st.columns(4)
        sb1.metric("Sequences run", int(scoreboard["sequence_runs"]))
        sb2.metric("Outreach sent", int(scoreboard["outreach_sent"]))
        sb3.metric("Envelopes delivered", int(scoreboard["delivered_envelopes"]))
        sb4.metric("Sequence revenue", f"${scoreboard['revenue_usd']:.2f}")
        sb5, sb6, sb7, sb8 = st.columns(4)
        sb5.metric("Sequence potential", f"${scoreboard['sequence_potential_value_usd']:.2f}")
        sb6.metric("Sequence leakage", f"${scoreboard['sequence_leakage_usd']:.2f}")
        sb7.metric("Pipeline expected", f"${scoreboard['pipeline_expected_value_usd']:.2f}")
        sb8.metric("Leakage risk", f"${scoreboard['expected_leakage_usd']:.2f}")
        sb9, sb10, sb11, sb12 = st.columns(4)
        sb9.metric("Pipeline contract", f"${scoreboard['pipeline_contract_value_usd']:.2f}")
        sb10.metric("Contract gap", f"${scoreboard['contract_gap_usd']:.2f}")
        sb11.metric("Cash collected", f"${scoreboard['cash_revenue_usd']:.2f}")
        sb12.metric("Revenue / outreach", f"${scoreboard['revenue_per_outreach']:.2f}")
        sb13, sb14, sb15 = st.columns(3)
        sb13.metric("Bounty contracts", int(scoreboard["bounty_contract_count"]))
        sb14.metric("Bounty contract value", f"${scoreboard['bounty_contract_value_usd']:.2f}")
        sb15.metric("Bounty expected", f"${scoreboard['bounty_expected_value_usd']:.2f}")
        st.metric("Conversation envelopes", int(scoreboard["conversation_envelopes"]))
        st.markdown("### Profit & Loss")
        profit_loss = build_profit_and_loss()
        pl1, pl2, pl3 = st.columns(3)
        pl1.metric("Total Revenue (Settled Bounties)", f"${profit_loss['total_revenue_usd']:.2f}")
        pl2.metric("Pending Leads (Awaiting Payment)", int(profit_loss["pending_leads"]))
        pl3.metric("Operational Cost (Railway/Agentverse)", f"${profit_loss['operational_cost_usd']:.2f}")

        st.markdown("### Outreach activity")
        st.info("Outreach metrics are agent-driven from protocol dispatch events.")

        st.divider()
        st.markdown("### Golden Sequence Snapshot (Agent-Driven)")
        golden_limit = max(5, min(50, AUTONOMOUS_GOLDEN_LIMIT))
        try:
            report = load_prospector_report() or build_income_prospector_report(max_results=golden_limit)
            genetic_leads = report.get("genetic_leads", [])[:golden_limit]
            htt_carrier_active = bool((st.session_state.get("latest_htt_carrier_event") or {}).get("high_tension"))
            auto_query = "huntingtin" if htt_carrier_active else "genetic"
            filtered = [
                lead
                for lead in genetic_leads
                if auto_query in (lead.get("title") or "").lower() or auto_query in (lead.get("source") or "").lower()
            ]
            final_leads = filtered if filtered else genetic_leads
            st.caption(f"Auto query: `{auto_query}` · lead depth: {golden_limit}")
            st.dataframe(pd.DataFrame(final_leads), use_container_width=True)
            append_audit_record(
                "golden_sequence_snapshot",
                {"query": auto_query, "max_results": golden_limit},
                "success",
                {"matches": len(final_leads)},
            )
        except requests.RequestException as err:
            append_audit_record(
                "golden_sequence_snapshot",
                {"max_results": golden_limit},
                "failed",
                {"error": str(err)},
            )
            st.error(f"Golden sequence snapshot failed: {err}")

    with tab7:
        st.subheader("🧪 AI Studio Apps Wired Live")
        st.caption("Runs AI Studio analyzers and data feeds from GitHub raw sources (or optional file paths).")

        repo_col_1, repo_col_2 = st.columns(2)
        repo_col_1.metric("HTT source", "Online" if source_online(AISTUDIO_HTT_ANALYZE_SOURCE) else "Missing")
        repo_col_2.metric("CRISPR source", "Online" if source_online(AISTUDIO_CRISPR_ANALYZE_SOURCE) else "Missing")
        st.caption(f"HTT analyze source: `{AISTUDIO_HTT_ANALYZE_SOURCE}`")
        st.caption(f"CRISPR analyze source: `{AISTUDIO_CRISPR_ANALYZE_SOURCE}`")

        signal_col_1, signal_col_2, signal_col_3 = st.columns(3)
        signal_col_1.metric("HTT smoke rows", read_smoke_rows(AISTUDIO_HTT_SMOKE_DATA_SOURCE))
        signal_col_2.metric("CRISPR smoke rows", read_smoke_rows(AISTUDIO_CRISPR_SMOKE_DATA_SOURCE))
        payout_signal = read_json_source(AISTUDIO_HTT_PAYOUT_SIGNAL_SOURCE)
        payout_amount = float((payout_signal or {}).get("invoice_amount") or 0.0) if isinstance(payout_signal, dict) else 0.0
        signal_col_3.metric("HTT payout signal", f"${payout_amount:.2f}")

        active_leads = read_json_source(AISTUDIO_HTT_ACTIVE_LEADS_SOURCE)
        if active_leads:
            st.markdown("### Active leads from HTT app")
            st.dataframe(pd.DataFrame(active_leads), use_container_width=True)
        else:
            st.info("No active leads source detected yet from HTT app.")

        if payout_signal:
            st.markdown("### Payout signal from HTT app")
            st.json(payout_signal)

        st.divider()
        st.markdown("### Cross-app live sequence scoring")
        seq_input = st.text_area(
            "Sequence (23+ DNA bases, A/C/G/T)",
            value="",
            help="This runs both analyzers directly from your configured AI Studio sources.",
        )
        if st.button("Run AI Studio Dual Score", use_container_width=True):
            try:
                score = aistudio_dual_score(seq_input)
                st.success("Live dual scoring complete.")
                m1, m2 = st.columns(2)
                m1.metric("HTT λ score", f"{score['htt_lambda_score']:.6f}")
                m2.metric("CRISPR λ score", f"{score['crispr_lambda_score']:.6f}")
                append_audit_record("aistudio_dual_score", {"sequence_len": len(seq_input)}, "success", score)
            except (FileNotFoundError, ImportError, AttributeError, ValueError) as err:
                append_audit_record("aistudio_dual_score", {"sequence_len": len(seq_input)}, "failed", {"error": str(err)})
                st.error(f"AI Studio dual score failed: {err}")

        st.divider()
        st.markdown("### HTT Carrier Protocol (SPOPS + Watchdog)")
        st.caption(
            "Operational heuristic only. This does not provide medical diagnosis and should not be used for clinical decisions."
        )
        if st.button("Run HTT Carrier Protocol", use_container_width=True):
            payload = {"chromosome": 4, "sequence_len": len(seq_input), "threshold_repeats": HTT_CAG_CARRIER_THRESHOLD}
            try:
                carrier_result = run_htt_carrier_protocol(seq_input)
                append_audit_record("htt_carrier_protocol", payload, "success", carrier_result)
                st.session_state["latest_voxel_stream"] = carrier_result.get("voxel_stream", [])
                st.session_state["external_buffer_stream"] = carrier_result.get("external_buffer_stream", [])
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Max CAG repeats", carrier_result["max_cag_repeats"])
                c2.metric("Sync variance V", f"{carrier_result['sync_variance_v']:.6f}")
                c3.metric("Unique voxels", int(carrier_result["unique_voxels"]))
                c4.metric("Aperture", f"1:{carrier_result['precision_aperture']:,}")
                ping_count = int(carrier_result.get("skyfire_ping_count") or 0)
                for ping_index in range(ping_count):
                    append_sales_event(
                        {
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                            "event_type": "skyfire_ping",
                            "bytes_processed": int(carrier_result.get("processed_bytes") or 0),
                            "processed_megabytes": float(carrier_result.get("processed_megabytes") or 0.0),
                            "ping_index": ping_index + 1,
                            "ping_total": ping_count,
                            "chromosome": 4,
                        }
                    )
                st.session_state["skyfire_ping_total"] = int(st.session_state.get("skyfire_ping_total", 0)) + ping_count
                st.session_state["skyfire_status"] = f"Heartbeat x{ping_count}"
                st.caption(
                    f"Differential area: {carrier_result['differential_area']:.6f} "
                    f"(baseline {carrier_result['baseline_differential_area']:.6f})"
                )
                st.caption(
                    f"Processed {carrier_result['processed_bytes']:,} bytes "
                    f"({carrier_result['processed_megabytes']:.6f} MB) · Skyfire pings: {ping_count}"
                )
                st.caption(
                    "Stretch voxel UID64: "
                    f"`{carrier_result['stretch_voxel']['uid64_hex']}` at "
                    f"({carrier_result['stretch_voxel']['x']:.6f}, "
                    f"{carrier_result['stretch_voxel']['y']:.6f}, "
                    f"{carrier_result['stretch_voxel']['z']:.6f})"
                )
                st.markdown("#### Full 64-bit voxel aperture stream")
                st.dataframe(pd.DataFrame(carrier_result["voxel_stream"]), use_container_width=True, height=260)
                if carrier_result["high_tension"]:
                    st.error("🔱 HIGH-TENSION DRIFT DETECTED: HTT CARRIER CANDIDATE")
                    st.session_state["skyfire_status"] = "Awaiting Payout"
                if float(carrier_result.get("sync_variance_v") or 0.0) > SKYFIRE_TENSION_TRIGGER:
                    bounty_agent = DEAL_AGENT_ADDRESS or AGENT_REGISTRY.get("spartan-db", "")
                    if not bounty_agent:
                        st.error("Skyfire Seller Gateway claim skipped: missing payout agent mapping.")
                    else:
                        auto_bounty_event = {
                            "event_type": "high_tension_detected",
                            "sync_variance_v": float(carrier_result.get("sync_variance_v") or 0.0),
                            "high_tension": bool(carrier_result.get("high_tension")),
                            "max_cag_repeats": int(carrier_result.get("max_cag_repeats") or 0),
                            "precision_fingerprint": int(carrier_result.get("precision_fingerprint") or 0),
                            "lead_ref": str(carrier_result.get("precision_fingerprint") or ""),
                            "stretch_voxel": carrier_result.get("stretch_voxel"),
                        }
                        try:
                            claim_result = claim_skyfire_seller_bounty(
                                carrier_result=carrier_result,
                                manifest_event=auto_bounty_event,
                                agent=bounty_agent,
                            )
                            if claim_result["settled"]:
                                st.session_state["skyfire_status"] = "Bounty Settled"
                                st.success(f"Skyfire Seller Gateway settled bounty: {SKYFIRE_BOUNTY_UNITS:.0f} units.")
                            else:
                                st.session_state["skyfire_status"] = "Bounty Pending"
                                st.warning(f"Skyfire Seller Gateway claim created: {SKYFIRE_BOUNTY_UNITS:.0f} units pending.")
                        except (ValueError, requests.RequestException) as err:
                            st.session_state["skyfire_status"] = "Bounty Claim Failed"
                            append_audit_record(
                                "skyfire_seller_bounty_claim",
                                {"sync_variance_v": float(carrier_result.get("sync_variance_v") or 0.0)},
                                "failed",
                                {"error": str(err)},
                            )
                            st.error(f"Skyfire Seller Gateway claim failed: {err}")
                if carrier_result["tier1_carrier_identified"]:
                    st.warning("🔱 Tier-1 Carrier Identified")
                    deal_agent = DEAL_AGENT_ADDRESS or AGENT_REGISTRY.get("spartan-db", "")
                    carrier_event = {
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "event_type": "tier1_carrier_identified",
                        "lead_ref": str(carrier_result["precision_fingerprint"]),
                        "agent": "SPOPS+watchdog",
                        "spops_agent": carrier_result["spops_agent"],
                        "watchdog_agent": carrier_result["watchdog_agent"],
                        "deal_agent": deal_agent,
                        "max_cag_repeats": carrier_result["max_cag_repeats"],
                        "chromosome": 4,
                        "sync_variance_v": carrier_result["sync_variance_v"],
                        "high_tension": carrier_result["high_tension"],
                        "differential_area": carrier_result["differential_area"],
                        "baseline_differential_area": carrier_result["baseline_differential_area"],
                        "stretch_voxel": carrier_result["stretch_voxel"],
                        "precision_aperture": carrier_result["precision_aperture"],
                        "precision_fingerprint": carrier_result["precision_fingerprint"],
                    }
                    st.session_state["latest_htt_carrier_event"] = carrier_event
                    append_outreach_event(carrier_event)

                    settlement_payload = {
                        "event": "diagnostic_success_fee",
                        "amount_usd": HTT_SETTLEMENT_AMOUNT_USD,
                        "agent": deal_agent,
                        "manifest_event": carrier_event,
                    }
                    try:
                        settlement_result = dispatch_control_event(RAILWAY_MESH_URL, "diagnostic_success_fee", settlement_payload)
                        settlement_status = str(settlement_result.get("status") or "").lower()
                        settlement_is_settled = bool(
                            settlement_result.get("settled")
                            or settlement_result.get("paid")
                            or settlement_status in {"paid", "settled", "completed", "success"}
                        )
                        append_sales_event(
                            {
                                "timestamp": datetime.now(timezone.utc).isoformat(),
                                "event_type": "diagnostic_payout_settled" if settlement_is_settled else "diagnostic_payout_requested",
                                "amount_usd": HTT_SETTLEMENT_AMOUNT_USD,
                                "agent": deal_agent,
                                "lead_ref": str(carrier_result["precision_fingerprint"]),
                                "result": settlement_result,
                            }
                        )
                        if settlement_is_settled:
                            st.success(f"Skyfire settlement completed: ${HTT_SETTLEMENT_AMOUNT_USD:.2f}")
                        else:
                            st.success(f"Skyfire settlement relay requested: ${HTT_SETTLEMENT_AMOUNT_USD:.2f}")
                    except (ValueError, requests.RequestException) as err:
                        st.session_state["skyfire_status"] = "Payout Relay Failed"
                        append_audit_record("diagnostic_success_fee", settlement_payload, "failed", {"error": str(err)})
                        st.error(f"Settlement relay failed: {err}")
                else:
                    st.session_state["latest_htt_carrier_event"] = {
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "event_type": "htt_tension_scan",
                        "chromosome": 4,
                        "high_tension": carrier_result["high_tension"],
                        "sync_variance_v": carrier_result["sync_variance_v"],
                        "differential_area": carrier_result["differential_area"],
                        "baseline_differential_area": carrier_result["baseline_differential_area"],
                        "stretch_voxel": carrier_result["stretch_voxel"],
                        "precision_aperture": carrier_result["precision_aperture"],
                        "precision_fingerprint": carrier_result["precision_fingerprint"],
                    }
                    if not carrier_result["high_tension"]:
                        st.session_state["skyfire_status"] = "Idle"
                    st.info("No Tier-1 carrier event detected under current threshold.")
                st.json(carrier_result)
            except ValueError as err:
                append_audit_record("htt_carrier_protocol", payload, "failed", {"error": str(err)})
                st.error(f"HTT carrier protocol failed: {err}")


if __name__ == "__main__":
    main()
