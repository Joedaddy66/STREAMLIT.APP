import streamlit as st
import numpy as np
import plotly.graph_objects as go
import hashlib
from typing import Any

# --- Visualization Functions ---

def create_voxel_helix(points: int = 200) -> go.Figure:
    theta = np.linspace(0, 8 * np.pi, points)
    z = np.linspace(0, 50, points)
    
    ENFORCED_RADIUS_UP = 17
    ENFORCED_RADIUS_DOWN = 15
    VOXEL_SCALE = 1024

    x1_f = np.cos(theta) * ENFORCED_RADIUS_UP
    y1_f = np.sin(theta) * ENFORCED_RADIUS_UP

    seed = hashlib.sha256(f"{ENFORCED_RADIUS_UP}:{ENFORCED_RADIUS_DOWN}:{points}".encode("utf-8")).digest()
    repeated = (seed * ((points // len(seed)) + 1))[:points]
    raw = np.frombuffer(repeated, dtype=np.uint8).astype(np.int64)
    jitter = (raw % 3) - 1
    
    x2_f = np.cos(theta + np.pi) * ENFORCED_RADIUS_DOWN + (jitter / VOXEL_SCALE)
    y2_f = np.sin(theta + np.pi) * ENFORCED_RADIUS_DOWN

    x1_i = np.rint(x1_f * VOXEL_SCALE).astype(np.int64)
    y1_i = np.rint(y1_f * VOXEL_SCALE).astype(np.int64)
    x2_i = np.rint(x2_f * VOXEL_SCALE).astype(np.int64)
    y2_i = np.rint(y2_f * VOXEL_SCALE).astype(np.int64)
    z_i = np.rint(z * VOXEL_SCALE).astype(np.int64)
    z_block_i = (z_i // np.int64(64)) * np.int64(64)

    fig = go.Figure()
    fig.add_trace(go.Scatter3d(
        x=x1_i / VOXEL_SCALE, y=y1_i / VOXEL_SCALE, z=z_block_i / VOXEL_SCALE,
        mode="lines", line=dict(color="#FFD700", width=8), name=f"Lead Helix",
    ))
    fig.add_trace(go.Scatter3d(
        x=x2_i / VOXEL_SCALE, y=y2_i / VOXEL_SCALE, z=z_block_i / VOXEL_SCALE,
        mode="lines", line=dict(color="#DC143C", width=6), name=f"Lag Helix",
    ))

    fig.update_layout(
        template="plotly_dark", paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
        margin=dict(l=0, r=0, b=0, t=40),
        scene=dict(
            aspectmode="data",
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False, title=''),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False, title=''),
            zaxis=dict(showgrid=False, zeroline=False, showticklabels=False, title=''),
        ),
        legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01),
        scene_camera=dict(eye=dict(x=1.5, y=1.5, z=0.5))
    )
    return fig

def create_folding_animation() -> go.Figure:
    n_points = 100
    t = np.linspace(-1, 1, n_points)
    x_initial, y_initial, z_initial = t * 10, np.sin(t * np.pi / 2) * 0.5, np.cos(t * np.pi / 2) * 0.5
    x_final, y_final, z_final = np.sin(t * 2 * np.pi) * 5, np.cos(t * 2 * np.pi) * 5, t * 10
    
    n_frames = 30
    fig = go.Figure(
        data=[go.Scatter3d(x=x_initial, y=y_initial, z=z_initial, mode="lines", line=dict(color="#a0a0a0", width=4))],
        layout=go.Layout(updatemenus=[dict(type="buttons",
            buttons=[dict(label="Play", method="animate", args=[None, {"frame": {"duration": 50, "redraw": True}, "fromcurrent": True}])],
            x=0.1, y=0.1, xanchor="left", yanchor="bottom")]),
        frames=[go.Frame(data=[go.Scatter3d(
            x=x_initial + (x_final - x_initial) * k/n_frames,
            y=y_initial + (y_final - y_initial) * k/n_frames,
            z=z_initial + (z_final - z_initial) * k/n_frames,
            mode="lines", line=dict(color="#FFD700", width=6))]) for k in range(1, n_frames + 1)]
    )
    fig.update_layout(template="plotly_dark", paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
        margin=dict(l=0, r=0, b=0, t=0), scene=dict(xaxis=dict(visible=False), yaxis=dict(visible=False), zaxis=dict(visible=False)))
    return fig

def create_voxel_assembly_animation(data_string: str) -> go.Figure:
    n_points = len(data_string)
    if n_points != 64:
        st.error("Input string must be 64 characters long.")
        return go.Figure()

    # Final state: 4x4x4 cube
    x_final, y_final, z_final = np.mgrid[0:4, 0:4, 0:4]
    x_final, y_final, z_final = x_final.flatten(), y_final.flatten(), z_final.flatten()

    # Initial state: a line
    x_initial = np.linspace(-8, 8, n_points)
    y_initial = np.zeros(n_points)
    z_initial = np.zeros(n_points)
    
    colors = [f'hsl({int(360 * i / n_points)}, 90%, 60%)' for i in range(n_points)]

    n_frames = 40
    fig = go.Figure(
        data=[go.Scatter3d(x=x_initial, y=y_initial, z=z_initial, mode="markers", text=list(data_string),
                           marker=dict(color=colors, size=5), hoverinfo="text")],
        layout=go.Layout(updatemenus=[dict(type="buttons",
            buttons=[dict(label="Assemble", method="animate", args=[None, {"frame": {"duration": 40, "redraw": True}, "fromcurrent": True}])],
            x=0.1, y=0.1, xanchor="left", yanchor="bottom")]),
        frames=[go.Frame(data=[go.Scatter3d(
            x=x_initial + (x_final - x_initial) * k/n_frames,
            y=y_initial + (y_final - y_initial) * k/n_frames,
            z=z_initial + (z_final - z_initial) * k/n_frames,
            mode="markers", text=list(data_string), marker=dict(color=colors, size=5))]) for k in range(1, n_frames + 1)]
    )
    fig.update_layout(template="plotly_dark", paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
        margin=dict(l=0, r=0, b=0, t=0), scene=dict(aspectratio=dict(x=1,y=1,z=1),
        xaxis=dict(visible=False), yaxis=dict(visible=False), zaxis=dict(visible=False)))
    return fig

# --- Main App ---
st.set_page_config(page_title="Spartan Bio-Validate", layout="wide", initial_sidebar_state="collapsed")
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Teko:wght@700&display=swap');
    .stApp { background-color: #0f0f0f; } .stApp > header { background-color: transparent; }
    h1,h2,h3,h4,h5,h6 { color: #f0f0f0; } p,li { color: #a0a0a0; }
    a { color: #FFD700; text-decoration: none; } a:hover { color: #FFFFFF; }
    h2 { font-size: 2.5rem; border-bottom: 2px solid #DC143C; padding-bottom: 10px; margin-bottom: 30px; }
    h3 { font-size: 1.8rem; color: #FFD700; }
    .st-emotion-cache-1fjr796 { background-color: #1a1a1a; border-left: 5px solid #DC143C; border-radius: 5px; }
    .spartan-title { font-family: 'Teko', sans-serif; font-size: 3.5rem; color: #FFD700; text-transform: uppercase; letter-spacing: 2px; }
</style>
""", unsafe_allow_html=True)

st.markdown('<p class="spartan-title">🔱 SPARTAN BIO-VALIDATE</p>', unsafe_allow_html=True)
st.subheader("A case study in bootstrapping a biotech venture with an agentic AI partner.")
st.markdown("---")

# --- Visualizations ---
tab1, tab2, tab3 = st.tabs(["Voxel Helix Architecture", "Conceptual Folding Process", "Voxel Assembly Process"])

with tab1:
    st.markdown("<h3>Interactive Technology Demo</h3>", unsafe_allow_html=True)
    st.write("This is a live visualization of the core 'Voxel Helix' architecture. Adjust the slider to see how the complexity of the model changes.")
    points = st.slider("Helix Points", min_value=50, max_value=500, value=200, step=10, key="helix_points")
    st.plotly_chart(create_voxel_helix(points=points), use_container_width=True)

with tab2:
    st.markdown("<h3>Conceptual Folding Animation</h3>", unsafe_allow_html=True)
    st.write("This animation illustrates a sequence folding from a simple state into a complex structure. Press 'Play' to see the animation.")
    st.plotly_chart(create_folding_animation(), use_container_width=True)

with tab3:
    st.markdown("<h3>Voxel Assembly Animation</h3>", unsafe_allow_html=True)
    st.write("This animation shows how a 64-character linear sequence is folded and stacked into a 4x4x4 cube, the foundational 'voxel' of our analysis. Press 'Assemble' to start.")
    input_string = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
    st.text_input("Input String (64 characters)", value=input_string, disabled=True)
    st.plotly_chart(create_voxel_assembly_animation(input_string), use_container_width=True)

st.markdown("---")

# --- Main Content ---
with st.container():
    col1, col2 = st.columns([2, 1])
    with col1:
        st.markdown("<h2>The Case Study: Bootstrapping with an AI Partner</h2>", unsafe_allow_html=True)
        st.markdown("[Content from previous version]")
    with col2:
        st.markdown("<h3>About Spartan Bio-Validate</h3>", unsafe_allow_html=True)
        st.info("...")
        st.markdown("<h3>Core Technology</h3>", unsafe_allow_html=True)
        st.info("...")
        st.markdown("<h3>About the Founder</h3>", unsafe_allow_html=True)
        st.info("...")

# --- Footer ---
st.markdown("---")
st.markdown("""
<div style="text-align: center;">
    <p>&copy; 2026 Joseph E. Purvis. All Rights Reserved.</p>
    <a href="#">Twitter / X</a> <a href="#">LinkedIn</a>
</div>
""", unsafe_allow_html=True)
