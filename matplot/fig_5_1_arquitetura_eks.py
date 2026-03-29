import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
import matplotlib.patheffects as pe

fig, ax = plt.subplots(figsize=(16, 11))
fig.patch.set_facecolor('#F8F9FA')
ax.set_xlim(0, 16)
ax.set_ylim(0, 11)
ax.axis('off')

# ── Color palette ─────────────────────────────────────────────────────────────
C_EKS_BG   = '#EBF5FB'
C_EKS_BD   = '#2E86C1'
C_NODE_BG  = '#FDFEFE'
C_NODE_BD  = '#5D6D7E'
C_DRIVER   = '#1A5276'
C_EXEC     = '#117A65'
C_EXEC_L   = '#D5F5E3'
C_EXEC_BD  = '#1E8449'
C_S3       = '#B7950B'
C_S3_BG    = '#FEF9E7'
C_S3_BD    = '#F39C12'
C_OP_BG    = '#F5EEF8'
C_OP_BD    = '#7D3C98'
C_IRSA     = '#E74C3C'
C_ARROW    = '#2C3E50'
C_ARROW2   = '#E67E22'
C_TITLE    = '#1A1A2E'
C_SUB      = '#5D6D7E'

def box(ax, x, y, w, h, fc, ec, lw=1.2, radius=0.25, alpha=1.0):
    ax.add_patch(FancyBboxPatch((x, y), w, h,
        boxstyle=f'round,pad=0.0,rounding_size={radius}',
        facecolor=fc, edgecolor=ec, linewidth=lw, alpha=alpha, zorder=2))

def label(ax, x, y, txt, fs=9, color='#1a1a1a', bold=False, ha='center', va='center', zorder=5):
    ax.text(x, y, txt, fontsize=fs, color=color,
            fontweight='bold' if bold else 'normal',
            ha=ha, va=va, zorder=zorder)

def arrow(ax, x1, y1, x2, y2, color=C_ARROW, lw=1.6, style='->', label='', lc=None):
    ax.annotate('', xy=(x2, y2), xytext=(x1, y1),
                arrowprops=dict(arrowstyle=style, color=color,
                                lw=lw, connectionstyle='arc3,rad=0.0'))
    if label:
        mx, my = (x1+x2)/2, (y1+y2)/2
        ax.text(mx, my+0.15, label, fontsize=7.5,
                color=lc or color, ha='center', va='bottom',
                bbox=dict(boxstyle='round,pad=0.2', fc='white', ec='none', alpha=0.85))

def curved_arrow(ax, x1, y1, x2, y2, color, rad=0.25, lw=1.6, label=''):
    ax.annotate('', xy=(x2, y2), xytext=(x1, y1),
                arrowprops=dict(arrowstyle='->', color=color,
                                lw=lw, connectionstyle=f'arc3,rad={rad}'))
    if label:
        mx = (x1+x2)/2 + (0.3 if rad > 0 else -0.3)
        my = (y1+y2)/2 + abs(rad)*1.2
        ax.text(mx, my, label, fontsize=7.5, color=color, ha='center',
                bbox=dict(boxstyle='round,pad=0.2', fc='white', ec='none', alpha=0.85))

def pod_box(ax, x, y, w, h, label_top, label_sub, fc, ec, badge=None):
    box(ax, x, y, w, h, fc, ec, lw=1.2, radius=0.15)
    ax.text(x+w/2, y+h*0.62, label_top, fontsize=8, color='white',
            fontweight='bold', ha='center', va='center', zorder=6)
    ax.text(x+w/2, y+h*0.28, label_sub, fontsize=6.8, color='white',
            ha='center', va='center', zorder=6, alpha=0.9)
    if badge:
        ax.add_patch(FancyBboxPatch((x+w-0.52, y+h-0.28), 0.48, 0.22,
            boxstyle='round,pad=0.02', facecolor=C_IRSA,
            edgecolor='white', linewidth=0.6, zorder=7))
        ax.text(x+w-0.28, y+h-0.17, 'IRSA', fontsize=5.5,
                color='white', ha='center', va='center', zorder=8, fontweight='bold')

# ── Title ─────────────────────────────────────────────────────────────────────
ax.text(8, 10.55, 'Arquitetura do Pipeline — Spark Operator + Amazon EKS',
        fontsize=14, fontweight='bold', ha='center', va='center',
        color=C_TITLE)
ax.text(8, 10.15, 'Experimento E2 · Dataset 3W (Petrobras) · 2 nós c6g.xlarge · 9 executors',
        fontsize=10, ha='center', va='center', color=C_SUB)

# ── S3 Bucket (left) ──────────────────────────────────────────────────────────
box(ax, 0.3, 7.2, 2.4, 2.4, C_S3_BG, C_S3_BD, lw=2.0, radius=0.3)
ax.text(1.5, 9.2, 'Amazon S3', fontsize=10, fontweight='bold',
        ha='center', va='center', color=C_S3, zorder=5)

# S3 folders
for i, (name, col) in enumerate([
    ('3w/raw/', '#7D6608'),
    ('3w/staging/', '#5D4037'),
    ('3w/results/', '#1A5276'),
]):
    fy = 8.75 - i * 0.45
    ax.add_patch(FancyBboxPatch((0.45, fy-0.16), 2.1, 0.32,
        boxstyle='round,pad=0.04', facecolor='white',
        edgecolor=col, linewidth=0.8, zorder=3))
    ax.text(1.5, fy, f'→ {name}', fontsize=7.5, ha='center',
            va='center', color=col, zorder=5)

# ── Spark Operator (right top) ────────────────────────────────────────────────
box(ax, 13.2, 7.8, 2.5, 1.6, C_OP_BG, C_OP_BD, lw=1.8, radius=0.3)
ax.text(14.45, 9.05, '⚙  Spark Operator', fontsize=9.5,
        fontweight='bold', ha='center', color=C_OP_BD, zorder=5)
ax.text(14.45, 8.65, 'kubeflow/spark-operator', fontsize=7.5,
        ha='center', color=C_SUB, zorder=5)
ax.text(14.45, 8.27, 'CRD: SparkApplication', fontsize=7.5,
        ha='center', color=C_SUB, zorder=5)
ax.text(14.45, 7.95, 'manages driver + executors', fontsize=7.0,
        ha='center', color=C_OP_BD, zorder=5, style='italic')

# ── EKS Cluster border ────────────────────────────────────────────────────────
box(ax, 2.9, 0.5, 12.8, 7.0, C_EKS_BG, C_EKS_BD, lw=2.2, radius=0.5, alpha=0.5)
ax.text(3.3, 7.25, 'Amazon EKS Cluster', fontsize=9.5,
        fontweight='bold', color=C_EKS_BD, ha='left', va='center', zorder=5)
ax.text(3.3, 6.9, 'Kubernetes v1.29 · Namespace: spark-jobs',
        fontsize=8, color=C_SUB, ha='left', va='center', zorder=5)

# ── Node 1 ────────────────────────────────────────────────────────────────────
box(ax, 3.1, 0.7, 5.8, 5.9, C_NODE_BG, C_NODE_BD, lw=1.5, radius=0.35)
ax.text(6.0, 6.35, 'Nó 1 — c6g.xlarge (ARM Graviton2)',
        fontsize=8.5, fontweight='bold', ha='center', color=C_NODE_BD, zorder=5)
ax.text(6.0, 6.0, '4 vCPU · 16 GB RAM · alocável: 3.900m CPU',
        fontsize=7.5, ha='center', color=C_SUB, zorder=5)

# Driver pod
pod_box(ax, 3.4, 3.8, 2.5, 1.9,
        'Driver Pod',
        '2 cores · 2 GB RAM\nServiceAccount: spark',
        C_DRIVER, '#0D3349', badge=True)

# Executors on Node 1 (3 pods)
for i in range(3):
    px = 3.35 + i * 1.82
    pod_box(ax, px, 1.0, 1.65, 1.6,
            f'Exec {i+1}',
            'coreReq: 700m\n1.5 GB RAM',
            C_EXEC, C_EXEC_BD, badge=True)

ax.text(6.0, 0.78, f'Executors 1–3 (de 9 total)',
        fontsize=7, ha='center', color=C_SUB, zorder=5, style='italic')

# kubelet badge Node 1
ax.add_patch(FancyBboxPatch((3.15, 2.7), 1.1, 0.32,
    boxstyle='round,pad=0.04', facecolor='#EBF5FB',
    edgecolor=C_EKS_BD, linewidth=0.7, zorder=4))
ax.text(3.7, 2.86, 'kubelet', fontsize=7.5,
        ha='center', color=C_EKS_BD, zorder=5, fontweight='bold')

# ── Node 2 ────────────────────────────────────────────────────────────────────
box(ax, 9.5, 0.7, 5.8, 5.9, C_NODE_BG, C_NODE_BD, lw=1.5, radius=0.35)
ax.text(12.4, 6.35, 'Nó 2 — c6g.xlarge (ARM Graviton2)',
        fontsize=8.5, fontweight='bold', ha='center', color=C_NODE_BD, zorder=5)
ax.text(12.4, 6.0, '4 vCPU · 16 GB RAM · alocável: 3.900m CPU',
        fontsize=7.5, ha='center', color=C_SUB, zorder=5)

# Executors on Node 2 (6 pods, 2 rows of 3)
exec_labels = [4,5,6,7,8,9]
for i in range(3):
    px = 9.75 + i * 1.82
    pod_box(ax, px, 3.3, 1.65, 1.6,
            f'Exec {exec_labels[i]}',
            'coreReq: 700m\n1.5 GB RAM',
            C_EXEC, C_EXEC_BD, badge=True)
for i in range(3):
    px = 9.75 + i * 1.82
    pod_box(ax, px, 1.0, 1.65, 1.6,
            f'Exec {exec_labels[i+3]}',
            'coreReq: 700m\n1.5 GB RAM',
            C_EXEC, C_EXEC_BD, badge=True)

ax.text(12.4, 0.78, 'Executors 4–9 (de 9 total)',
        fontsize=7, ha='center', color=C_SUB, zorder=5, style='italic')

# kubelet badge Node 2
ax.add_patch(FancyBboxPatch((9.55, 2.6), 1.1, 0.32,
    boxstyle='round,pad=0.04', facecolor='#EBF5FB',
    edgecolor=C_EKS_BD, linewidth=0.7, zorder=4))
ax.text(10.1, 2.76, 'kubelet', fontsize=7.5,
        ha='center', color=C_EKS_BD, zorder=5, fontweight='bold')

# ── DATA FLOW ARROWS ──────────────────────────────────────────────────────────

# 1) S3 raw → Driver (pyarrow read)
ax.annotate('', xy=(3.4, 4.6), xytext=(2.7, 8.2),
    arrowprops=dict(arrowstyle='->', color='#1A5276', lw=1.8,
                    connectionstyle='arc3,rad=-0.15'))
ax.text(2.2, 6.5, '① Lê 3W\n(pyarrow\nboto3)', fontsize=7.5,
        color='#1A5276', ha='center', va='center', zorder=6,
        bbox=dict(boxstyle='round,pad=0.25', fc='white', ec='#1A5276', alpha=0.9, lw=0.8))

# 2) Driver → S3 staging write
ax.annotate('', xy=(2.7, 7.5), xytext=(3.4, 4.2),
    arrowprops=dict(arrowstyle='->', color=C_ARROW2, lw=1.8,
                    connectionstyle='arc3,rad=0.2'))
ax.text(1.55, 6.0, '② Escreve\nstaging', fontsize=7.5,
        color=C_ARROW2, ha='center', va='center', zorder=6,
        bbox=dict(boxstyle='round,pad=0.25', fc='white', ec=C_ARROW2, alpha=0.9, lw=0.8))

# 3) S3 staging → Executors Node 1 (read)
ax.annotate('', xy=(4.2, 2.6), xytext=(2.7, 7.2),
    arrowprops=dict(arrowstyle='->', color=C_EXEC_BD, lw=1.8,
                    connectionstyle='arc3,rad=0.3'))
ax.text(2.05, 4.8, '③ Lê\nstaging', fontsize=7.5,
        color=C_EXEC_BD, ha='center', va='center', zorder=6,
        bbox=dict(boxstyle='round,pad=0.25', fc='white', ec=C_EXEC_BD, alpha=0.9, lw=0.8))

# 4) S3 staging → Executors Node 2 (read)
ax.annotate('', xy=(11.0, 4.0), xytext=(2.7, 7.5),
    arrowprops=dict(arrowstyle='->', color=C_EXEC_BD, lw=1.8,
                    connectionstyle='arc3,rad=0.15'))

# 5) Executors → S3 results write
ax.annotate('', xy=(2.7, 7.8), xytext=(12.5, 2.6),
    arrowprops=dict(arrowstyle='->', color=C_S3, lw=1.8,
                    connectionstyle='arc3,rad=-0.1'))
ax.text(7.5, 0.75, '④ Escreve resultados (36.182 janelas)',
        fontsize=7.5, color=C_S3, ha='center', va='center', zorder=6,
        bbox=dict(boxstyle='round,pad=0.25', fc='white', ec=C_S3, alpha=0.9, lw=0.8))

# 6) Spark Operator → Driver (manages)
ax.annotate('', xy=(5.9, 5.7), xytext=(13.2, 8.5),
    arrowprops=dict(arrowstyle='->', color=C_OP_BD, lw=1.5,
                    connectionstyle='arc3,rad=-0.2', linestyle='dashed'))
ax.text(10.0, 7.6, 'cria/monitora\nSparkApplication CRD',
        fontsize=7.5, color=C_OP_BD, ha='center', va='center', zorder=6,
        bbox=dict(boxstyle='round,pad=0.25', fc='white', ec=C_OP_BD, alpha=0.9, lw=0.8))

# 7) Driver → Executors (DAG coordination — dashed)
ax.annotate('', xy=(9.75, 2.0), xytext=(5.9, 2.0),
    arrowprops=dict(arrowstyle='<->', color='#5D6D7E', lw=1.2,
                    connectionstyle='arc3,rad=0.0', linestyle='dashed'))
ax.text(7.8, 2.2, 'DAG / shuffle', fontsize=7,
        color='#5D6D7E', ha='center', va='bottom', zorder=6,
        bbox=dict(boxstyle='round,pad=0.2', fc='white', ec='none', alpha=0.85))

# ── IRSA legend box ───────────────────────────────────────────────────────────
box(ax, 9.6, 7.8, 3.3, 1.6, '#FDEDEC', C_IRSA, lw=1.4, radius=0.25)
ax.text(11.25, 9.08, 'IRSA — IAM Role for Service Accounts',
        fontsize=8, fontweight='bold', ha='center', color=C_IRSA, zorder=5)
ax.text(11.25, 8.72, 'serviceAccount: spark → IAM Role ARN',
        fontsize=7.5, ha='center', color='#555555', zorder=5)
ax.text(11.25, 8.40, 'WebIdentityTokenFile → AWS STS → S3 access',
        fontsize=7.5, ha='center', color='#555555', zorder=5)
ax.text(11.25, 8.05, 'Sem credenciais hardcoded nos YAMLs',
        fontsize=7.5, ha='center', color=C_IRSA, zorder=5, style='italic')

# ── Spark config summary ──────────────────────────────────────────────────────
box(ax, 0.2, 0.3, 2.6, 6.7, '#F0F3F4', '#AAB7B8', lw=1.0, radius=0.2)
ax.text(1.5, 6.75, 'Configuração Spark', fontsize=8.5,
        fontweight='bold', ha='center', color='#2C3E50', zorder=5)

cfg_lines = [
    ('Driver', None, False),
    ('  cores: 2', '#1A5276', False),
    ('  memory: "2g"', '#1A5276', False),
    ('  svcAccount: spark', '#1A5276', False),
    ('', None, False),
    ('Executor (×9)', None, False),
    ('  instances: 9', C_EXEC_BD, False),
    ('  cores: 2', C_EXEC_BD, False),
    ('  coreRequest: 700m', C_EXEC_BD, False),
    ('  coreLimit: 1500m', C_EXEC_BD, False),
    ('  memory: 1500m', C_EXEC_BD, False),
    ('  svcAccount: spark', C_EXEC_BD, False),
    ('', None, False),
    ('Spark SS', None, False),
    ('  trigger: once=True', '#5D4037', False),
    ('  outputMode: complete', '#5D4037', False),
    ('  shuffle.parts: 8', '#5D4037', False),
    ('  watermark: 5s', '#5D4037', False),
]

y_cfg = 6.35
for line, color, bold in cfg_lines:
    if line:
        ax.text(0.35, y_cfg, line, fontsize=7.2,
                color=color or '#2C3E50', ha='left', va='center', zorder=5,
                fontfamily='monospace',
                fontweight='bold' if bold else 'normal')
    y_cfg -= 0.33

# ── Footer ────────────────────────────────────────────────────────────────────
ax.text(8, 0.2, 'Fonte: elaboração própria · Spark Operator v2.x · Amazon EKS · Spark 3.5.3 · c6g.xlarge ARM Graviton2',
        fontsize=7.5, ha='center', color='#888780')

plt.tight_layout(pad=0.3)
plt.savefig('/mnt/user-data/outputs/fig5_1_arquitetura_eks.png',
            dpi=180, bbox_inches='tight', facecolor='#F8F9FA')
plt.close()
print("OK")
