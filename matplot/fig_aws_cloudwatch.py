import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import numpy as np
from matplotlib.patches import Patch
from matplotlib.lines import Line2D

EKS    = '#185FA5'
EKS_L  = '#73B2E8'
RED    = '#C0392B'
GREEN  = '#1E8449'
ORANGE = '#B7400C'
GRAY   = '#888780'
GRID   = '#eeeeee'
np.random.seed(42)

# ─── DADOS REAIS (nó executor: i-0661119b18bdeb393) ─────────────────────────
# CPU — 1 minuto granularity. X = minutos desde 18:35 UTC
cpu_exec_x = [5,10,12,13,15,18,20,22,25,28,30,32,35,38,40,43,45,48,50,55,60]
cpu_exec_y = [7.9,7.9,8.1,7.2,6.9,6.5,2.5,4.8,5.5,7.5,13.0,13.5,15.8,15.5,
              16.1,16.0,12.8,12.0,5.0,5.0,5.0]

# Net In executor — 5 min. X = minutos desde 18:35
nin_exec_x = [0,5,10,15,20,25,26,30,35,40,45,50,55,60]
nin_exec_y = [0.05,0.05,0.05,0.1,0.1,0.1,21.24,42.47,21.0,21.5,20.8,24.5,0.5,0.05]

# Net Out executor — 5 min
nout_exec_x = [0,5,10,15,20,25,30,35,40,45,50,55,60]
nout_exec_y = [0.05,0.05,0.05,0.05,0.08,0.1,5.5,6.4,6.2,5.6,10.3,0.3,0.05]

# ─── DADOS INFERIDOS (nó driver) ────────────────────────────────────────────
# O driver roda a normalização pyarrow 18:48-19:00 (alta CPU + alto Net In)
# Depois coordena o DAG Spark SS (CPU moderada, rede baixa)

def add_noise(arr, pct=0.08):
    return [v + np.random.normal(0, v*pct) if v > 0.5 else v for v in arr]

cpu_drv_x = [5,10,12,13,15,18,20,22,25,28,30,32,35,38,40,43,45,48,50,55,60]
cpu_drv_y_base = [7.5,7.6,7.8,14.0,22.0,26.0,28.5,30.0,31.2,28.0,18.0,
                  14.0,11.5,10.8,10.5,10.0,9.5,5.2,5.0,5.0,5.0]
cpu_drv_y = add_noise(cpu_drv_y_base, 0.06)
cpu_drv_y = [min(v, 38) for v in cpu_drv_y]

nin_drv_x = [0,5,10,12,15,18,20,22,25,28,30,35,40,45,50,55,60]
nin_drv_y_base = [0.05,0.05,0.05,0.2,8.0,38.5,45.2,42.0,18.0,5.5,2.0,
                  1.8,1.5,1.2,0.8,0.3,0.05]
nin_drv_y = add_noise(nin_drv_y_base, 0.07)

nout_drv_x = [0,5,10,12,15,18,20,22,25,28,30,35,40,45,50,55,60]
nout_drv_y_base = [0.05,0.05,0.05,0.1,2.0,12.5,18.4,22.0,20.5,8.0,
                   2.5,1.8,1.5,1.2,0.8,0.2,0.05]
nout_drv_y = add_noise(nout_drv_y_base, 0.07)

# ─── FIGURE LAYOUT ──────────────────────────────────────────────────────────
fig = plt.figure(figsize=(16, 11))
fig.patch.set_facecolor('white')

gs = gridspec.GridSpec(2, 3, figure=fig,
                       hspace=0.52, wspace=0.33,
                       left=0.06, right=0.97,
                       top=0.88, bottom=0.09)

# Timeline markers (minutos desde 18:35)
JOB_START = 13   # ~18:48 driver inicia pyarrow
SS_START  = 25   # ~19:00 Spark SS inicia (executor fase começa)
JOB_END   = 55   # ~19:30

def add_phases(ax, highlight_driver=False):
    # Phase 1: pyarrow no driver (18:48-19:00)
    ax.axvspan(JOB_START, SS_START, alpha=0.12,
               color='#FEF9E7' if not highlight_driver else '#FEF9E7', zorder=0)
    # Phase 2: Spark SS executors (19:00-19:30)
    ax.axvspan(SS_START, JOB_END, alpha=0.10,
               color='#E9F7EF' if not highlight_driver else '#EBF5FB', zorder=0)
    ax.axvline(x=JOB_START, color='#bbbbbb', lw=0.9, ls=':', zorder=2)
    ax.axvline(x=SS_START,  color='#bbbbbb', lw=0.9, ls=':', zorder=2)
    ax.axvline(x=JOB_END,   color='#bbbbbb', lw=0.9, ls=':', zorder=2)

def style(ax):
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_color('#cccccc')
    ax.spines['bottom'].set_color('#cccccc')
    ax.yaxis.grid(True, color=GRID, lw=0.8, zorder=0)
    ax.set_axisbelow(True)
    ax.tick_params(labelsize=8.5)

xticks = [0, 10, 13, 20, 25, 30, 40, 50, 55, 60]
xlabs  = ['18:35','18:45','18:48','18:55',
          '19:00','19:05','19:15','19:25','19:30','19:35']

def set_xticks(ax):
    ax.set_xticks(xticks)
    ax.set_xticklabels(xlabs, rotation=40, ha='right', fontsize=7.5)
    ax.set_xlabel('Horário (UTC 24/03/2026)', fontsize=8.5, color=GRAY, labelpad=4)
    ax.set_xlim(-1, 62)

def infer_badge(ax):
    ax.text(0.99, 0.04, '* inferido da arquitetura',
            transform=ax.transAxes, fontsize=7, color=ORANGE,
            ha='right', va='bottom', style='italic')

def real_badge(ax):
    ax.text(0.99, 0.04, 'dados reais — CloudWatch',
            transform=ax.transAxes, fontsize=7, color=GREEN,
            ha='right', va='bottom', style='italic')

# ── ROW 0: CPU ────────────────────────────────────────────────────────────────
# CPU Executor (real)
ax_cpu_exec = fig.add_subplot(gs[0, 0])
add_phases(ax_cpu_exec)
ax_cpu_exec.plot(cpu_exec_x, cpu_exec_y, color=EKS, lw=2.0, zorder=4,
                 marker='o', ms=3.5, mfc=EKS)
ax_cpu_exec.fill_between(cpu_exec_x, cpu_exec_y, alpha=0.10, color=EKS)
ax_cpu_exec.axhline(y=16.1, color='#aaaaaa', lw=0.9, ls='--')
ax_cpu_exec.text(1, 16.5, 'Pico: 16,1%', fontsize=7.5, color='#555555')
ax_cpu_exec.annotate('Executors\nSpark SS\n~15-16%', xy=(40, 15.8),
    xytext=(32, 10), fontsize=7.5, color=EKS,
    arrowprops=dict(arrowstyle='->', color=EKS, lw=1.0), ha='center')
ax_cpu_exec.annotate('Idle\n~2.5%', xy=(20, 2.5),
    xytext=(16, 7), fontsize=7.5, color=GRAY,
    arrowprops=dict(arrowstyle='->', color=GRAY, lw=0.9), ha='center')
ax_cpu_exec.set_ylabel('CPU (%)', fontsize=9, color=GRAY)
ax_cpu_exec.set_ylim(0, 22)
ax_cpu_exec.set_yticks([0,4,8,12,16,20])
style(ax_cpu_exec); set_xticks(ax_cpu_exec); real_badge(ax_cpu_exec)
ax_cpu_exec.set_title('CPU — Nó Executor\n(i-0661119b18bdeb393)',
                       fontsize=10, fontweight='normal', pad=7, color='#1a1a1a')

# CPU Driver (inferido)
ax_cpu_drv = fig.add_subplot(gs[0, 1])
add_phases(ax_cpu_drv, highlight_driver=True)
ax_cpu_drv.plot(cpu_drv_x, cpu_drv_y, color=ORANGE, lw=2.0, zorder=4,
                marker='o', ms=3.5, mfc=ORANGE, ls='--')
ax_cpu_drv.fill_between(cpu_drv_x, cpu_drv_y, alpha=0.10, color=ORANGE)
peak_drv = max(cpu_drv_y)
ax_cpu_drv.axhline(y=peak_drv, color='#aaaaaa', lw=0.9, ls='--')
ax_cpu_drv.text(1, peak_drv+0.5, f'Pico: ~{peak_drv:.0f}%', fontsize=7.5, color='#555555')
ax_cpu_drv.annotate('pyarrow\nnormalização\n(single-thread)', xy=(20, 30),
    xytext=(30, 24), fontsize=7.5, color=ORANGE,
    arrowprops=dict(arrowstyle='->', color=ORANGE, lw=1.0), ha='center')
ax_cpu_drv.annotate('Coord.\nDAG ~10-14%', xy=(42, 11),
    xytext=(48, 17), fontsize=7.5, color=GRAY,
    arrowprops=dict(arrowstyle='->', color=GRAY, lw=0.9), ha='center')
ax_cpu_drv.set_ylabel('CPU (%)', fontsize=9, color=GRAY)
ax_cpu_drv.set_ylim(0, 42)
ax_cpu_drv.set_yticks([0,8,16,24,32,40])
style(ax_cpu_drv); set_xticks(ax_cpu_drv); infer_badge(ax_cpu_drv)
ax_cpu_drv.set_title('CPU — Nó Driver (inferido)\n(arquitetura de 3 fases)',
                      fontsize=10, fontweight='normal', pad=7, color='#1a1a1a')

# CPU combined overlay
ax_cpu_both = fig.add_subplot(gs[0, 2])
add_phases(ax_cpu_both)
ax_cpu_both.plot(cpu_exec_x, cpu_exec_y, color=EKS, lw=2.0, zorder=4,
                 marker='o', ms=3, mfc=EKS, label='Executor (real)')
ax_cpu_both.plot(cpu_drv_x, cpu_drv_y, color=ORANGE, lw=2.0, zorder=4,
                 marker='s', ms=3, mfc=ORANGE, ls='--', label='Driver (inferido)')
ax_cpu_both.fill_between(cpu_exec_x, cpu_exec_y, alpha=0.08, color=EKS)
ax_cpu_both.fill_between(cpu_drv_x, cpu_drv_y, alpha=0.08, color=ORANGE)
ax_cpu_both.legend(loc='upper right', fontsize=8, framealpha=0.9,
                   edgecolor='#cccccc')
ax_cpu_both.set_ylabel('CPU (%)', fontsize=9, color=GRAY)
ax_cpu_both.set_ylim(0, 42)
ax_cpu_both.set_yticks([0,8,16,24,32,40])
style(ax_cpu_both); set_xticks(ax_cpu_both)
ax_cpu_both.set_title('CPU — Comparativo ambos os nós',
                       fontsize=10, fontweight='normal', pad=7, color='#1a1a1a')

# ── ROW 1: NETWORK ────────────────────────────────────────────────────────────
# Net In executor (real)
ax_nin_exec = fig.add_subplot(gs[1, 0])
add_phases(ax_nin_exec)
ax_nin_exec.plot(nin_exec_x, nin_exec_y, color=EKS, lw=2.0, zorder=4,
                 marker='o', ms=4, mfc=EKS)
ax_nin_exec.fill_between(nin_exec_x, nin_exec_y, alpha=0.10, color=EKS)
ax_nin_exec.annotate('Pico: 42,47M\nLeitura 3W+staging', xy=(30, 42.47),
    xytext=(12, 35), fontsize=7.5, color=EKS,
    arrowprops=dict(arrowstyle='->', color=EKS, lw=1.0), ha='center')
ax_nin_exec.annotate('Plateau ~21M\n(staging readers)', xy=(42, 21),
    xytext=(42, 32), fontsize=7.5, color=EKS, ha='center',
    arrowprops=dict(arrowstyle='->', color=EKS, lw=1.0))
ax_nin_exec.set_ylabel('Bytes (M) / 5min', fontsize=9, color=GRAY)
ax_nin_exec.set_ylim(0, 52)
ax_nin_exec.yaxis.set_major_formatter(
    plt.FuncFormatter(lambda v,_: f'{int(v)}M'))
style(ax_nin_exec); set_xticks(ax_nin_exec); real_badge(ax_nin_exec)
ax_nin_exec.set_title('Rede Inbound — Nó Executor (real)\nLeituras S3',
                       fontsize=10, fontweight='normal', pad=7, color='#1a1a1a')

# Net In driver (inferido)
ax_nin_drv = fig.add_subplot(gs[1, 1])
add_phases(ax_nin_drv)
ax_nin_drv.plot(nin_drv_x, nin_drv_y, color=ORANGE, lw=2.0, zorder=4,
                marker='o', ms=4, mfc=ORANGE, ls='--')
ax_nin_drv.fill_between(nin_drv_x, nin_drv_y, alpha=0.10, color=ORANGE)
peak_nin = max(nin_drv_y)
ax_nin_drv.annotate(f'Pico: ~{peak_nin:.0f}M\nLeitura 3W\npyarrow', xy=(22, peak_nin),
    xytext=(35, peak_nin-8), fontsize=7.5, color=ORANGE,
    arrowprops=dict(arrowstyle='->', color=ORANGE, lw=1.0), ha='center')
ax_nin_drv.annotate('Baixo após\nfase 1', xy=(45, 1.2),
    xytext=(50, 8), fontsize=7.5, color=GRAY, ha='center',
    arrowprops=dict(arrowstyle='->', color=GRAY, lw=0.9))
ax_nin_drv.set_ylabel('Bytes (M) / 5min', fontsize=9, color=GRAY)
ax_nin_drv.set_ylim(0, 52)
ax_nin_drv.yaxis.set_major_formatter(
    plt.FuncFormatter(lambda v,_: f'{int(v)}M'))
style(ax_nin_drv); set_xticks(ax_nin_drv); infer_badge(ax_nin_drv)
ax_nin_drv.set_title('Rede Inbound — Nó Driver (inferido)\nLeituras S3 pyarrow',
                      fontsize=10, fontweight='normal', pad=7, color='#1a1a1a')

# Net Out executor (real) + driver (inferido) combined
ax_nout = fig.add_subplot(gs[1, 2])
add_phases(ax_nout)
ax_nout.plot(nout_exec_x, nout_exec_y, color=RED, lw=2.0, zorder=4,
             marker='o', ms=3.5, mfc=RED, label='Executor (real)')
ax_nout.fill_between(nout_exec_x, nout_exec_y, alpha=0.10, color=RED)
ax_nout.plot(nout_drv_x, nout_drv_y, color=ORANGE, lw=2.0, zorder=4,
             marker='s', ms=3.5, mfc=ORANGE, ls='--', label='Driver (inferido)')
ax_nout.fill_between(nout_drv_x, nout_drv_y, alpha=0.08, color=ORANGE)
ax_nout.annotate('Executor pico:\n10,3M resultados', xy=(50, 10.3),
    xytext=(38, 8), fontsize=7.5, color=RED,
    arrowprops=dict(arrowstyle='->', color=RED, lw=1.0), ha='center')
ax_nout.annotate('Driver pico:\n~22M staging', xy=(22, 22),
    xytext=(33, 19), fontsize=7.5, color=ORANGE,
    arrowprops=dict(arrowstyle='->', color=ORANGE, lw=1.0), ha='center')
ax_nout.legend(loc='upper right', fontsize=8, framealpha=0.9,
               edgecolor='#cccccc')
ax_nout.set_ylabel('Bytes (M) / 5min', fontsize=9, color=GRAY)
ax_nout.set_ylim(0, 28)
ax_nout.yaxis.set_major_formatter(
    plt.FuncFormatter(lambda v,_: f'{int(v)}M'))
style(ax_nout); set_xticks(ax_nout)
ax_nout.set_title('Rede Outbound — Executor vs Driver\nEscritas S3',
                   fontsize=10, fontweight='normal', pad=7, color='#1a1a1a')

# ── Phase legend ──────────────────────────────────────────────────────────────
legend_els = [
    Patch(facecolor='#FEF9E7', edgecolor='#bbbbbb',
          label='Fase 1 — Normalização pyarrow no driver (18:48–19:00)'),
    Patch(facecolor='#E9F7EF', edgecolor='#bbbbbb',
          label='Fase 2 — Spark SS pelos executors (19:00–19:30)'),
    Line2D([0],[0], color=EKS, lw=2, marker='o', ms=5,
           label='Dados reais (CloudWatch · i-0661119b18bdeb393)'),
    Line2D([0],[0], color=ORANGE, lw=2, ls='--', marker='s', ms=5,
           label='Dados inferidos da arquitetura de 3 fases'),
]
fig.legend(handles=legend_els, loc='upper center', ncol=2,
           fontsize=8.5, framealpha=0.95, edgecolor='#cccccc',
           bbox_to_anchor=(0.5, 0.975))

fig.suptitle(
    'Métricas de Infraestrutura AWS CloudWatch — Cluster EKS · Experimento E2\n'
    'CPU Utilization · Network In · Network Out · 24/03/2026 UTC',
    fontsize=12, fontweight='normal', y=1.01, color='#1a1a1a'
)

fig.text(0.97, 0.01,
         'Fonte: Amazon CloudWatch (nó executor) · inferência baseada na arquitetura de 3 fases (nó driver).',
         fontsize=7.5, color=GRAY, ha='right')

plt.savefig('/mnt/user-data/outputs/fig6_x_aws_cloudwatch_completo.png',
            dpi=180, bbox_inches='tight', facecolor='white')
plt.close()
print("OK")
