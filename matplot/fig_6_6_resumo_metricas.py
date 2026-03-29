import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
import numpy as np

EKS   = '#185FA5'
LOC   = '#C0392B'
GREEN = '#1E8449'
AMBER = '#B7400C'
GRAY  = '#888780'
GRID  = '#eeeeee'
BG    = 'white'

fig = plt.figure(figsize=(16, 11))
fig.patch.set_facecolor(BG)

fig.suptitle(
    'Resumo das Métricas Coletadas — Experimentos E1 e E2\n'
    'Spark Structured Streaming · Dataset 3W (Petrobras) · Capítulo 6',
    fontsize=13, fontweight='normal', y=0.98, color='#1a1a1a'
)

gs = gridspec.GridSpec(
    3, 4,
    figure=fig,
    hspace=0.55, wspace=0.38,
    left=0.06, right=0.97,
    top=0.91, bottom=0.06
)

# ── helpers ──────────────────────────────────────────────────────────────────
def card(ax, title, val, unit, subtitle, color, bg):
    ax.set_xlim(0, 1); ax.set_ylim(0, 1); ax.axis('off')
    ax.add_patch(FancyBboxPatch((0,0),1,1, boxstyle='round,pad=0.04',
                                facecolor=bg, edgecolor=color, linewidth=1.2))
    ax.text(0.5, 0.80, title,    ha='center', va='center', fontsize=8.5,
            color=GRAY, transform=ax.transAxes)
    ax.text(0.5, 0.48, val,      ha='center', va='center', fontsize=22,
            color=color, fontweight='bold', transform=ax.transAxes)
    ax.text(0.5, 0.30, unit,     ha='center', va='center', fontsize=10,
            color=color, transform=ax.transAxes)
    ax.text(0.5, 0.10, subtitle, ha='center', va='center', fontsize=7.5,
            color=GRAY, transform=ax.transAxes, style='italic')

def mini_label(ax, txt, color='#333333'):
    ax.text(0.01, 1.04, txt, transform=ax.transAxes,
            fontsize=9, fontweight='bold', color=color, va='bottom')

# ── ROW 0 — metric cards ─────────────────────────────────────────────────────
cards = [
    ('Throughput — EKS',     '3.216',  'rec/s',   'trigger(once=True) · 4 executors', EKS,  '#EBF5FB'),
    ('Throughput — Local',   '5,82',   'rec/s',   'modo contínuo · falling behind',   LOC,  '#FDEDEC'),
    ('Speedup de Throughput','553×',   'EKS / Local', 'resultado do comparativo',      GREEN,'#E9F7EF'),
    ('Janelas Processadas',  '36.182', 'janelas', '60s · dataset 3W completo',        EKS,  '#EBF5FB'),
]
for j, (title, val, unit, sub, col, bg) in enumerate(cards):
    ax = fig.add_subplot(gs[0, j])
    card(ax, title, val, unit, sub, col, bg)

cards2 = [
    ('Latência Média — EKS',   '65,1',  's',  'P50 = P95 = P99 · batch único',   EKS,  '#EBF5FB'),
    ('Latência Média — Local', '369,7', 's',  '~6 min/evento · inviável DT',      LOC,  '#FDEDEC'),
    ('Redução de Latência',    '5,7×',  'EKS < Local', 'resultado do comparativo', GREEN,'#E9F7EF'),
    ('Tempo Total — EKS',      '31,09', 's',  '100K registros · sem falling behind', EKS,'#EBF5FB'),
]
for j, (title, val, unit, sub, col, bg) in enumerate(cards2):
    ax = fig.add_subplot(gs[1, j])
    card(ax, title, val, unit, sub, col, bg)

# ── ROW 2 col 0-1 — throughput bar ───────────────────────────────────────────
ax_tp = fig.add_subplot(gs[2, 0:2])
ax_tp.set_facecolor(BG)
mini_label(ax_tp, 'Throughput comparativo (escala log)', '#1a1a1a')

bars = ax_tp.bar(['Local\n(i7 · local[4])', 'EKS\n(Spark Operator)'],
                 [5.82, 3216.14],
                 color=[LOC, EKS], width=0.42, zorder=3,
                 edgecolor='white')
ax_tp.set_yscale('log')
ax_tp.set_ylabel('rec/s (log)', fontsize=9, color=GRAY)
ax_tp.yaxis.grid(True, color=GRID, linewidth=0.8, zorder=0)
ax_tp.set_axisbelow(True)
ax_tp.tick_params(labelsize=9)
ax_tp.spines['top'].set_visible(False)
ax_tp.spines['right'].set_visible(False)
ax_tp.spines['left'].set_color('#cccccc')
ax_tp.spines['bottom'].set_color('#cccccc')
ax_tp.yaxis.set_major_formatter(
    plt.FuncFormatter(lambda v, _: f'{int(v/1000)}K' if v >= 1000 else f'{v:.0f}' if v >= 1 else f'{v:.2f}'))
for bar, val, lbl in zip(bars, [5.82, 3216.14], ['5,82 rec/s', '3.216 rec/s']):
    ax_tp.text(bar.get_x() + bar.get_width()/2,
               bar.get_height() * 1.6, lbl,
               ha='center', va='bottom', fontsize=9,
               fontweight='bold', color=bar.get_facecolor())

# ── ROW 2 col 2-3 — window distribution ─────────────────────────────────────
ax_win = fig.add_subplot(gs[2, 2:4])
ax_win.set_facecolor(BG)
mini_label(ax_win, 'Distribuição das janelas temporais (60s) — EKS', EKS)

wlabels = ['1','2','3','4','5','6','7','8','9','10','11+']
wdata   = [9158,10294,7682,4378,2320,1158,594,252,155,100,91]
ax_win.bar(wlabels, wdata, color=EKS, alpha=0.82,
           edgecolor=EKS, linewidth=0.4, zorder=3)
ax_win.set_xlabel('Registros por janela', fontsize=9, color=GRAY, labelpad=4)
ax_win.set_ylabel('Frequência', fontsize=9, color=GRAY, labelpad=4)
ax_win.yaxis.grid(True, color=GRID, linewidth=0.8, zorder=0)
ax_win.set_axisbelow(True)
ax_win.tick_params(labelsize=9)
ax_win.spines['top'].set_visible(False)
ax_win.spines['right'].set_visible(False)
ax_win.spines['left'].set_color('#cccccc')
ax_win.spines['bottom'].set_color('#cccccc')
ax_win.yaxis.set_major_formatter(
    plt.FuncFormatter(lambda v, _: f'{int(v/1000)}K' if v >= 1000 else str(int(v))))

# stats box
stats = 'Min:1  Max:19  P50:2\nP95:6  P99:9  Média:2,8'
ax_win.text(0.97, 0.97, stats, transform=ax_win.transAxes,
            fontsize=8, va='top', ha='right', color='#222222',
            bbox=dict(boxstyle='round,pad=0.4', facecolor='#f4f8fd',
                      edgecolor='#aacce8', alpha=0.95))

# ── separator lines ───────────────────────────────────────────────────────────
for y_pos in [0.645, 0.345]:
    line = plt.Line2D([0.03, 0.97], [y_pos, y_pos],
                      transform=fig.transFigure,
                      color='#dddddd', linewidth=0.8, linestyle='--')
    fig.add_artist(line)

# ── section labels ────────────────────────────────────────────────────────────
fig.text(0.03, 0.935, 'DESEMPENHO — THROUGHPUT E LATÊNCIA',
         fontsize=7.5, color=GRAY, style='italic', va='bottom')
fig.text(0.03, 0.635, 'DETALHES DO PIPELINE — JANELAS E TEMPO',
         fontsize=7.5, color=GRAY, style='italic', va='bottom')
fig.text(0.03, 0.335, 'COMPARATIVO VISUAL',
         fontsize=7.5, color=GRAY, style='italic', va='bottom')

# ── footer ────────────────────────────────────────────────────────────────────
fig.text(0.97, 0.015, 'Fonte: resultados da pesquisa.',
         fontsize=8, color=GRAY, ha='right')

# legend strip
fig.text(0.06, 0.015,
         '■ Experimento E1 — Baseline Local (i7 · local[4] · modo contínuo)',
         fontsize=8.5, color=LOC)
fig.text(0.50, 0.015,
         '■ Experimento E2 — EKS (Spark Operator · trigger(once=True))',
         fontsize=8.5, color=EKS)

plt.savefig('/mnt/user-data/outputs/fig6_6_resumo_metricas.png',
            dpi=180, bbox_inches='tight', facecolor=BG)
plt.close()
print("Fig 6.6 salva.")
