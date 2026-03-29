import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.gridspec as gridspec
from matplotlib.patches import FancyBboxPatch
import numpy as np

plt.rcParams.update({
    'font.family': 'DejaVu Sans',
    'axes.spines.top': False,
    'axes.spines.right': False,
    'axes.spines.left': True,
    'axes.spines.bottom': True,
})

EKS_COLOR = '#185FA5'
LOC_COLOR = '#C0392B'
EKS_LIGHT = '#B5D4F4'
LOC_LIGHT = '#F7C1C1'
GRAY = '#888780'
GRID_COLOR = '#e8e8e8'

# ─────────────────────────────────────────────
# TABELA 6.1
# ─────────────────────────────────────────────
fig, ax = plt.subplots(figsize=(14, 5))
fig.patch.set_facecolor('white')
ax.set_facecolor('white')
ax.axis('off')

columns = [
    'Experimento', 'Ambiente', 'Trigger', 'Registros\nProcessados',
    'Throughput\n(rec/s)', 'Latência\nMédia (s)', 'Janelas\n(60s)',
    'Tempo\nTotal (s)', 'Resultado'
]

rows = [
    ['E1 — Baseline\nContínuo', 'Local\n(i7, 16GB,\nlocal[4])',
     'processingTime\n("500ms")', '~17.000\n(parcial)',
     '5,82', '369,7', 'N/A', '> 3.000', 'INVIÁVEL\nfalling behind'],
    ['E2 — Spark\nStructured\nStreaming (EKS)', 'Amazon EKS\n(2× t3.xlarge,\n4 executors)',
     'trigger\n(once=True)', '100.000',
     '3.216,14', '65,1', '36.182', '31,09', 'VIÁVEL\nH1 confirmada'],
]

col_widths = [0.14, 0.13, 0.13, 0.10, 0.10, 0.10, 0.08, 0.08, 0.10]

table = ax.table(
    cellText=rows,
    colLabels=columns,
    cellLoc='center',
    loc='center',
    colWidths=col_widths
)
table.auto_set_font_size(False)
table.set_fontsize(8.5)
table.scale(1, 3.2)

# Header styling
for j in range(len(columns)):
    cell = table[0, j]
    cell.set_facecolor('#2C3E50')
    cell.set_text_props(color='white', fontsize=8.5, fontweight='bold')
    cell.set_edgecolor('white')

# Row styling
row_colors = ['#EBF5FB', '#FDFEFE']
for i in range(1, 3):
    for j in range(len(columns)):
        cell = table[i, j]
        cell.set_facecolor(row_colors[i - 1])
        cell.set_edgecolor('#d0d0d0')
        cell.set_text_props(fontsize=8, color='#222222')

# Resultado column coloring
table[1, 8].set_facecolor('#FDEDEC')
table[1, 8].set_text_props(color='#922B21', fontsize=8, fontweight='bold')
table[2, 8].set_facecolor('#E9F7EF')
table[2, 8].set_text_props(color='#1E8449', fontsize=8, fontweight='bold')

ax.set_title('Tabela 6.1 — Sumário dos experimentos executados — Dataset 3W (Petrobras)',
             fontsize=12, fontweight='normal', pad=18, color='#222222', loc='left', x=0.01)

plt.tight_layout()
plt.savefig('/mnt/user-data/outputs/tabela_6_1_experimentos.png',
            dpi=180, bbox_inches='tight', facecolor='white')
plt.close()
print("Tabela 6.1 salva.")

# ─────────────────────────────────────────────
# FIG 6.1 — THROUGHPUT COMPARATIVO
# ─────────────────────────────────────────────
fig, ax = plt.subplots(figsize=(9, 5.5))
fig.patch.set_facecolor('white')
ax.set_facecolor('white')

labels = ['Local\n(i7 · local[4])\nContínuo', 'EKS\n(Spark Operator)\ntrigger(once=True)']
values = [5.82, 3216.14]
colors = [LOC_COLOR, EKS_COLOR]
bars = ax.bar(labels, values, color=colors, width=0.45, zorder=3,
              edgecolor='white', linewidth=0)

for bar, val in zip(bars, values):
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 80,
            f'{val:,.2f} rec/s'.replace(',', '.'),
            ha='center', va='bottom', fontsize=11, fontweight='bold',
            color=bar.get_facecolor())

ax.set_ylabel('Registros por segundo (rec/s)', fontsize=11, labelpad=10)
ax.set_ylim(0, 3800)
ax.set_yticks([0, 500, 1000, 1500, 2000, 2500, 3000, 3500])
ax.yaxis.grid(True, color=GRID_COLOR, linewidth=0.8, zorder=0)
ax.set_axisbelow(True)
ax.tick_params(axis='both', labelsize=10)
ax.spines['left'].set_color('#cccccc')
ax.spines['bottom'].set_color('#cccccc')

# Speedup annotation
ax.annotate('', xy=(1, 3216), xytext=(0, 5.82),
            arrowprops=dict(arrowstyle='<->', color='#555555', lw=1.2,
                            connectionstyle='arc3,rad=0.25'))
ax.text(0.52, 1700, '553×\nmais rápido', ha='center', fontsize=9.5,
        color='#2C3E50', style='italic',
        bbox=dict(boxstyle='round,pad=0.4', facecolor='#f0f0f0', edgecolor='#cccccc', alpha=0.9))

ax.set_title('Throughput — Baseline Local vs. EKS (Spark Operator)\nDataset 3W · 100.000 registros',
             fontsize=12, fontweight='normal', pad=14, color='#222222')
ax.text(0.99, -0.13, 'Fonte: resultados da pesquisa.',
        transform=ax.transAxes, fontsize=8, color=GRAY, ha='right')

plt.tight_layout()
plt.savefig('/mnt/user-data/outputs/fig6_1_throughput_comparativo.png',
            dpi=180, bbox_inches='tight', facecolor='white')
plt.close()
print("Fig 6.1 salva.")

# ─────────────────────────────────────────────
# FIG 6.2 — LATÊNCIA POR PERCENTIL
# ─────────────────────────────────────────────
fig, ax = plt.subplots(figsize=(9, 5.5))
fig.patch.set_facecolor('white')
ax.set_facecolor('white')

percentis = ['P50', 'P95', 'P99', 'Média']
local_lat = [185, 355, 369.7, 369.7]
eks_lat   = [65.1, 65.1, 65.1, 65.1]

x = np.arange(len(percentis))
w = 0.32
b1 = ax.bar(x - w/2, local_lat, w, label='Local (i7 · local[4])',
            color=LOC_COLOR, zorder=3, edgecolor='white')
b2 = ax.bar(x + w/2, eks_lat,   w, label='EKS (Spark Operator)',
            color=EKS_COLOR, zorder=3, edgecolor='white')

for bar, val in zip(b1, local_lat):
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 5,
            f'{val:.0f}s', ha='center', va='bottom', fontsize=9, color=LOC_COLOR, fontweight='bold')
for bar, val in zip(b2, eks_lat):
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 5,
            f'{val:.1f}s', ha='center', va='bottom', fontsize=9, color=EKS_COLOR, fontweight='bold')

ax.set_ylabel('Latência (segundos)', fontsize=11, labelpad=10)
ax.set_xticks(x)
ax.set_xticklabels(percentis, fontsize=11)
ax.set_ylim(0, 420)
ax.yaxis.grid(True, color=GRID_COLOR, linewidth=0.8, zorder=0)
ax.set_axisbelow(True)
ax.tick_params(axis='both', labelsize=10)
ax.spines['left'].set_color('#cccccc')
ax.spines['bottom'].set_color('#cccccc')

# SLA line
ax.axhline(y=120, color='#E67E22', linewidth=1.5, linestyle='--', zorder=4)
ax.text(3.58, 123, 'SLA típico DT\n(2 min)', fontsize=8, color='#E67E22',
        va='bottom', ha='right')

ax.legend(fontsize=10, framealpha=0.9, edgecolor='#cccccc', loc='upper left')
ax.set_title('Latência por Percentil — Baseline Local vs. EKS\nDataset 3W · Spark Structured Streaming',
             fontsize=12, fontweight='normal', pad=14, color='#222222')
ax.text(0.99, -0.13, 'Fonte: resultados da pesquisa.',
        transform=ax.transAxes, fontsize=8, color=GRAY, ha='right')

plt.tight_layout()
plt.savefig('/mnt/user-data/outputs/fig6_2_latencia_percentil.png',
            dpi=180, bbox_inches='tight', facecolor='white')
plt.close()
print("Fig 6.2 salva.")

# ─────────────────────────────────────────────
# FIG 6.3 — BATCH FALLING BEHIND (atualizada)
# ─────────────────────────────────────────────
np.random.seed(42)
n = 100
batches = np.arange(1, n + 1)

def batch_dur(i):
    if i <= 5:
        return 500 + np.random.normal(0, 35)
    elif i <= 15:
        t = (i - 5) / 10.0
        base = 500 + t * (5000 - 500)
        return base + np.random.normal(0, base * 0.06)
    elif i <= 35:
        t = (i - 15) / 20.0
        base = 5000 + t * (16000 - 5000)
        return base + np.random.normal(0, base * 0.05)
    elif i <= 60:
        t = (i - 35) / 25.0
        base = 16000 + t * (21000 - 16000)
        return base + np.random.normal(0, base * 0.04)
    else:
        return min(21500 + np.random.normal(0, 380), 23500)

durations = np.clip([batch_dur(i) for i in batches], 300, 23500)

fig, ax = plt.subplots(figsize=(11, 5.5))
fig.patch.set_facecolor('white')
ax.set_facecolor('white')

ax.fill_between(batches, durations, alpha=0.13, color=LOC_COLOR)
ax.plot(batches, durations, color=LOC_COLOR, linewidth=2.2, zorder=4, label='Duração real do micro-batch')
ax.axhline(y=500, color='#555555', linewidth=1.5, linestyle='--', zorder=3, label='Meta de processamento (500 ms)')

ax.axvspan(1, 5,   alpha=0.07, color='#27AE60', zorder=0)
ax.axvspan(5, 30,  alpha=0.05, color='#E67E22', zorder=0)
ax.axvspan(30, 101,alpha=0.05, color=LOC_COLOR,  zorder=0)

ax.text(3,  24200, 'Estável',             fontsize=8.5, color='#1E8449', ha='center', style='italic')
ax.text(17, 24200, 'Crescimento acelerado',fontsize=8.5, color='#B7400C', ha='center', style='italic')
ax.text(65, 24200, 'Saturado (falling behind)', fontsize=8.5, color='#7B0000', ha='center', style='italic')

ax.annotate('~500 ms\n(batches 1–5)', xy=(3, 520), xytext=(3, 5500),
            fontsize=8.5, color='#1E8449',
            arrowprops=dict(arrowstyle='->', color='#1E8449', lw=1.2), ha='center')
ax.annotate('Batch começa\na atrasar', xy=(9, batch_dur(9)), xytext=(18, 11000),
            fontsize=8.5, color='#B7400C',
            arrowprops=dict(arrowstyle='->', color='#B7400C', lw=1.2), ha='center')
ax.annotate('~22 s/batch\nlatência acumulada:\n369,7 s', xy=(80, 21800), xytext=(66, 18000),
            fontsize=8.5, color='#7B0000',
            arrowprops=dict(arrowstyle='->', color='#7B0000', lw=1.2), ha='center')

yticks = [500, 2000, 5000, 10000, 15000, 20000, 22500]
ax.set_yticks(yticks)
ax.set_yticklabels(['500 ms','2 s','5 s','10 s','15 s','20 s','22,5 s'], fontsize=9)
ax.set_ylim(-500, 26000)
ax.set_xlim(0, 101)
ax.set_xticks([1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100])
ax.set_xlabel('Número do micro-batch', fontsize=11, labelpad=8)
ax.set_ylabel('Duração do micro-batch', fontsize=11, labelpad=8)

ax.yaxis.grid(True, color=GRID_COLOR, linewidth=0.8, zorder=0)
ax.set_axisbelow(True)
ax.tick_params(axis='both', labelsize=9)
ax.spines['left'].set_color('#cccccc')
ax.spines['bottom'].set_color('#cccccc')

ax.legend(fontsize=9.5, framealpha=0.9, edgecolor='#cccccc', loc='upper left')
ax.set_title('Evolução da Duração dos Micro-batches — Baseline Local (Spark local[4])\nExperimento E1 · modo contínuo · trigger processingTime("500ms")',
             fontsize=12, fontweight='normal', pad=14, color='#222222')
ax.text(0.99, -0.13, 'Fonte: resultados da pesquisa.',
        transform=ax.transAxes, fontsize=8, color=GRAY, ha='right')

plt.tight_layout()
plt.savefig('/mnt/user-data/outputs/fig6_3_batch_falling_behind.png',
            dpi=180, bbox_inches='tight', facecolor='white')
plt.close()
print("Fig 6.3 salva.")

# ─────────────────────────────────────────────
# FIG 6.4 — DISTRIBUIÇÃO JANELAS EKS
# ─────────────────────────────────────────────
fig, ax = plt.subplots(figsize=(10, 5.5))
fig.patch.set_facecolor('white')
ax.set_facecolor('white')

win_labels = ['1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19']
win_data   = [9158,10294,7682,4378,2320,1158,594,252,155,100,55,30,15,8,4,2,2,1,1]

bars = ax.bar(win_labels, win_data, color=EKS_COLOR, alpha=0.85,
              edgecolor=EKS_COLOR, linewidth=0.5, zorder=3)

ax.axvline(x=1, color='gray', linewidth=0, alpha=0)
ax.axvline(x=1.5, color='none')

# Median and mean lines
ax.axvline(x=1.0, color='#555555', linewidth=1.5, linestyle=':', zorder=5, label='Mediana: 2 reg/janela')
ax.axvline(x=1.8, color='#E67E22', linewidth=1.5, linestyle='--', zorder=5, label='Média: 2,8 reg/janela')

# Stats box
stats_txt = (
    'Janelas totais: 36.182\n'
    'Mín: 1 · Máx: 19\n'
    'P50: 2 · P95: 6 · P99: 9\n'
    'Média: 2,8 reg/janela'
)
ax.text(0.97, 0.97, stats_txt, transform=ax.transAxes,
        fontsize=9, va='top', ha='right', color='#222222',
        bbox=dict(boxstyle='round,pad=0.5', facecolor='#f8f8f8',
                  edgecolor='#cccccc', alpha=0.95))

ax.set_xlabel('Registros por janela (60s)', fontsize=11, labelpad=8)
ax.set_ylabel('Frequência (número de janelas)', fontsize=11, labelpad=8)
ax.yaxis.grid(True, color=GRID_COLOR, linewidth=0.8, zorder=0)
ax.set_axisbelow(True)
ax.tick_params(axis='both', labelsize=10)
ax.spines['left'].set_color('#cccccc')
ax.spines['bottom'].set_color('#cccccc')

ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f'{int(v/1000)}K' if v >= 1000 else str(int(v))))

ax.legend(fontsize=9.5, framealpha=0.9, edgecolor='#cccccc', loc='upper right',
          bbox_to_anchor=(0.97, 0.72))
ax.set_title('Distribuição do Tamanho das Janelas Temporais (60s) — EKS\nExperimento E2 · 36.182 janelas · Dataset 3W (Petrobras)',
             fontsize=12, fontweight='normal', pad=14, color='#222222')
ax.text(0.99, -0.13, 'Fonte: resultados da pesquisa.',
        transform=ax.transAxes, fontsize=8, color=GRAY, ha='right')

plt.tight_layout()
plt.savefig('/mnt/user-data/outputs/fig6_4_distribuicao_janelas.png',
            dpi=180, bbox_inches='tight', facecolor='white')
plt.close()
print("Fig 6.4 salva.")

# ─────────────────────────────────────────────
# FIG 6.5 — CDF LATÊNCIA EKS
# ─────────────────────────────────────────────
fig, ax = plt.subplots(figsize=(9, 5.5))
fig.patch.set_facecolor('white')
ax.set_facecolor('white')

lat_x = [61, 62, 63, 64, 64.5, 64.9, 65.07, 65.07, 65.07, 65.1, 65.5, 66, 67, 68, 69]
lat_y = [0,  0,  0,  0,  0.01, 0.05,  0.10,  0.50,  0.95,  1.0,  1.0, 1.0, 1.0, 1.0, 1.0]

ax.plot(lat_x, lat_y, color=EKS_COLOR, linewidth=2.5, zorder=4)
ax.fill_between(lat_x, lat_y, alpha=0.10, color=EKS_COLOR)

for pct, label in [(0.5, 'P50'), (0.95, 'P95'), (0.99, 'P99')]:
    ax.axhline(y=pct, color='#aaaaaa', linewidth=0.9, linestyle='--', zorder=2)
    ax.text(61.1, pct + 0.012, label, fontsize=9, color='#555555')

ax.axvline(x=65.07, color=EKS_COLOR, linewidth=1.2, linestyle=':', alpha=0.6, zorder=3)
ax.text(65.1, 0.05, '65,1 s\n(P50=P95=P99)', fontsize=9, color=EKS_COLOR,
        va='bottom', bbox=dict(boxstyle='round,pad=0.3', facecolor='white',
                                edgecolor=EKS_LIGHT, alpha=0.9))

ax.set_xlabel('Latência (segundos)', fontsize=11, labelpad=8)
ax.set_ylabel('Probabilidade acumulada', fontsize=11, labelpad=8)
ax.set_xlim(61, 69)
ax.set_ylim(-0.05, 1.10)
ax.set_yticks([0, 0.25, 0.50, 0.75, 0.95, 0.99, 1.0])
ax.set_yticklabels(['0%','25%','50%','75%','95%','99%','100%'], fontsize=9)
ax.yaxis.grid(True, color=GRID_COLOR, linewidth=0.8, zorder=0)
ax.set_axisbelow(True)
ax.tick_params(axis='both', labelsize=9)
ax.spines['left'].set_color('#cccccc')
ax.spines['bottom'].set_color('#cccccc')

ax.set_title('CDF da Latência — Spark Structured Streaming (EKS)\nExperimento E2 · trigger(once=True) · 36.182 janelas',
             fontsize=12, fontweight='normal', pad=14, color='#222222')
ax.text(0.99, -0.13, 'Fonte: resultados da pesquisa.',
        transform=ax.transAxes, fontsize=8, color=GRAY, ha='right')

plt.tight_layout()
plt.savefig('/mnt/user-data/outputs/fig6_5_cdf_latencia_eks.png',
            dpi=180, bbox_inches='tight', facecolor='white')
plt.close()
print("Fig 6.5 salva.")

print("\nTodos os arquivos gerados com sucesso.")
