import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from matplotlib.patches import FancyBboxPatch
import numpy as np

EKS   = '#185FA5'
LOC   = '#C0392B'
GREEN = '#1E8449'
AMBER = '#B7400C'
PARTIAL = '#7D6608'
GRAY  = '#888780'
GRID  = '#eeeeee'
BG    = 'white'

fig = plt.figure(figsize=(16, 12))
fig.patch.set_facecolor(BG)
fig.suptitle(
    'Validação das Hipóteses de Pesquisa — Resultados Experimentais\n'
    'Spark Structured Streaming + Kubernetes · Dataset 3W (Petrobras)',
    fontsize=13, fontweight='normal', y=0.99, color='#1a1a1a'
)

gs = gridspec.GridSpec(2, 2, figure=fig,
                       hspace=0.45, wspace=0.32,
                       left=0.05, right=0.97,
                       top=0.92, bottom=0.04)

STATUS = {
    'CONFIRMADA':          ('#E9F7EF', GREEN,  '✔ CONFIRMADA'),
    'CONFIRMADA PARCIAL':  ('#FEF9E7', PARTIAL, '◑ PARCIALMENTE CONFIRMADA'),
    'NÃO AVALIADA':        ('#F4F6F7', GRAY,    '○ PENDENTE — experimento futuro'),
}

def hipotese_panel(ax, code, title, evidence, status_key, metrics=None):
    bg, col, label = STATUS[status_key]
    ax.set_facecolor(BG)
    ax.set_xlim(0, 10); ax.set_ylim(0, 10); ax.axis('off')

    # border
    ax.add_patch(FancyBboxPatch((0.05, 0.05), 9.9, 9.9,
                                boxstyle='round,pad=0.1',
                                facecolor=BG, edgecolor=col, linewidth=1.8,
                                transform=ax.transData, zorder=1))

    # status badge
    ax.add_patch(FancyBboxPatch((0.2, 8.5), 9.6, 1.2,
                                boxstyle='round,pad=0.1',
                                facecolor=bg, edgecolor=col, linewidth=0.8,
                                transform=ax.transData, zorder=2))
    ax.text(5, 9.15, label, ha='center', va='center', fontsize=10,
            color=col, fontweight='bold', transform=ax.transData)

    # hypothesis code + title
    ax.text(0.4, 8.1, code, ha='left', va='top', fontsize=10,
            color=col, fontweight='bold', transform=ax.transData)
    # wrap title manually
    words = title.split()
    line, lines = '', []
    for w in words:
        test = (line + ' ' + w).strip()
        if len(test) > 62:
            lines.append(line); line = w
        else:
            line = test
    if line: lines.append(line)
    for k, ln in enumerate(lines[:2]):
        ax.text(0.4, 7.6 - k*0.75, ln, ha='left', va='top', fontsize=8.5,
                color='#222222', transform=ax.transData)

    # divider
    ax.plot([0.3, 9.7], [6.15, 6.15], color='#dddddd', linewidth=0.8)

    # evidence items
    ax.text(0.4, 5.85, 'Evidências:', ha='left', va='top', fontsize=8.5,
            color=GRAY, style='italic', transform=ax.transData)
    for k, ev in enumerate(evidence):
        y = 5.35 - k * 0.78
        if y < 0.3: break
        marker = '▸'
        ax.text(0.5, y, marker, ha='left', va='top', fontsize=8,
                color=col, transform=ax.transData)
        ax.text(1.0, y, ev, ha='left', va='top', fontsize=8,
                color='#333333', transform=ax.transData, wrap=False)

    # metrics bar (optional)
    if metrics:
        bar_y = 0.5
        ax.text(0.4, bar_y + 0.55, 'Métrica-chave:', ha='left', va='bottom',
                fontsize=7.5, color=GRAY, transform=ax.transData)
        for i, (lbl, val, maxv, c) in enumerate(metrics):
            bx = 0.4 + i * 4.6
            w = min(val / maxv, 1.0) * 4.0
            ax.add_patch(FancyBboxPatch((bx, bar_y - 0.25), 4.0, 0.45,
                                        boxstyle='round,pad=0.02',
                                        facecolor='#f0f0f0', edgecolor='#dddddd',
                                        linewidth=0.5, transform=ax.transData))
            ax.add_patch(FancyBboxPatch((bx, bar_y - 0.25), max(w, 0.1), 0.45,
                                        boxstyle='round,pad=0.02',
                                        facecolor=c, edgecolor='none',
                                        linewidth=0, transform=ax.transData, alpha=0.75))
            ax.text(bx + 0.1, bar_y + 0.05, f'{lbl}: {val}',
                    ha='left', va='center', fontsize=7.5,
                    color='white' if w > 0.8 else '#333333',
                    fontweight='bold', transform=ax.transData)

# ── H1 ────────────────────────────────────────────────────────────────────────
ax1 = fig.add_subplot(gs[0, 0])
hipotese_panel(
    ax1,
    'H1 — Hipótese Principal',
    'Spark + Kubernetes é viável para processamento de dados de monitoramento offshore e ML para Digital Twins.',
    [
        '3.216 rec/s > limiar mínimo de 42 rec/s (1 poço a 1 Hz × overhead)',
        'Sem batch falling behind: trigger_execution = 28.708 ms (< 31.093 s total)',
        '36.182 janelas de 60s processadas corretamente, sem perda de dados',
        'Latência de 65,1s < SLA de 120s (2 min) para alertas de DT offshore',
        'Arquitetura Spark Operator + EKS reproduzível e documentada',
    ],
    'CONFIRMADA',
    metrics=[
        ('Throughput', 3216, 3216, EKS),
        ('Latência (s)', 65, 120, EKS),
    ]
)

# ── H2 ────────────────────────────────────────────────────────────────────────
ax2 = fig.add_subplot(gs[0, 1])
hipotese_panel(
    ax2,
    'H2 — Baseline Local',
    'local[*] apresenta degradação severa de latência em streaming contínuo, inviabilizando uso em Digital Twin.',
    [
        'Batch falling behind confirmado: duração escalou de 500ms → ~22s/batch',
        'Latência acumulada: 369,7s (~6 min/evento) — 5,7× acima do SLA de 2 min',
        'Throughput degradado: 5,82 rec/s vs 3.216 rec/s no EKS (553× inferior)',
        'Modo contínuo inviável: sistema não consegue processar janelas de 60s em 500ms',
        'Causa raiz: local[4] sem paralelismo real + shuffle em disco local',
    ],
    'CONFIRMADA',
    metrics=[
        ('Latência (s)', 370, 370, LOC),
        ('Throughput', 6, 3216, LOC),
    ]
)

# ── H3 ────────────────────────────────────────────────────────────────────────
ax3 = fig.add_subplot(gs[1, 0])
hipotese_panel(
    ax3,
    'H3 — Escalabilidade',
    'Spark + Kubernetes escala proporcionalmente ao aumento de executor pods, com eficiência mensurável.',
    [
        'Experimento E2 executado com configuração fixa: 4 executor pods',
        'Experimentos de escala (2, 4, 8 pods) ainda não executados',
        'Resultado atual: 3.216 rec/s com 4 executors — base para comparação futura',
        'Hipótese não pode ser confirmada ou refutada com um único ponto de dado',
        'Trabalho futuro: repetir E2 com 2× e 4× executors para curva de speedup',
    ],
    'NÃO AVALIADA',
)

# ── H4 ────────────────────────────────────────────────────────────────────────
ax4 = fig.add_subplot(gs[1, 1])
hipotese_panel(
    ax4,
    'H4 — Overhead do Kubernetes',
    'O overhead do Kubernetes (scheduling, S3, IRSA) mantém-se em fração controlável do tempo total.',
    [
        'Tempo total E2: 31,09s · trigger_execution: 28,71s → overhead: 2,38s (7,7%)',
        'Para 100K registros o overhead (~7,7%) é significativo mas aceitável',
        'Overhead esperado reduz proporcionalmente com volumes maiores (500K, 2,8M)',
        'Estimativa: com 2,8M registros (dataset 3W completo), overhead < 2%',
        'H4 parcialmente confirmada — overhead controlável mas não desprezível a 100K',
    ],
    'CONFIRMADA PARCIAL',
    metrics=[
        ('Overhead atual', 8, 20, AMBER),
        ('Overhead estimado 2,8M', 2, 20, GREEN),
    ]
)

# ── footer ────────────────────────────────────────────────────────────────────
fig.text(0.97, 0.01, 'Fonte: resultados da pesquisa.',
         fontsize=8, color=GRAY, ha='right')
fig.text(0.03, 0.01,
         '✔ Confirmada     ◑ Parcialmente confirmada     ○ Pendente (experimento futuro)',
         fontsize=8.5, color=GRAY)

plt.savefig('/mnt/user-data/outputs/fig6_7_validacao_hipoteses.png',
            dpi=180, bbox_inches='tight', facecolor=BG)
plt.close()
print("Fig 6.7 salva.")
