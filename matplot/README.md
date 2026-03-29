# Scripts de Geração de Figuras — Dissertação

Scripts Python usados para gerar as figuras do Capítulo 5 e 6.

## Dependências

```bash
pip install matplotlib numpy
```

## Figuras geradas

| Script | Figura | Descrição |
|--------|--------|-----------|
| `fig_6_1_throughput_e_latencia.py` | Fig 6.1–6.5 | Throughput, latência, falling behind, janelas, CDF |
| `fig_6_6_resumo_metricas.py` | Fig 6.6 | Dashboard resumo de todas as métricas |
| `fig_6_7_hipoteses.py` | Fig 6.7 | Validação das hipóteses H1–H4 |
| `fig_aws_cloudwatch.py` | Fig 6.X | Métricas CloudWatch: CPU, Net In, Net Out |
| `fig_5_1_arquitetura_eks.py` | Fig 5.1 | Diagrama da arquitetura EKS + Spark Operator + S3 |

## Dados de entrada

Os scripts usam os dados reais do experimento E2 extraídos de:
- `spark_metrics.json` — métricas do Spark Structured Streaming (throughput, latência, janelas)
- Screenshots do Amazon CloudWatch — CPU e rede dos nós c6g.xlarge

Os valores numéricos estão embutidos diretamente nos scripts como constantes,
extraídos do spark_metrics.json e das capturas de tela do CloudWatch.

## Como executar

```bash
python fig_6_1_throughput_e_latencia.py
# Gera: fig6_1_throughput_comparativo.png
#       fig6_2_latencia_percentil.png
#       fig6_3_batch_falling_behind.png
#       fig6_4_distribuicao_janelas.png
#       fig6_5_cdf_latencia_eks.png
#       tabela_6_1_experimentos.png

python fig_6_6_resumo_metricas.py
# Gera: fig6_6_resumo_metricas.png

python fig_6_7_hipoteses.py
# Gera: fig6_7_validacao_hipoteses.png

python fig_aws_cloudwatch.py
# Gera: fig_aws_cloudwatch_completo.png

python fig_5_1_arquitetura_eks.py
# Gera: fig5_1_arquitetura_eks.png
```

## Repositório

https://github.com/jonathanmorais/streaming-petro-uff-master
