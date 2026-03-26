# Setup: EKS + Spark Operator — Benchmark 3W

---

## Por que Kubernetes (EKS)?

O benchmark compara Spark Structured Streaming vs Flink em condições controladas e reproduzíveis.
Rodar localmente (docker-compose) serve para desenvolvimento, mas introduz variáveis que contaminam
os resultados: contenção de CPU/memória com outros processos, I/O do disco local, rede loopback.

O EKS resolve isso de três formas:

| Problema local | Solução no EKS |
|---|---|
| Recursos compartilhados com o SO | Nós EC2 dedicados ao benchmark |
| Dados no disco local (latência variável) | Dataset no S3 (latência de rede consistente) |
| Scale manual via `docker-compose scale` | `replicas:` declarativo, Operator gerencia |
| Resultados não reproduzíveis por outros pesquisadores | Cluster recriável em ~15 min via `cluster.yaml` |

O **Spark Operator** (Kubeflow) traduz um `SparkApplication` YAML em pods Kubernetes,
gerenciando o ciclo de vida do driver e dos executors sem precisar de um cluster Spark permanente.

---

## Arquitetura

```
EKS Control Plane (gerenciado pela AWS)
        │
        ▼
Node Group (EC2)
├── benchmark-node-1 (t3.xlarge 4 vCPU / 16 GB)  ← Spark Driver + Executor
└── benchmark-node-2 (t3.xlarge 4 vCPU / 16 GB)  ← Spark Executor

        │  lê/escreve via s3a://
        ▼
S3: s3://streaming-3w/
├── 3w/dataset/       ← dataset 3W (parquet, ~2 GB)
├── results/spark/    ← métricas e resultados do benchmark
└── results/flink/    ← (futuro)
```

O acesso ao S3 é feito via **IRSA** (IAM Roles for Service Accounts): a IAM role fica anotada
no ServiceAccount Kubernetes `spark`, e o pod recebe um token Web Identity automaticamente
montado em `/var/run/secrets/eks.amazonaws.com/serviceaccount/token`. Nenhuma credencial
hardcoded na imagem ou no YAML.

---

## Por que uma imagem Docker customizada?

A imagem oficial `apache/spark:3.5.3-python3` **não inclui** `hadoop-aws` nem `aws-java-sdk-bundle`.
Sem esses JARs o Spark não consegue inicializar o filesystem S3A (`s3a://`).

A solução natural seria `spark.jars.packages` (download via Maven em runtime), mas isso cria
um problema de ovo e galinha:

```
spark-submit tenta baixar o script de s3a://...
    └─ para isso inicializa S3AFileSystem
        └─ que precisa de aws-java-sdk-bundle
            └─ que só seria baixado pelo spark.jars.packages
                └─ que só roda depois do script ser baixado ← LOOP
```

A imagem customizada ([Dockerfile.spark-benchmark](../Dockerfile.spark-benchmark)) resolve os dois problemas:

1. **JARs pré-instalados** em `/opt/spark/jars/` — S3A funciona desde o início
2. **Script embutido** em `/opt/spark/scripts/` — `mainApplicationFile: local:///...` sem dependência de S3

```dockerfile
FROM apache/spark:3.5.3-python3
RUN cd /opt/spark/jars && \
    curl -O .../hadoop-aws-3.3.4.jar && \
    curl -O .../aws-java-sdk-bundle-1.12.262.jar
COPY benchmark_simple/run_spark_benchmark.py /opt/spark/scripts/
```

---

## Pré-requisitos

```bash
# AWS CLI
aws --version

# eksctl
curl -sLO "https://github.com/eksctl-io/eksctl/releases/latest/download/eksctl_Linux_amd64.tar.gz"
tar xzf eksctl_Linux_amd64.tar.gz && sudo mv eksctl /usr/local/bin/
eksctl version

# kubectl
curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
sudo install kubectl /usr/local/bin/
kubectl version --client

# Helm
curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash

# Docker (para build da imagem customizada)
docker --version
```

---

## Parte 1: Dataset no S3

```bash
aws s3 mb s3://streaming-3w --region us-east-1

aws s3 sync /home/jonathan/3W/dataset s3://streaming-3w/3w/dataset/ \
  --storage-class STANDARD_IA
```

---

## Parte 2: Criar o cluster EKS

```bash
eksctl create cluster -f k8s/cluster.yaml
# Leva ~15 min

kubectl get nodes
```

---

## Parte 3: IAM — acesso S3 via IRSA

```bash
cat > /tmp/s3-policy.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": ["s3:GetObject","s3:PutObject","s3:ListBucket","s3:DeleteObject"],
    "Resource": [
      "arn:aws:s3:::streaming-3w",
      "arn:aws:s3:::streaming-3w/*"
    ]
  }]
}
EOF

aws iam create-policy \
  --policy-name benchmark-3w-s3 \
  --policy-document file:///tmp/s3-policy.json

POLICY_ARN=$(aws iam list-policies \
  --query "Policies[?PolicyName=='benchmark-3w-s3'].Arn" \
  --output text)

eksctl create iamserviceaccount \
  --cluster benchmark-3w \
  --namespace spark \
  --name spark \
  --attach-policy-arn $POLICY_ARN \
  --approve \
  --override-existing-serviceaccounts
```

---

## Parte 4: Build e push da imagem customizada

```bash
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REGION=us-east-1
IMAGE=$ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/spark-benchmark:latest

# Criar repositório ECR (só na primeira vez)
aws ecr create-repository --repository-name spark-benchmark --region $REGION

# Login no ECR
aws ecr get-login-password --region $REGION \
  | docker login --username AWS --password-stdin \
    $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com

# Build e push
docker build -f Dockerfile.spark-benchmark -t --platform linux/arm64 $IMAGE .
docker push $IMAGE

# Atualiza o YAML com o account ID correto
sed -i "s/<SEU_ACCOUNT_ID>/$ACCOUNT_ID/" k8s/spark/spark-benchmark.yaml
```

---

## Parte 5: Spark Operator

```bash
kubectl create namespace spark

helm repo add spark-operator https://kubeflow.github.io/spark-operator
helm repo update

helm install spark-operator spark-operator/spark-operator \
  --namespace spark \
  --set sparkJobNamespace=spark \
  --set webhook.enable=true
```

### Correções de RBAC necessárias (Spark Operator v2.5.0)

O chart v2.5.0 gera RBAC incompleto — o controller não tem permissão de
`list`/`watch` nos próprios recursos que gerencia. Aplique os patches abaixo
**uma única vez** após instalar o Helm chart:

```bash
# 1. O controller assistia "default" em vez de "spark"
kubectl patch deployment spark-operator-controller -n spark \
  --type='json' \
  -p='[{"op":"replace","path":"/spec/template/spec/containers/0/args/4","value":"--namespaces=spark"}]'

# 2. Role namespace-scoped: faltavam sparkapplications, pods, services, etc.
kubectl patch role spark-operator-controller -n spark --type='json' -p='[
  {"op":"add","path":"/rules/-","value":{
    "apiGroups":["sparkoperator.k8s.io"],
    "resources":["sparkapplications","sparkapplications/status","sparkapplications/finalizers",
                 "scheduledsparkapplications","scheduledsparkapplications/status","scheduledsparkapplications/finalizers",
                 "sparkconnects","sparkconnects/status","sparkconnects/finalizers"],
    "verbs":["get","list","watch","create","update","patch","delete"]
  }},
  {"op":"add","path":"/rules/-","value":{
    "apiGroups":[""],
    "resources":["pods","services","configmaps","persistentvolumeclaims","serviceaccounts"],
    "verbs":["get","list","watch","create","update","patch","delete"]
  }}
]'

# 3. ClusterRole: faltavam sparkapplications e sparkconnects no nível cluster
kubectl patch clusterrole spark-operator-controller --type='json' -p='[
  {"op":"add","path":"/rules/-","value":{
    "apiGroups":["sparkoperator.k8s.io"],
    "resources":["sparkapplications","sparkapplications/status","sparkapplications/finalizers",
                 "scheduledsparkapplications","scheduledsparkapplications/status","scheduledsparkapplications/finalizers",
                 "sparkconnects","sparkconnects/status","sparkconnects/finalizers"],
    "verbs":["get","list","watch","create","update","patch","delete"]
  }}
]'

# 4. RoleBinding: permissão para criar pods no namespace spark
kubectl create rolebinding spark-operator-crb \
  --clusterrole=edit \
  --serviceaccount=spark:spark-operator-controller \
  -n spark

# 5. Reiniciar o controller para aplicar tudo
kubectl rollout restart deployment/spark-operator-controller -n spark
kubectl rollout status deployment/spark-operator-controller -n spark
```

**Verificação:**
```bash
kubectl auth can-i list sparkapplications -n spark \
  --as=system:serviceaccount:spark:spark-operator-controller
# deve retornar: yes
```

---

## Parte 6: Rodar o benchmark

```bash
kubectl apply -f k8s/spark/spark-benchmark.yaml

# Acompanhar status
kubectl get sparkapplication -n spark -w

# Logs do driver (nome gerado dinamicamente)
kubectl logs -f -l spark-role=driver -n spark

# Spark UI (enquanto roda)
kubectl port-forward \
  $(kubectl get pod -n spark -l spark-role=driver -o jsonpath='{.items[0].metadata.name}') \
  4040:4040 -n spark
# http://localhost:4040

# Resultados no S3
aws s3 ls s3://streaming-3w/results/spark/ --recursive
```

---

## Parte 7: Teardown

```bash
# Deletar apenas o job (mantém o cluster para o próximo teste)
kubectl delete sparkapplication 3w-spark-benchmark -n spark

# Deletar o cluster inteiro ao finalizar
eksctl delete cluster --name benchmark-3w --region us-east-1
```

---

## Custo estimado (us-east-1)

| Recurso | $/hr | Para 2h |
|---|---|---|
| 2× t3.xlarge (nodes) | $0.166/hr cada | ~$0.67 |
| EKS Control Plane | $0.10/hr | ~$0.20 |
| S3 (~2GB) | desprezível | — |
| ECR (imagem ~500MB) | desprezível | — |
| **Total por sessão** | | **~$0.90** |

> **Importante:** o EKS cobra o control plane mesmo com o cluster ocioso. Delete o cluster ao terminar.

---

## Problemas conhecidos e soluções

| Problema | Causa | Solução |
|---|---|---|
| `spec.env` rejeitado pelo webhook | `env` é field do driver/executor, não do spec raiz | Mover para `spec.driver.env` e `spec.executor.env` |
| Controller não cria pods | RBAC incompleto gerado pelo chart v2.5.0 | Patches de Role e ClusterRole (Parte 5) |
| `--namespaces=default` no controller | `sparkJobNamespace` depreciado no v2.x | Patch direto no deployment arg |
| `ClassNotFoundException: WebIdentityTokenFileCredentialsProvider` | hadoop-aws não está na imagem base; chicken-and-egg com `spark.jars.packages` | Imagem customizada com JARs pré-instalados |
| `configMap` não suportado em `spark.kubernetes.driver.volumes` | Spark Kubernetes só suporta `emptyDir`, `hostPath`, `pvc`, `nfs` | Script embutido na imagem via Dockerfile |
| `/nonexistent/.ivy2/cache` não existe | Usuário do container não tem home directory | `spark.jars.ivy=/tmp/.ivy` |
| Controller não reconcilia após delete+apply | Bug no informer do v2.5.0: perde o evento CREATE quando o objeto é recriado rápido demais | `sleep 5` entre delete e apply, ou `kubectl rollout restart deployment/spark-operator-controller -n spark` |
| `s3:ListBucket` negado mesmo com IRSA funcionando | IAM policy criada com nome de bucket placeholder (`seu-bucket-3w`) | Atualizar a policy com o nome real do bucket via `aws iam create-policy-version` |
| `Illegal Parquet type: INT64 (TIMESTAMP(NANOS,false))` | Dataset 3W usa timestamps em nanossegundos, não suportados por padrão no Spark | Adicionar `spark.sql.parquet.nanosAsLong=true` ao `sparkConf` |

---

## Próximo passo: Flink Operator

Após validar o Spark, adicionar o Flink Operator no mesmo cluster:

```bash
helm repo add flink-operator-repo \
  https://downloads.apache.org/flink/flink-kubernetes-operator-1.11.0/
helm install flink-kubernetes-operator \
  flink-operator-repo/flink-kubernetes-operator \
  --namespace flink --create-namespace
```

---

## Referências

- [eksctl](https://eksctl.io/)
- [Kubeflow Spark Operator](https://www.kubeflow.org/docs/components/spark-operator/)
- [Spark S3A Guide](https://spark.apache.org/docs/latest/cloud-integration.html)
- [IRSA — IAM Roles for Service Accounts](https://docs.aws.amazon.com/eks/latest/userguide/iam-roles-for-service-accounts.html)
- [Spark Operator v2 RBAC](https://github.com/kubeflow/spark-operator/tree/master/charts/spark-operator-chart)
