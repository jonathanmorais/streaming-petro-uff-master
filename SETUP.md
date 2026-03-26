# Setup Guide — Benchmark 3W no EKS

Passo a passo completo para subir o cluster e rodar o benchmark do zero,
incluindo todos os erros encontrados durante o desenvolvimento e como evitá-los.

---

## Pré-requisitos

```bash
# Ferramentas necessárias
aws --version       # AWS CLI v2
eksctl version      # >= 0.190
kubectl version     # >= 1.28
helm version        # >= 3.14
docker buildx version  # para build multi-arch (ARM64)
```

Configure o AWS CLI com uma role/user que tenha permissão para criar clusters EKS:

```bash
aws configure
aws sts get-caller-identity  # confirma que está autenticado
```

---

## 1. Criar o cluster EKS

```bash
eksctl create cluster -f k8s/cluster.yaml
```

> O arquivo `k8s/cluster.yaml` define o cluster `benchmark-3w` com nós `c6g.xlarge`
> (ARM64/Graviton2). A flag `withOIDC: true` é obrigatória para o passo de IRSA
> (acesso ao S3 sem credenciais hardcoded).

Aguarde ~15 minutos. Ao final, o kubeconfig é atualizado automaticamente:

```bash
kubectl get nodes  # deve listar 3 nós em estado Ready
```

---

## 2. Criar namespace e IAM Service Account (IRSA)

```bash
# Cria o namespace
kubectl create namespace spark

# Cria o ServiceAccount com a IAM Role para acesso ao S3
# IMPORTANTE: faça isso ANTES de aplicar o rbac.yaml
# O eksctl cria o SA com a annotation eks.amazonaws.com/role-arn
# Se você aplicar o rbac.yaml antes, o SA é criado sem a annotation e os executors
# não conseguem acessar o S3 (erro 403 AccessDenied)
eksctl create iamserviceaccount \
  --name spark \
  --namespace spark \
  --cluster benchmark-3w \
  --attach-policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess \
  --approve \
  --override-existing-serviceaccounts
```

Verifique que a annotation foi criada:

```bash
kubectl get serviceaccount spark -n spark -o yaml | grep role-arn
# deve mostrar: eks.amazonaws.com/role-arn: arn:aws:iam::XXXX:role/...
```

---

## 3. Aplicar RBAC

O Spark Operator v2.5.0 não concede ao ServiceAccount `spark` as permissões
necessárias para o driver pod gerenciar os executor pods. Sem este passo você
verá erros como:

- `pods is forbidden: User "system:serviceaccount:spark:spark" cannot create resource "pods"`
- `configmaps is forbidden: ...`
- `services is forbidden: ...`
- `persistentvolumeclaims is forbidden: ...`

Aplique o RBAC **após** criar o SA via eksctl (passo 2), para não sobrescrever
a annotation IRSA:

```bash
kubectl apply -f k8s/spark/rbac.yaml
```

Verifique:

```bash
kubectl get role spark-driver-role -n spark
kubectl get rolebinding spark-driver-rolebinding -n spark
```

> **Por que `deletecollection` é necessário?**
> O Spark scheduler (no shutdown) chama `deletecollection` em pods, configmaps,
> services e PVCs para limpar recursos de uma vez. Sem esse verb, o cleanup falha
> com `forbidden` e o SparkApplication fica preso em estado `FAILING`.

---

## 4. Instalar o Spark Operator

```bash
helm repo add spark-operator https://kubeflow.github.io/spark-operator
helm repo update

helm install spark-operator spark-operator/spark-operator \
  --namespace spark-operator \
  --create-namespace \
  --set webhook.enable=true \
  --version 2.5.0
```

Aguarde o operator ficar pronto:

```bash
kubectl rollout status deployment spark-operator-controller -n spark-operator
```

---

## 5. Build e push da imagem Docker

> **ATENÇÃO — arquitetura ARM64:**
> Os nós `c6g` são Graviton2 (ARM64). Se você buildar a imagem em uma máquina
> x86/amd64 sem especificar a plataforma, o Kubernetes tentará executar um binário
> amd64 em ARM64 e falhará com:
> ```
> exec /opt/entrypoint.sh: exec format error
> ```
> Sempre especifique `--platform linux/arm64` ao buildar para c6g.

```bash
# Autenticar no ECR
AWS_ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
AWS_REGION=us-east-1
ECR_REPO=$AWS_ACCOUNT.dkr.ecr.$AWS_REGION.amazonaws.com/spark-benchmark

aws ecr get-login-password --region $AWS_REGION \
  | docker login --username AWS --password-stdin $AWS_ACCOUNT.dkr.ecr.$AWS_REGION.amazonaws.com

# Criar repositório ECR (apenas na primeira vez)
aws ecr create-repository --repository-name spark-benchmark --region $AWS_REGION

# Build para ARM64 (c6g/Graviton) e push direto para o ECR
docker buildx build \
  --platform linux/arm64 \
  -f Dockerfile.spark-benchmark \
  -t $ECR_REPO:latest \
  --push \
  .
```

Se o seu cluster usasse nós x86 (`t3`, `m5`, `c5`), seria `--platform linux/amd64`.
Para suportar ambos simultaneamente: `--platform linux/amd64,linux/arm64`.

Habilite o buildx se necessário:

```bash
docker buildx create --use --name multiarch
docker buildx inspect --bootstrap
```

---

## 6. Criar bucket S3

```bash
aws s3 mb s3://streaming-3w --region us-east-1

# Verifique que o dataset está no lugar certo
aws s3 ls s3://streaming-3w/3w/dataset/ --recursive | head -5
```

---

## 7. Rodar o benchmark

```bash
# Submete o SparkApplication ao Operator
kubectl apply -f k8s/spark/spark-benchmark.yaml

# Acompanha o status
kubectl get sparkapplication benchmark-3w-spark -n spark -w

# Logs do driver em tempo real
kubectl logs -n spark -l spark-role=driver -f

# Ao terminar, verifica os resultados no S3
aws s3 ls s3://streaming-3w/results/spark/
aws s3 cp s3://streaming-3w/results/spark/metrics.json - | python3 -m json.tool
```

---

## 8. Limpeza

```bash
# Remove o SparkApplication
kubectl delete sparkapplication benchmark-3w-spark -n spark

# Para destruir o cluster inteiro (cobra até ser deletado)
eksctl delete cluster --name benchmark-3w
```

---

## Troubleshooting rápido

| Erro | Causa | Solução |
|---|---|---|
| `exec format error` | Imagem amd64 em nó ARM64 (c6g) | Rebuildar com `--platform linux/arm64` |
| `pods is forbidden` | RBAC não aplicado | `kubectl apply -f k8s/spark/rbac.yaml` |
| `deletecollection is forbidden` | Verb ausente no Role | Já incluído no `rbac.yaml` atual |
| `403 AccessDenied` no S3 | IRSA annotation ausente no SA | Verificar annotation `eks.amazonaws.com/role-arn`; recriar SA com eksctl |
| `IRSA annotation perdida` | `rbac.yaml` sobrescreveu o SA | Não definir SA no `rbac.yaml`; criar apenas com eksctl |
| `0 registros no streaming` | Staging em `_staging/` (underscore) | Diretório deve ser `staging/` sem underscore |
| `FailedScheduling: Insufficient cpu` | `coreRequest` alto demais | Reduzir `coreRequest` no YAML (ver `spark-benchmark.yaml`) |
| `TIMESTAMP(NANOS) AnalysisException` | Parquet com nanosegundos | Pipeline usa boto3+pyarrow — não requer configuração extra |
