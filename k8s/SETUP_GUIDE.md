# Setup: EKS + Spark Operator — Benchmark 3W

---

## Arquitetura

```
EKS Control Plane (gerenciado pela AWS)
        │
        ▼
Node Group (EC2)
├── benchmark-node-1 (t3.xlarge)  ← Spark Driver + Executor
└── benchmark-node-2 (t3.xlarge)  ← Spark Executor

        │  lê/escreve
        ▼
S3: s3://seu-bucket-3w/
├── 3w/dataset/   ← parquet do dataset 3W
└── results/      ← métricas do benchmark
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
```

---

## Parte 1: Dataset no S3

```bash
# Criar bucket
aws s3 mb s3://seu-bucket-3w --region us-east-1

# Upload do dataset 3W
aws s3 sync /home/jonathan/3W/dataset s3://seu-bucket-3w/3w/dataset/ \
  --storage-class STANDARD_IA

# Upload do script de benchmark
aws s3 cp benchmark_simple/run_spark_benchmark.py s3://seu-bucket-3w/scripts/
```

---

## Parte 2: Criar o cluster EKS

```bash
eksctl create cluster -f k8s/cluster.yaml
# Leva ~15 min

# Verificar
kubectl get nodes
```

---

## Parte 3: IAM — acesso S3 via IRSA

```bash
# Criar policy S3
cat > /tmp/s3-policy.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": ["s3:GetObject","s3:PutObject","s3:ListBucket","s3:DeleteObject"],
    "Resource": [
      "arn:aws:s3:::seu-bucket-3w",
      "arn:aws:s3:::seu-bucket-3w/*"
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

# Criar service account com IRSA (a role IAM fica vinculada ao SA do K8s)
eksctl create iamserviceaccount \
  --cluster benchmark-3w \
  --namespace spark \
  --name spark \
  --attach-policy-arn $POLICY_ARN \
  --approve \
  --override-existing-serviceaccounts
```

---

## Parte 4: Spark Operator

```bash
kubectl create namespace spark

helm repo add spark-operator https://kubeflow.github.io/spark-operator
helm repo update

helm install spark-operator spark-operator/spark-operator \
  --namespace spark \
  --set sparkJobNamespace=spark \
  --set webhook.enable=true

# Verificar
kubectl get pods -n spark
```

---

## Parte 5: SparkApplication — benchmark 3W

```bash
kubectl apply -f k8s/spark/spark-benchmark.yaml

# Acompanhar status
kubectl get sparkapplication -n spark -w

# Logs do driver
kubectl logs -f 3w-spark-benchmark-driver -n spark

# Spark UI (enquanto roda)
kubectl port-forward pod/3w-spark-benchmark-driver 4040:4040 -n spark
# http://localhost:4040
```

---

## Parte 6: Teardown

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
| **Total por sessão** | | **~$0.90** |

> **Importante:** o EKS cobra o control plane mesmo com o cluster parado. Delete o cluster ao terminar.

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
