# k8s

## Installation

```bash
# Install k3s without Traefik
curl -sfL https://get.k3s.io | INSTALL_K3S_EXEC="--disable=traefik" sh -

# Install kubectl
curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
chmod +x kubectl

# Install kubectl with colors
go install github.com/kubecolor/kubecolor@latest

# Allow kubectl to run without root
mkdir -p ~/.kube
sudo cp /etc/rancher/k3s/k3s.yaml ~/.kube/config
sudo chown $USER:$USER ~/.kube/config
echo 'export KUBECONFIG=~/.kube/config' >> ~/.bashrc
```

## Setup

```bash
docker build -t hello:dev .
docker save hello:dev | sudo k3s ctr images import -

kube apply -f app.yaml -f proxy.yaml
kube get pods -w

kube port-forward svc/proxy 8000:80
for i in $(seq 6); do curl -s localhost:8080; done
```
