#!/usr/bin/env bash
# [TV1] Dựng node server k3s trên laptop thứ nhất. Các laptop nối với nhau qua Tailscale
# (mạng riêng ảo, không cần cùng Wi-Fi). Chạy trên Linux (hoặc VM Ubuntu trên macOS/Windows).
#   curl -fsSL https://tailscale.com/install.sh | sh && sudo tailscale up
#   bash infra/k3s/install-server.sh
set -euo pipefail
TS_IP=$(tailscale ip -4)
curl -sfL https://get.k3s.io | INSTALL_K3S_EXEC="server \
  --node-ip ${TS_IP} --advertise-address ${TS_IP} --flannel-iface tailscale0 \
  --disable traefik --write-kubeconfig-mode 644" sh -
echo "Token để các máy khác tham gia:"
sudo cat /var/lib/rancher/k3s/server/node-token
echo "Server URL: https://${TS_IP}:6443"
mkdir -p ~/.kube && sudo cp /etc/rancher/k3s/k3s.yaml ~/.kube/config && sudo chown "$USER" ~/.kube/config
kubectl get nodes -o wide
