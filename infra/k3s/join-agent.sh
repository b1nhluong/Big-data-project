#!/usr/bin/env bash
# [TV1] Laptop thứ 2, 3 tham gia cụm:  K3S_URL=https://<ip-server>:6443 K3S_TOKEN=<token> bash join-agent.sh
set -euo pipefail
: "${K3S_URL:?cần K3S_URL}" "${K3S_TOKEN:?cần K3S_TOKEN}"
TS_IP=$(tailscale ip -4)
curl -sfL https://get.k3s.io | K3S_URL="$K3S_URL" K3S_TOKEN="$K3S_TOKEN" \
  INSTALL_K3S_EXEC="agent --node-ip ${TS_IP} --flannel-iface tailscale0" sh -
