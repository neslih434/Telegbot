#!/usr/bin/env bash
set -e

# Quick deploy script for vm.u1host.com (Ubuntu)
# Usage:
#   sudo bash deploy/install_vm_u1host.sh https://github.com/<owner>/<repo>.git

if [[ "${EUID}" -ne 0 ]]; then
  echo "Run as root: sudo bash deploy/install_vm_u1host.sh <repo_url>"
  exit 1
fi

REPO_URL="${1:-}"
if [[ -z "${REPO_URL}" ]]; then
  read -r -p "GitHub repo URL (for example https://github.com/vuducngo290-code/Telegram-bot2.git): " REPO_URL
fi

APP_DIR="/opt/telegram-bot2"
BOT_USER="bot"
SERVICE_NAME="telegram-bot2"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"

echo "[1/8] Installing system packages..."
apt update -y
apt install -y git python3 python3-venv python3-pip

echo "[2/8] Creating bot user (if missing)..."
if ! id -u "${BOT_USER}" >/dev/null 2>&1; then
  adduser --disabled-password --gecos "" "${BOT_USER}"
fi

echo "[3/8] Downloading project..."
mkdir -p /opt
if [[ -d "${APP_DIR}/.git" ]]; then
  su - "${BOT_USER}" -c "cd ${APP_DIR} && git pull"
else
  rm -rf "${APP_DIR}"
  su - "${BOT_USER}" -c "cd /opt && git clone ${REPO_URL} telegram-bot2"
fi
chown -R "${BOT_USER}:${BOT_USER}" "${APP_DIR}"

echo "[4/8] Creating virtual environment and installing dependencies..."
su - "${BOT_USER}" -c "cd ${APP_DIR} && python3 -m venv .venv && . .venv/bin/activate && pip install --upgrade pip && pip install -r requirements.txt"

echo "[5/8] Preparing .env and data folder..."
su - "${BOT_USER}" -c "cd ${APP_DIR} && mkdir -p /opt/telegram-bot2/data"
if [[ ! -f "${APP_DIR}/.env" ]]; then
  su - "${BOT_USER}" -c "cd ${APP_DIR} && cp .env.example .env"
fi

echo "[6/8] Installing systemd service..."
cp "${APP_DIR}/deploy/telegram-bot2.service" "${SERVICE_FILE}"
systemctl daemon-reload
systemctl enable "${SERVICE_NAME}"

echo "[7/8] Checking .env values..."
if grep -Eq "replace_me|1234567890:replace_me|API_ID=123456" "${APP_DIR}/.env"; then
  echo
  echo "You must fill .env first:"
  echo "  nano ${APP_DIR}/.env"
  echo
  echo "After editing, run:"
  echo "  systemctl restart ${SERVICE_NAME}"
  echo "  systemctl status ${SERVICE_NAME} --no-pager"
  echo "  journalctl -u ${SERVICE_NAME} -f"
  exit 0
fi

echo "[8/8] Starting bot..."
systemctl restart "${SERVICE_NAME}"
sleep 2
systemctl --no-pager --full status "${SERVICE_NAME}" || true

echo

echo "Done. Useful commands:"
echo "  systemctl status ${SERVICE_NAME} --no-pager"
echo "  journalctl -u ${SERVICE_NAME} -f"
