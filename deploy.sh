#!/usr/bin/env bash
set -e

# 0. Проверка переменных
: "${BINANCE_KEY?}"; : "${BINANCE_SECRET?}"; : "${TG_BOT_TOKEN?}"; : "${TG_CHAT_ID?}"

echo "📦 1. Устанавливаю пакеты"
sudo apt-get update -qq
sudo apt-get install -y git python3.11 python3.11-venv build-essential cron

echo "📁 2. Клонирую репо"
[ -d ~/pairs_lab ] || git clone https://github.com/ValeraTotsenko/pairs_lab.git ~/pairs_lab
cd ~/pairs_lab

echo "🐍 3. Создаю venv и ставлю зависимости"
python3.11 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install duckdb pandas numpy tqdm aiogram pymoo vectorbt ccxt

echo "🗄 4. Инициализирую DuckDB"
python scripts/init_db.py

echo "⬇️ 5. Качаю историю свечей"
python scripts/ingest_binance.py --days 365 \
      --api_key "$BINANCE_KEY" --api_secret "$BINANCE_SECRET" --silent

echo "🔎 6. Кандидаты → анализ → мульти-NSGA"
python scripts/find_candidates.py
python scripts/copula_fit.py
python scripts/build_params_multi.py

echo "⚙️ 7. Создаю systemd-unit для мультипарного бота"
SERVICE=/etc/systemd/system/pairsbot.service
sudo tee $SERVICE > /dev/null <<EOF
[Unit]
Description=Pairs Trading Multi-Bot
After=network.target

[Service]
Type=simple
Environment="BINANCE_KEY=$BINANCE_KEY"
Environment="BINANCE_SECRET=$BINANCE_SECRET"
Environment="TG_BOT_TOKEN=$TG_BOT_TOKEN"
Environment="TG_CHAT_ID=$TG_CHAT_ID"
WorkingDirectory=$(pwd)
ExecStart=$(pwd)/.venv/bin/python scripts/online_signals_multi.py
Restart=on-failure

[Install]
WantedBy=multi-user.target
EOF

echo "⏰ 8. Cron на ETL+retrain+переоптимизацию"
sudo tee /etc/cron.d/pairs_lab > /dev/null <<EOF
0 3 * * * $(whoami) cd $(pwd) && \
  source .venv/bin/activate && \
  python scripts/ingest_binance.py --days 1 --silent && \
  python scripts/find_candidates.py && \
  python scripts/copula_fit.py && \
  python scripts/build_params_multi.py
EOF

echo "🚀 9. Запуск сервиса"
sudo systemctl daemon-reload
sudo systemctl enable --now pairsbot.service
sudo systemctl status pairsbot.service --no-pager

echo "✅ Готово! Мультипарный сигнальный бот работает, параметры обновляются каждую ночь."
echo ""
echo "‼️ Если потребуется ручной запуск: source .venv/bin/activate && python scripts/online_signals_multi.py"
