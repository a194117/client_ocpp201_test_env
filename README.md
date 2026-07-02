# OCPP Test Environment

Ambiente para simulação de um cliente OCPP 2.0.1 e testes de transações com o servidor CitrineOS.

## Pré-requisitos

- Python 3.10+
- CSMS Opnesouce CitrineOS
- WSL (opcional, para executar scripts shell)

## Instalação

```bash
git clone https://github.com/a194117/client_ocpp201_test_env.git
cd client_ocpp201_test_env
python3 -m venv venv
source venv/bin/activate  
pip install -r requirements.txt
cp config/.env.example config/.env
chmod +x ./scripts/*.sh
# Edite config/.env com suas configurações
```

## Execute

```bash
./scripts/run_interactive.sh
