INVENTORY  = ansible/inventory.ini
VAULT_FILE = ansible/vars/vault.yml
VAULT_PASS_FILE = ~/.ansible_vault_pass

deploy:
	ansible-playbook -i $(INVENTORY) ansible/playbook.yml --vault-password-file $(VAULT_PASS_FILE)

deploy-clickhouse:
	ansible-playbook -i $(INVENTORY) ansible/clickhouse.yml --vault-password-file $(VAULT_PASS_FILE)

deploy-scripts:
	ansible-playbook -i $(INVENTORY) ansible/scripts.yml --vault-password-file $(VAULT_PASS_FILE)

deploy-bitrix:
	ansible-playbook -i $(INVENTORY) ansible/bitrix.yml --vault-password-file $(VAULT_PASS_FILE)

# Ручной прогон импорта Bitrix24 на сервере (вывод — в консоль и в /opt/scripts/bitrix/logs)
run-bitrix:
	ansible clickhouse -i $(INVENTORY) -b -m shell \
		-a 'cd /opt/scripts/bitrix && /opt/scripts/.venv/bin/python3 insert.py 2>&1 | tee -a /opt/scripts/bitrix/logs; exit $${PIPESTATUS[0]}' \
		-e ansible_shell_executable=/bin/bash

deploy-yogile:
	ansible-playbook -i $(INVENTORY) ansible/yogile.yml --vault-password-file $(VAULT_PASS_FILE)

deploy-appmetrica:
	ansible-playbook -i $(INVENTORY) ansible/appmetrica.yml --vault-password-file $(VAULT_PASS_FILE)

encrypt:
	ansible-vault encrypt $(VAULT_FILE) --vault-password-file $(VAULT_PASS_FILE)

decrypt:
	ansible-vault decrypt $(VAULT_FILE) --vault-password-file $(VAULT_PASS_FILE)

# Получить OAuth-токен Яндекса: make token CLIENT_ID=<ClientID приложения с oauth.yandex.ru>
token:
ifndef CLIENT_ID
	$(error Укажи CLIENT_ID: make token CLIENT_ID=<ClientID с https://oauth.yandex.ru>)
endif
	@echo "Открой ссылку, подтверди доступ и скопируй токен:"
	@echo "https://oauth.yandex.ru/authorize?response_type=token&client_id=$(CLIENT_ID)"
	@open "https://oauth.yandex.ru/authorize?response_type=token&client_id=$(CLIENT_ID)" 2>/dev/null || true

# Проверить доступ токена к Метрике и AppMetrica: make token-check TOKEN=<токен>
token-check:
ifndef TOKEN
	$(error Укажи TOKEN: make token-check TOKEN=<токен>)
endif
	@printf "Metrika:    " && curl -s -o /dev/null -w "HTTP %{http_code}\n" -H "Authorization: OAuth $(TOKEN)" https://api-metrika.yandex.net/management/v1/counters
	@printf "AppMetrica: " && curl -s -o /dev/null -w "HTTP %{http_code}\n" -H "Authorization: OAuth $(TOKEN)" https://api.appmetrica.yandex.ru/management/v1/applications

.PHONY: deploy deploy-clickhouse deploy-scripts deploy-bitrix run-bitrix deploy-yogile deploy-appmetrica encrypt decrypt token token-check
