INVENTORY  = ansible/inventory.ini
VAULT_FILE = ansible/vars/vault.yml
VAULT_PASS_FILE = ~/.ansible_vault_pass

deploy:
	ansible-playbook -i $(INVENTORY) ansible/playbook.yml --vault-password-file $(VAULT_PASS_FILE)

deploy-clickhouse:
	ansible-playbook -i $(INVENTORY) ansible/clickhouse.yml --vault-password-file $(VAULT_PASS_FILE)

deploy-scripts:
	ansible-playbook -i $(INVENTORY) ansible/scripts.yml --vault-password-file $(VAULT_PASS_FILE)

encrypt:
	ansible-vault encrypt $(VAULT_FILE) --vault-password-file $(VAULT_PASS_FILE)

decrypt:
	ansible-vault decrypt $(VAULT_FILE) --vault-password-file $(VAULT_PASS_FILE)

.PHONY: deploy deploy-clickhouse deploy-scripts encrypt decrypt
