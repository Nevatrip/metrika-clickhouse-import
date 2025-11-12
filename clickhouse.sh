#cloud-config
# vim: syntax=yaml

package_update: true
package_upgrade: true
packages:
  - curl
  - ufw
  - python3
  - python3-venv
  - python3-dev
  - gcc
  - cron

write_files:
  - path: /opt/clickhouse/.env
    content: |
      FIRST_DATE='2025-01-01'
      CLICKHOUSE_USER=default
      CLICKHOUSE_PASSWORD="__PASSWORD__"
      METRIKA_COUNTER="__METRIKA_COUNTER__"
      METRIKA_KEY="__METRIKA_KEY__"

  - path: /etc/clickhouse-server/users.d/default-password.xml
    content: |
      <clickhouse>
          <users>
              <default>
                  <password remove='1' />
                  <password_sha256_hex>__SHA256_PASSWORD__</password_sha256_hex>
              </default>
          </users>
      </clickhouse>
  - path: /etc/clickhouse-server/config.d/listen.xml
    content: |
      <clickhouse>
          <listen_host>0.0.0.0</listen_host>
      </clickhouse>
  - path: /etc/clickhouse-server/config.d/logs.xml
    content: |
      <clickhouse>
          <query_log>
              <ttl>21600</ttl>
          </query_log>
          <trace_log>
              <ttl>21600</ttl>
          </trace_log>
          <text_log>
              <ttl>21600</ttl>
          </text_log>
          <metric_log>
              <ttl>21600</ttl>
          </metric_log>
      </clickhouse>

runcmd:
  - mkdir -p /opt/clickhouse
  - curl -L "https://github.com/Nevatrip/metrika-clickhouse-import/tarball/main" >| /opt/clickhouse/import.tgz
  - tar -xzf /opt/clickhouse/import.tgz -C /opt/clickhouse --strip-components=1
  - /opt/clickhouse/install.sh

  - systemctl daemon-reload
  - systemctl enable clickhouse-server
  - systemctl start clickhouse-server

  - python3 -m venv /opt/clickhouse/.venv
  - /opt/clickhouse/.venv/bin/pip install -r /opt/clickhouse/requirements.txt
  - /opt/clickhouse/.venv/bin/python /opt/clickhouse/init.py

  - echo "0 0 * * * /opt/clickhouse/.venv/bin/python /opt/clickhouse/insert.py 1>> /opt/clickhouse/logs 2>> /opt/clickhouse/logs" | crontab -

  # Configure firewall
  - ufw allow 22/tcp  # Keep SSH access
  - ufw allow 8123/tcp # Clickhouse HTTP
  - ufw allow 9000/tcp # Clickhouse native protocol

  - echo "y" | ufw enable

