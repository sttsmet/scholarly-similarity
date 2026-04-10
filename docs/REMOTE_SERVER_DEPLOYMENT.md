# Remote Server Deployment (Streamlit + systemd + Nginx)

This guide runs the app as a long-lived service on the server, keeps artifacts on the server filesystem, and exposes the UI safely via reverse proxy.

## Workspace Root

These commands assume the repository root is:

```bash
/home/stts/projects/scholarly-similarity
```

## 1) Start Streamlit as a systemd service

Install the service unit into systemd and enable it:

```bash
cd /home/stts/projects/scholarly-similarity
sudo cp deploy/systemd/scholarly-similarity-streamlit.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now scholarly-similarity-streamlit.service
sudo systemctl status scholarly-similarity-streamlit.service
```

Streamlit is configured to bind to `127.0.0.1:8501` when used with Nginx reverse proxy.

## 2) Install and enable Nginx reverse proxy

Install Nginx (Debian/Ubuntu):

```bash
sudo apt-get update
sudo apt-get install -y nginx
```

Enable and start Nginx:

```bash
sudo systemctl enable --now nginx
sudo systemctl status nginx
```

## 3) Link the Nginx site config

The provided Nginx config already uses `server_name _;` so it works as a catch-all for first-time deployment by server IP.
If you later attach a real DNS hostname, update `deploy/nginx/scholarly-similarity.conf` to use that hostname before enabling TLS.

```bash
cd /home/stts/projects/scholarly-similarity
sudo ln -sf /home/stts/projects/scholarly-similarity/deploy/nginx/scholarly-similarity.conf /etc/nginx/sites-available/scholarly-similarity.conf
sudo ln -sf /etc/nginx/sites-available/scholarly-similarity.conf /etc/nginx/sites-enabled/scholarly-similarity.conf
sudo rm -f /etc/nginx/sites-enabled/default
```

Test and reload Nginx:

```bash
sudo nginx -t
sudo systemctl reload nginx
```

## 4) Optional TLS (Certbot)

Only run this after DNS is pointing to the server and port 80 is reachable from the internet.
Replace `APP_HOSTNAME` below with your real domain name:

```bash
sudo apt-get install -y certbot python3-certbot-nginx
sudo certbot --nginx -d APP_HOSTNAME
```

## Access Modes

## A) Preferred: Reverse proxy mode

- Keep Streamlit on `127.0.0.1:8501`
- Nginx serves users on port `80` (or `443` once TLS is enabled)
- Browse from client: `http://SERVER_IP` or your hostname

## B) LAN-only direct Streamlit mode

Use this when no reverse proxy is desired on a trusted LAN.

1. Change `.streamlit/config.toml`:

```toml
[server]
headless = true
address = "0.0.0.0"
port = 8501

[browser]
gatherUsageStats = false
```

2. Restart service:

```bash
sudo systemctl restart scholarly-similarity-streamlit.service
```

3. Open from LAN client:

```text
http://SERVER_IP:8501
```

## C) SSH tunnel mode

Keep Streamlit bound to `127.0.0.1:8501` and tunnel from your client machine:

```bash
ssh -L 8501:127.0.0.1:8501 stts@SERVER_HOSTNAME_OR_IP
```

Then browse locally on your client:

```text
http://127.0.0.1:8501
```

## D) Public internet mode

Requirements:

- Public DNS name pointing to the server public IP, or
- Router/firewall port-forwarding/NAT from public IP to this server
- Inbound ports `80` (and `443` for TLS) opened

With reverse proxy configured, browse by server IP or hostname:

```text
http://SERVER_IP
```

If TLS is configured with Certbot, browse:

```text
https://YOUR_DOMAIN
```

## Troubleshooting

If the app gets stuck loading behind a proxy, first verify WebSocket proxy headers.

- Ensure Nginx includes:
  - `proxy_http_version 1.1`
  - `proxy_set_header Upgrade $http_upgrade`
  - `proxy_set_header Connection $connection_upgrade`
- Ensure the `map $http_upgrade $connection_upgrade` block exists in the active config.

Only for troubleshooting, test Streamlit with:

```bash
.venv/bin/python -m streamlit run src/ui/streamlit_app.py --server.enableWebsocketCompression=false
```

If needed, temporarily test with:

```bash
.venv/bin/python -m streamlit run src/ui/streamlit_app.py --server.enableCORS=false
```

Set `browser.serverAddress` to the external hostname only if needed for proxy troubleshooting.

Useful checks:

```bash
sudo journalctl -u scholarly-similarity-streamlit.service -f
sudo tail -f /var/log/nginx/error.log
sudo tail -f /var/log/nginx/access.log
```
