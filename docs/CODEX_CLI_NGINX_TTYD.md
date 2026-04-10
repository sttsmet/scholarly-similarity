# Codex CLI Through Nginx Without APIs

This setup exposes a logged-in `codex` CLI session to a remote client through Nginx.
It does not require OpenAI API keys.
It relies on:

- a normal local `codex` CLI login on the server
- `ttyd` to turn the terminal session into a browser/WebSocket app
- Nginx as the public reverse proxy

## When To Use This

Use this when:

- your account can use Codex CLI directly
- you do not have API access
- you want remote browser access to the CLI from another machine

Do not use this as an unauthenticated public shell.
Treat it like remote shell access.

## Server Assumptions

These files assume:

```bash
/home/stts/projects/scholarly-similarity
```

Adjust paths, usernames, and hostnames before enabling the service.

## 1) Install Codex CLI And Log In Once

Install Codex CLI on the server using your normal install method.
Then, as the same user that will run the service, log in once:

```bash
codex
```

or, if your local install needs an explicit login step:

```bash
codex login
```

Finish the browser or device-flow login.
The service reuses that same local login state from `CODEX_HOME`.

## 2) Install ttyd And Nginx

On Debian or Ubuntu:

```bash
sudo apt-get update
sudo apt-get install -y nginx ttyd
```

Enable Nginx:

```bash
sudo systemctl enable --now nginx
```

## 3) Install The ttyd Service

Copy the service unit:

```bash
cd /home/stts/projects/scholarly-similarity
sudo cp deploy/systemd/scholarly-similarity-codex-ttyd.service /etc/systemd/system/
sudo cp deploy/systemd/scholarly-similarity-codex-ttyd.env.example /etc/default/scholarly-similarity-codex-ttyd
```

Edit the environment file if needed:

```bash
sudo editor /etc/default/scholarly-similarity-codex-ttyd
```

Important settings:

- `WORKSPACE_ROOT`: repo path the terminal should open in
- `CODEX_HOME`: path containing the logged-in Codex CLI state
- `TTYD_PORT`: default backend port is `7681`
- `TTYD_CREDENTIALS`: optional built-in `ttyd` auth in `user:password` form

Start the service:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now scholarly-similarity-codex-ttyd.service
sudo systemctl status scholarly-similarity-codex-ttyd.service
```

## 4) Install The Nginx Site

Copy the provided site config:

```bash
cd /home/stts/projects/scholarly-similarity
sudo cp deploy/nginx/scholarly-similarity-codex-ttyd.conf /etc/nginx/sites-available/
```

Edit the hostname:

```bash
sudo editor /etc/nginx/sites-available/scholarly-similarity-codex-ttyd.conf
```

Set:

```nginx
server_name codex.example.com;
```

Enable the site:

```bash
sudo ln -sf /etc/nginx/sites-available/scholarly-similarity-codex-ttyd.conf /etc/nginx/sites-enabled/scholarly-similarity-codex-ttyd.conf
sudo nginx -t
sudo systemctl reload nginx
```

If you want this on a separate host from Streamlit, use a dedicated hostname such as:

```text
codex.example.com
```

That avoids path-prefix problems with terminal apps.

## 5) Optional TLS

After DNS points at the server:

```bash
sudo apt-get install -y certbot python3-certbot-nginx
sudo certbot --nginx -d codex.example.com
```

## 6) Connect From A Client

Open:

```text
http://codex.example.com
```

or, after TLS:

```text
https://codex.example.com
```

You should land in a web terminal already positioned in the repository root and running `codex`.

## Security Notes

This is remote shell access.
At minimum:

- run it as a dedicated low-privilege user
- use a dedicated hostname
- enable either `TTYD_CREDENTIALS`, upstream auth, a VPN, or an IP allowlist
- prefer TLS if it leaves your LAN
- never expose a privileged login shell to the public internet

If you already have SSO, VPN, or Cloudflare Access, put that in front of Nginx and leave `ttyd` bound to `127.0.0.1`.

## Troubleshooting

If the page opens but the terminal does not connect:

- verify the `scholarly-similarity-codex-ttyd.service` unit is running
- verify `ttyd` is listening on `127.0.0.1:7681`
- verify the Nginx site includes the WebSocket headers

Useful checks:

```bash
sudo systemctl status scholarly-similarity-codex-ttyd.service
sudo journalctl -u scholarly-similarity-codex-ttyd.service -f
sudo nginx -t
sudo tail -f /var/log/nginx/error.log
sudo tail -f /var/log/nginx/access.log
ss -ltnp | grep 7681
```

If the service starts but exits immediately:

- confirm `codex` is installed on the server
- confirm `ttyd` is installed on the server
- confirm the service user can run `codex` manually
- confirm the service user already completed CLI login
