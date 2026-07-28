# Oracle deployment

1. Copy this directory, `service/`, `.venv/`, `build/manifest.json`, and the
   two SQLite release assets to `/opt/lexora-lexicon`.
2. Create a locked-down `lexora` system user and install Python dependencies
   in the virtual environment.
3. Install `deploy/systemd/lexora-lexicon.service`, enable it, and start it.
4. Install Caddy and use `deploy/Caddyfile`; Cloudflare DNS should point the
   proxied `lexicon.12323456.xyz` record at the Oracle public IPv4.
5. Allow TCP 80/443 in the Oracle security list/NSG; keep TCP 22 restricted to
   the administrator's address.

The API never writes to the SQLite files and supports range requests through
the ASGI file response, so interrupted downloads can resume.
