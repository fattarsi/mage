# XMage Web Gateway (play vs AI)

A lightweight web client for XMage. It boots the real XMage server in-process
(no JBoss transporter) and exposes it over WebSocket/JSON, so you can play
Human-vs-AI games from the browser — no desktop client required.

Each browser connection gets its own game. The engine and all card
implementations are the stock XMage ones; this module only adds the transport
gateway (`Mage.Server.Web`) and a static web client (`src/main/resources/web/`).

## Run with Docker (easiest for others)

No JDK or Maven needed — just Docker. From the repo root:

```bash
docker compose up --build      # or: docker build -t xmage-web . && docker run --rm -p 8080:8080 xmage-web
```

Then open <http://localhost:8080/>. To enable a deck source (and Commander):

```bash
XMAGE_WEB_DECK_SOURCE_URL=https://your-deck-host docker compose up --build
```

> The first build compiles the full XMage card pool, so it is heavy — expect
> several minutes and a few GB of RAM. After that, Docker layer caching makes
> rebuilds fast unless sources change.

## Build (local toolchain)

If you prefer to build/run directly (JDK 17+, Maven), from the repo root:

```bash
mvn -q -pl Mage.Server.Web -am install -DskipTests
```

## Run

```bash
mvn -pl Mage.Server.Web exec:java -Dexec.mainClass=mage.server.web.WebServerMain
# then open http://localhost:8080/
```

> Note: `exec:java` serves the web client from `target/classes`, so re-run the
> build (`mvn -pl Mage.Server.Web install`) after editing files under
> `src/main/resources/web/` to redeploy them.

## Configuration

All configuration is external — nothing environment-specific is baked into the
code. Pass options as JVM system properties (`-Dname=value`):

| Property | Default | Purpose |
| --- | --- | --- |
| `xmage.web.port` | `8080` | HTTP/WebSocket port. |
| `xmage.config.path` | XMage default | Path to the server `config.xml` (e.g. `Mage.Server/config/config.xml`). |
| `xmage.web.deckSourceUrl` | _(unset)_ | Optional external deck source — see below. |

### Optional: external deck source

Without a deck source you can still play: paste a decklist or use a random deck.
Commander games, however, need real decks, so they require a deck source.

A deck source is a self-hosted "Deck Manager" REST API. Because the URL is
specific to your environment, it is **not** committed anywhere — configure it
per-deployment with either:

```bash
# system property
mvn -pl Mage.Server.Web exec:java -Dexec.mainClass=mage.server.web.WebServerMain \
    -Dxmage.web.deckSourceUrl=https://your-deck-host

# or environment variable
export XMAGE_WEB_DECK_SOURCE_URL=https://your-deck-host
```

The convenient, no-leak way to keep a personal URL out of git: copy `.env.example`
to `.env` (which is git-ignored) and set `XMAGE_WEB_DECK_SOURCE_URL` there.
`docker compose up` reads it automatically, and `./run-web.sh` (from the repo
root) loads it for local runs.

The expected REST shape (Django-style) is documented in
[`DeckManagerSource.java`](src/main/java/mage/server/web/DeckManagerSource.java):

- `GET /api/decks/` → `{results: [{id, name, commander:{name}, partner}]}`
- `GET /api/decks/{id}/` → `{name, commander, partner, cards: [{name, oracle_card:{set_code, collector_number}}]}`

To load decks from a different backend, implement the small `DeckSource`
interface and wire it up in `WebServerMain` instead of `DeckManagerSource`.
