# streamedtom3u

HLS-Proxy für die [streamed.pk](https://streamed.pk/docs) API. Generiert eine
**M3U-Playlist mit lokalen Proxy-URLs**, die in Jellyfin (oder VLC / Kodi / OBS)
direkt als Live-TV-Quelle nutzbar ist.

## Warum dieser Umweg?

Die `streamed.pk` API liefert pro Match nur eine `embedUrl` – eine HTML-Seite
mit JW-Player-Bundle. Die echte `m3u8`-URL existiert nur kurzzeitig und ist
token-geschützt; die TS-Segmente brauchen den richtigen `Referer`-Header. Eine
direkte M3U mit den Embed-URLs funktioniert in Jellyfin **nicht**.

Dieser Server löst das, indem er:

1. pro angefragtem Stream einen Headless-Chromium-Tab öffnet,
2. den `m3u8`-Response-Body per Network-Interception abgreift,
3. relative TS-Pfade in `/seg?u=…`-Proxy-URLs umschreibt,
4. die TS-Segmente mit dem korrekten `Referer` durchreicht.

So bekommt Jellyfin am Ende einen sauberen, dauerhaft funktionierenden HLS-Strom.

## Schnellstart mit Docker (empfohlen)

Fertige Multi-Arch-Images (`linux/amd64` + `linux/arm64`) liegen auf
GitHub Container Registry:

```bash
docker run -d \
  --name streamedtom3u \
  --restart unless-stopped \
  --shm-size=1g \
  -p 8765:8765 \
  ghcr.io/tgru-dev/streamedtom3u:latest
```

`--shm-size=1g` ist wichtig: Chromium braucht mehr als die Docker-Defaults von 64 MB,
sonst crasht der Tab nach kurzer Zeit.

Oder mit `docker-compose.yml` (liegt im Repo):

```bash
curl -O https://raw.githubusercontent.com/tgru-dev/streamedtom3u/main/docker-compose.yml
docker compose up -d
docker compose logs -f
```

Update:

```bash
docker compose pull && docker compose up -d
```

## Setup ohne Docker (Entwicklung)

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/playwright install chromium
.venv/bin/python server.py        # läuft auf 0.0.0.0:8765
```

Optionaler Systemd-Service (auf einem Pi/NAS) am Ende.

## Was die Playlist enthält

Die Playlist ist auf **deutschen Fußball** gefiltert:

- Bundesliga (1)
- 2. Bundesliga
- 3. Liga
- WM (FIFA World Cup)
- EM (UEFA European Championship)

Pro Match wird **nur ein Eintrag** ausgegeben – die **erste `echo`-Source**, Stream `#1`.
Spiele ohne `echo`-Source oder ohne erkennbare Liga werden übersprungen.

> Die Liga-Erkennung läuft über Slugs in den `delta`/`admin`-Source-IDs
> (z. B. `live_germany-bundesliga_…`, `live_fifa-world-cup_…`). Wenn ein Match
> trotz erwarteter Liga nicht erscheint, hilft `/debug/football`.

## Endpoints

| Pfad | Zweck |
| --- | --- |
| `GET /playlist.m3u` | Gefilterte M3U für heute (live + heute geplant) |
| `GET /playlist.m3u?scope=live` | Nur aktuell laufende Spiele |
| `GET /stream/{source}/{id}/{n}.m3u8` | Live-Playlist für einen einzelnen Stream |
| `GET /streams/{source}/{id}` | Verfügbare Stream-Nummern eines Matches |
| `GET /debug/football` | Diagnose: alle Football-Matches + erkannte Liga + Echo-ID |
| `GET /seg?u=…` | TS-Segment-Proxy (intern) |

## Jellyfin einrichten

1. **Dashboard → Live-TV → Tuner hinzufügen → M3U-Tuner**
2. **Datei oder URL:** `http://<server-ip>:8765/playlist.m3u`
3. Speichern. Optional unter **TV-Programm** den XMLTV-Guide leer lassen
   (es gibt aktuell kein EPG).
4. Bibliothek aktualisieren – die Sender erscheinen unter **Live-TV**.

> Auf dem Server (Pi/NAS) muss Port `8765` aus dem LAN erreichbar sein.

## Wie es intern arbeitet

```
Client (Jellyfin) ─► /playlist.m3u            (statische M3U mit Match-Liste)
Client (Jellyfin) ─► /stream/echo/.../1.m3u8  (HLS Live-Playlist)
       │                ├─► öffnet Chromium-Tab auf embedsports.top
       │                ├─► sniffert m3u8 Response Body
       │                └─► rewrite: jede .ts-Zeile → /seg?u=<base64-url>
Client (Jellyfin) ─► /seg?u=…                 (TS-Segment)
                        └─► httpx GET upstream mit Referer https://embedsports.top/
```

Pro aktivem Stream bleibt **ein** Browser-Tab offen. Der Tab wird nach
`IDLE_CLOSE_SECONDS = 90` ohne Zugriff geschlossen. Die m3u8 wird maximal
`M3U8_MAX_AGE_SECONDS = 8` aus dem Cache geliefert; danach wird der Tab
neu geladen, damit ein frisches Live-Window vom Upstream kommt.

## Bekannte Einschränkungen

- **Brittle**: Wenn `streamed.pk` das Embed-Layout, das CDN oder den
  Token-Mechanismus ändert, bricht die Extraktion. Ein Update von
  `server.py` reicht meist; debuggen mit `LOG_LEVEL=DEBUG`.
- **Ressourcenhungrig**: jeder gleichzeitig zuschauende Client öffnet einen
  Chromium-Tab (~100 MB RAM). Für ein Familien-Setup okay, nicht für
  Multi-User-Hosting gedacht.
- **Kein EPG**: die API liefert kein Programm, daher reine "Channel"-Liste.
- **Nur `echo`-Stream #1**: pro Match ist genau ein Eintrag in der Playlist.
  `/streams/{source}/{id}` zeigt die anderen verfügbaren Streams; bei Bedarf
  `.../2.m3u8` etc. von Hand in eine eigene M3U eintragen.
- **Saisonpause**: zwischen Mai und August zeigt `/playlist.m3u` typischerweise
  nichts — Bundesliga pausiert, und WM/EM finden nur in bestimmten Jahren statt.
  `/debug/football` zeigt, was die API gerade liefert.
- **Liga-Erkennung ist heuristisch**: wenn `streamed.pk` einen neuen Slug für
  z. B. die 3. Liga einführt, taucht das Match nicht auf. Anpassbar in
  `LEAGUE_RULES` in `server.py`.

## Optional: systemd-Unit (Linux/NAS)

`/etc/systemd/system/streamedtom3u.service`

```
[Unit]
Description=streamedtom3u proxy
After=network-online.target

[Service]
WorkingDirectory=/opt/streamedtom3u
ExecStart=/opt/streamedtom3u/.venv/bin/python server.py
Restart=on-failure
Environment=PORT=8765

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable --now streamedtom3u
```

## Rechtlicher Hinweis

`streamed.pk` aggregiert Streams, deren Rechtmäßigkeit pro Sport-Event
unterschiedlich ist. Dieses Tool ändert daran nichts – es leitet nur das,
was die öffentliche API ohnehin frei ausliefert, in ein Jellyfin-taugliches
Format um. Eigenverantwortliche Nutzung im rechtlichen Rahmen deines Landes.
