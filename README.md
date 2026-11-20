# Geladeira de Vacinas (IoT G0813 · Trabalho N2)

Monitor de geladeira de vacinas para posto de saúde. O ESP32 mede a temperatura (DHT22) e a porta (HC-SR04). Se a temperatura sair de 2–8 °C por mais de 5 min, ou se a porta ficar aberta por mais de 60 s, ele toca a sirene **sozinho, sem depender da internet**. Todas as leituras (1 por minuto) vão para um histórico no Cloudflare D1. Quando o Wi-Fi cai, as leituras ficam guardadas na flash e são reenviadas depois, para o posto conseguir provar que a vacina nunca saiu da faixa.

```
DHT22 + HC-SR04 ─► ESP32 (decide na borda) ─► sirene / LEDs
                     │ MQTT sobre wss 443 (TLS)          │ HTTPS 443 + token
                     ▼                                   ▼
        ┌──────────── Cloudflare Worker: geladeira-vacinas.gabee-roggia.workers.dev ───────────┐
        │  /mqtt  broker MQTT 3.1.1 (Durable Object: ACL, retain, Last Will)                 │
        │  /api/* histórico D1 · limites/usuários/cooldown no KV · cron offline · Telegram   │
        │  /      painel (MQTT wss + API)                                                    │
        └────────────────────────────────────────────────────────────────────────────────────┘
```

Wokwi: https://wokwi.com/projects/480357222904008705

## Estrutura

| Pasta | O que tem |
|---|---|
| `firmware/geladeira/` | Código do ESP32 (Arduino). `secrets.example.h` → copiar para `secrets.h` |
| `wokwi/` | `diagram.json` e `libraries.txt` para simular no Wokwi |
| `worker/` | Worker (API + cron), `schema.sql` do D1, painel em `public/index.html`, teste automático |
| `docs/` | Caderno de projeto e evidências dos testes |

## Como montar

**Wokwi:** crie um projeto ESP32, cole `geladeira.ino`, `config.h`, `certs.h`, `secrets.h`, e use o `diagram.json` e o `libraries.txt` desta pasta. O Wi-Fi é `Wokwi-GUEST` com senha vazia.

**Placa real:** mesmas ligações do `diagram.json`. O ECHO do HC-SR04 sai em 5 V: use um divisor (1 kΩ + 2 kΩ) antes do GPIO 18. O sensor fica dentro da geladeira, apontado para a porta: menos de 10 cm quer dizer porta fechada.

| Peça | GPIO |
|---|---|
| DHT22 (dados) | 15 |
| HC-SR04 TRIG / ECHO | 5 / 18 |
| Buzzer (sirene) | 26 |
| LED vermelho / verde | 25 / 27 |
| Botão silenciar (para o GND) | 14 |

## Como colocar no ar

1. **Broker:** já vem no Worker (`src/broker.js`, um Durable Object). O Pub/Sub da Cloudflare está em beta fechado, então o broker é nosso. Usuários e ACL ficam no KV, só com o hash da senha:
   - `mqtt:user:gel01`: publica em `sis1a/g01/posto-centro/gel01/{telemetria,estado,status,ack,alerta}` e assina `.../comando`
   - `mqtt:user:painel`: assina `sis1a/g01/#` e só publica em `sis1a/g01/+/+/comando`
   - `painel:sha256`: hash da senha do painel (a mesma do usuário MQTT `painel`)
2. **Worker** (veja também `worker/DEPLOY.md`):
   ```bash
   cd worker && npm install
   npx wrangler d1 create geladeira-db          # cole o id no wrangler.toml
   npx wrangler kv namespace create KV          # cole o id no wrangler.toml
   npm run db:init
   # token da geladeira: o Worker guarda só o hash
   TOKEN=$(openssl rand -hex 32); echo "DEVICE_TOKEN=$TOKEN  (vai no secrets.h)"
   npx wrangler kv key put --binding=KV --remote "dev:$(echo -n $TOKEN | sha256sum | cut -d' ' -f1)" gel01
   npx wrangler deploy
   ```
   Opcional: `TELEGRAM_TOKEN` e `TELEGRAM_CHAT_ID` como secrets para receber os alertas no celular.
3. **ESP32:** preencha `secrets.h` (Wi-Fi, broker, URL do Worker, token) e grave.
4. **Painel:** abra `https://geladeira-vacinas.gabee-roggia.workers.dev/` e entre com o usuário `painel` e a senha do painel. Ela não fica gravada no código.

## Como testar

- **Worker (automático):** `npx wrangler dev --local --test-scheduled` e, em outro terminal, `python3 test/test_worker.py` (antes, rode o schema com `--local` e registre o token de teste no KV com `--local`). São 24 verificações: token, validação, lote offline, duplicidade, resumo SQL, cooldown e limites. A saída está em `docs/teste_worker_saida.txt`.
- **Broker (automático):** `node test/test_broker.mjs` (precisa de `npm i mqtt`). São 14 verificações: senha errada, ACL de publicação e assinatura, retain, Last Will numa queda abrupta, DISCONNECT limpo e client id duplicado.
- **No Wokwi:**
  - Clique no DHT22 e suba a temperatura para 9 °C. Depois de 5 min a sirene toca, o LED fica vermelho e chega um alerta no painel. Volte para 5 °C e, após 1 min, o alarme desarma.
  - Mude a distância do HC-SR04 para 30 cm (porta aberta). Em 60 s, bip curto e alerta.
  - Desconecte o fio SDA do DHT. Em cerca de 6 s o alarme `sensor_falha` dispara (estado seguro).
  - No painel, clique em "Silenciar 10 min". A linha passa de ENVIADO para EXECUTADO quando chega o ack.
- **Ataque:** publique um comando falso sem `id`/`ts`, ou repita um `id` já usado. O ESP32 recusa e responde no `ack` com o motivo. Com o usuário `gel01` não é possível publicar no `comando` de outra geladeira (ACL).

## Segurança, em resumo

MQTT só em wss na 443 com TLS e certificado conferido (`beginSslWithCA`/`setCACert`, sem `setInsecure`). Cada dispositivo tem usuário e ACL próprios no broker. O token HTTP é por geladeira e o Worker guarda apenas o hash SHA-256 no KV. Todo comando passa por lista branca, `id` único e `ts` com no máximo 60 s. `secrets.h` e `.dev.vars` estão no `.gitignore` e nenhum segredo é impresso no Serial.
