# Publicar o Worker (1 vez) — ativa o broker MQTT

O D1, o KV e o Worker já estão criados na sua conta Cloudflare. Falta só este
deploy, porque o broker usa um Durable Object e o painel da Cloudflare não
consegue ativá-lo; o wrangler consegue.

Precisa do Node.js 18+ instalado (https://nodejs.org).

```bash
cd worker
npm install
npx wrangler login        # abre o navegador para autorizar a sua conta
npx wrangler deploy
```

Se o wrangler avisar que a configuração do painel é diferente da local, responda **y**
(a do arquivo `wrangler.toml` já tem o mesmo D1 e o mesmo KV, mais o broker e os crons).

No fim deve aparecer:

```
env.BROKER (Broker)      Durable Object
env.KV                   KV Namespace
env.DB (geladeira-db)    D1 Database
https://geladeira-vacinas.gabee-roggia.workers.dev
schedule: */5 * * * *
schedule: 0 6 * * *
```

Teste rápido: abra https://geladeira-vacinas.gabee-roggia.workers.dev/mqtt no navegador.
Deve aparecer "use WebSocket (subprotocolo mqtt)". Isso quer dizer que o broker está no ar.
