// Worker da Geladeira de Vacinas (Cloudflare Workers + D1 + KV)
//
// Rotas do DISPOSITIVO (token proprio de cada geladeira, guardado no KV como hash):
//   POST /api/leituras   uma leitura ou um lote (fila offline)       -> D1 + KV
//   POST /api/alertas    alerta do ESP32, cooldown no KV -> Telegram  -> D1
//   GET  /api/config     limites atuais (o ESP32 le a cada 10 min)    <- KV
// Rotas do PAINEL (PAINEL_TOKEN):
//   GET  /api/ultimo  /api/leituras  /api/resumo  /api/alertas  /api/dispositivos
//   PUT  /api/config     ajusta limites remotamente
// Broker MQTT:
//   wss://.../mqtt   MQTT 3.1.1 sobre WebSocket (ver broker.js)
// Cron:
//   */5 min  dispositivo sem leitura ha OFFLINE_MIN -> alerta "offline" (regra de nuvem)
//   diario   apaga leituras mais velhas que RETENCAO_DIAS

export { Broker } from "./broker.js";
import PAINEL_HTML from "./painel.html";

const PORTAS = new Set(["fechada", "aberta", "desconhecida"]);
const ALARMES = new Set(["normal", "temp_alta", "temp_baixa", "sensor_falha"]);
const TIPOS_ALERTA = new Set([
  "temp_alta", "temp_baixa", "sensor_falha", "porta_aberta", "porta_sensor",
  "normalizado", "fila_cheia",
]);
const CONFIG_PADRAO = { lim_min: 2.0, lim_max: 8.0, hist: 0.5, atraso_s: 300, porta_s: 60 };
const RE_DISP = /^[a-z0-9-]{1,32}$/;

const agora = () => Math.floor(Date.now() / 1000);

function json(dados, status = 200) {
  return new Response(JSON.stringify(dados), {
    status,
    headers: { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" },
  });
}
const erro = (status, msg) => json({ erro: msg }, status);

async function sha256hex(texto) {
  const buf = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(texto));
  return [...new Uint8Array(buf)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

// Comparacao em tempo constante (evita descobrir o token pelo tempo de resposta)
function iguais(a, b) {
  if (typeof a !== "string" || typeof b !== "string" || a.length !== b.length) return false;
  let r = 0;
  for (let i = 0; i < a.length; i++) r |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return r === 0;
}

function bearer(req) {
  const h = req.headers.get("authorization") || "";
  return h.startsWith("Bearer ") ? h.slice(7).trim() : "";
}

// Dispositivo: token -> sha256 -> KV "dev:<hash>" = disp. Sem registro = 401.
async function autenticarDispositivo(req, env) {
  const tok = bearer(req);
  if (tok.length < 32) return null;
  return await env.KV.get("dev:" + (await sha256hex(tok)));
}

// Painel: a mesma senha do usuario MQTT "painel". O Worker guarda so o hash no KV
// ("painel:sha256"); PAINEL_TOKEN (wrangler secret) tambem e aceito, se existir.
async function autenticarPainel(req, env) {
  const tok = bearer(req);
  if (tok.length < 16) return false;
  if (env.PAINEL_TOKEN && iguais(tok, env.PAINEL_TOKEN)) return true;
  const hash = await env.KV.get("painel:sha256");
  return !!hash && iguais(await sha256hex(tok), hash);
}

function numeroOuNull(v, min, max) {
  if (v === null || v === undefined) return null;
  const n = Number(v);
  if (!Number.isFinite(n) || n < min || n > max) throw new Error("fora da faixa");
  return n;
}

function validarLeitura(l, disp, t) {
  if (typeof l !== "object" || l === null) throw new Error("leitura nao e objeto");
  if (l.disp !== disp) throw new Error("disp nao confere com o token");
  const seq = Number(l.seq);
  if (!Number.isInteger(seq) || seq < 0) throw new Error("seq invalido");
  let ts = Number(l.ts);
  let origem = 0;
  if (!Number.isInteger(ts)) throw new Error("ts invalido");
  if (ts === 0) { ts = t; origem = 2; }                       // ESP32 sem NTP: hora estimada
  else if (ts > t + 300 || ts < t - 30 * 86400) throw new Error("ts fora da janela");
  else if (t - ts > 120) origem = 1;                           // veio da fila offline
  const porta = String(l.porta || "");
  const alarme = String(l.alarme || "");
  if (!PORTAS.has(porta) || !ALARMES.has(alarme)) throw new Error("porta/alarme invalido");
  return {
    disp, seq, ts, origem, porta, alarme,
    temp_c: numeroOuNull(l.temp_c, -40, 80),
    umid_pct: numeroOuNull(l.umid_pct, 0, 100),
    mudo: l.mudo ? 1 : 0,
  };
}

const validarConfig = (c) =>
  c && Number.isFinite(c.lim_min) && Number.isFinite(c.lim_max) && Number.isFinite(c.hist) &&
  c.lim_min >= 0 && c.lim_max <= 15 && c.lim_min < c.lim_max &&
  c.hist >= 0.1 && c.hist <= 2 && c.lim_max - c.lim_min > 2 * c.hist &&
  Number.isInteger(c.atraso_s) && c.atraso_s >= 30 && c.atraso_s <= 1800 &&
  Number.isInteger(c.porta_s) && c.porta_s >= 15 && c.porta_s <= 600;

async function lerConfig(env, disp) {
  const salvo = await env.KV.get("config:" + disp, "json");
  return { ...CONFIG_PADRAO, ...(salvo || {}) };
}

async function telegram(env, texto) {
  if (!env.TELEGRAM_TOKEN || !env.TELEGRAM_CHAT_ID) return false;
  const r = await fetch(`https://api.telegram.org/bot${env.TELEGRAM_TOKEN}/sendMessage`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ chat_id: env.TELEGRAM_CHAT_ID, text: texto }),
  });
  return r.ok;
}

// Registra o alerta no D1 e notifica respeitando o cooldown (chave com TTL no KV)
async function registrarAlerta(env, disp, tipo, valor, msg, ts) {
  const cd = Number(env.COOLDOWN_S || 900);
  const chave = `cd:${disp}:${tipo}`;
  let notificado = 0;
  if (!(await env.KV.get(chave))) {
    await env.KV.put(chave, String(agora()), { expirationTtl: Math.max(60, cd) });
    const icone = tipo === "normalizado" ? "OK" : "ALERTA";
    notificado = (await telegram(env, `[${icone}] ${disp}: ${msg}${valor != null ? ` (${valor})` : ""}`)) ? 1 : 0;
  }
  await env.DB.prepare(
    "INSERT INTO alertas (disp, ts, tipo, valor, msg, notificado, recebido_em) VALUES (?,?,?,?,?,?,?)"
  ).bind(disp, ts, tipo, valor, msg, notificado, agora()).run();
  return notificado;
}

// ------------------------------------------------------------------
async function postLeituras(req, env) {
  const disp = await autenticarDispositivo(req, env);
  if (!disp) return erro(401, "token do dispositivo invalido");
  const txt = await req.text();
  if (txt.length > 64 * 1024) return erro(413, "lote grande demais");
  let corpo;
  try { corpo = JSON.parse(txt); } catch { return erro(400, "JSON invalido"); }
  const lista = Array.isArray(corpo) ? corpo : [corpo];
  if (lista.length === 0 || lista.length > 100) return erro(400, "envie de 1 a 100 leituras");

  const t = agora();
  let validas;
  try { validas = lista.map((l) => validarLeitura(l, disp, t)); }
  catch (e) { return erro(400, "leitura rejeitada: " + e.message); }

  const st = env.DB.prepare(
    `INSERT OR IGNORE INTO leituras (disp, seq, ts, temp_c, umid_pct, porta, alarme, mudo, origem, recebido_em)
     VALUES (?,?,?,?,?,?,?,?,?,?)`
  );
  const res = await env.DB.batch(
    validas.map((v) => st.bind(v.disp, v.seq, v.ts, v.temp_c, v.umid_pct, v.porta, v.alarme, v.mudo, v.origem, t))
  );
  const gravadas = res.reduce((s, r) => s + (r.meta?.changes || 0), 0);

  const ultima = validas.reduce((a, b) => (b.ts >= a.ts ? b : a));
  await env.KV.put("ultimo:" + disp, JSON.stringify({ ...ultima, recebido_em: t }));
  return json({ ok: true, recebidas: validas.length, gravadas, duplicadas: validas.length - gravadas });
}

async function postAlerta(req, env) {
  const disp = await autenticarDispositivo(req, env);
  if (!disp) return erro(401, "token do dispositivo invalido");
  let a;
  try { a = await req.json(); } catch { return erro(400, "JSON invalido"); }
  if (a.disp !== disp) return erro(400, "disp nao confere com o token");
  if (!TIPOS_ALERTA.has(a.tipo)) return erro(400, "tipo de alerta desconhecido");
  let valor;
  try { valor = numeroOuNull(a.valor, -100, 1000); } catch { return erro(400, "valor invalido"); }
  const msg = String(a.msg || a.tipo).slice(0, 200);
  const ts = Number.isInteger(a.ts) && a.ts > 0 ? a.ts : agora();
  const notificado = await registrarAlerta(env, disp, a.tipo, valor, msg, ts);
  return json({ ok: true, notificado: !!notificado, cooldown: !notificado });
}

async function getConfig(req, env, url) {
  const dispTok = await autenticarDispositivo(req, env);
  const painel = await autenticarPainel(req, env);
  const disp = url.searchParams.get("disp") || dispTok;
  if (!disp || !RE_DISP.test(disp)) return erro(400, "disp invalido");
  if (!painel && dispTok !== disp) return erro(401, "nao autorizado");
  return json(await lerConfig(env, disp));
}

async function putConfig(req, env, url) {
  if (!(await autenticarPainel(req, env))) return erro(401, "nao autorizado");
  const disp = url.searchParams.get("disp");
  if (!disp || !RE_DISP.test(disp)) return erro(400, "disp invalido");
  let c;
  try { c = await req.json(); } catch { return erro(400, "JSON invalido"); }
  const nova = { ...(await lerConfig(env, disp)), ...c };
  for (const k of ["lim_min", "lim_max", "hist", "atraso_s", "porta_s"]) nova[k] = Number(nova[k]);
  if (!validarConfig(nova)) return erro(400, "limites fora do aceitavel (0-15 C, histerese 0,1-2 C, atraso 30-1800 s)");
  const limpa = Object.fromEntries(Object.keys(CONFIG_PADRAO).map((k) => [k, nova[k]]));
  await env.KV.put("config:" + disp, JSON.stringify(limpa));
  return json({ ok: true, config: limpa });
}

function parametroDisp(url) {
  const disp = url.searchParams.get("disp") || "gel01";
  if (!RE_DISP.test(disp)) throw new Error("disp invalido");
  return disp;
}

async function getUltimo(env, url) {
  const disp = parametroDisp(url);
  const u = await env.KV.get("ultimo:" + disp, "json");
  if (!u) return erro(404, "sem leituras ainda");
  const idade = agora() - u.recebido_em;
  return json({ ...u, idade_s: idade, offline: idade > Number(env.OFFLINE_MIN || 10) * 60 });
}

async function getLeituras(env, url) {
  const disp = parametroDisp(url);
  const horas = Math.min(168, Math.max(1, Number(url.searchParams.get("horas")) || 6));
  const { results } = await env.DB.prepare(
    `SELECT ts, temp_c, umid_pct, porta, alarme, origem FROM leituras
     WHERE disp = ? AND ts >= ? ORDER BY ts LIMIT 12000`
  ).bind(disp, agora() - horas * 3600).all();
  return json(results);
}

// CONSULTA DE RESUMO: "A vacina saiu de 2-8 C nas ultimas N horas? Quando, quanto, e ha buracos?"
async function getResumo(env, url) {
  const disp = parametroDisp(url);
  const horas = Math.min(720, Math.max(1, Number(url.searchParams.get("horas")) || 24));
  const cfg = await lerConfig(env, disp);
  const desde = agora() - horas * 3600;
  const porHora = await env.DB.prepare(
    `SELECT strftime('%Y-%m-%d %H:00', ts, 'unixepoch', '-3 hours') AS hora,
            COUNT(*)                         AS leituras,
            ROUND(MIN(temp_c), 1)            AS min_c,
            ROUND(MAX(temp_c), 1)            AS max_c,
            ROUND(AVG(temp_c), 2)            AS media_c,
            SUM(temp_c < ?1 OR temp_c > ?2)  AS min_fora_faixa,
            SUM(temp_c IS NULL)              AS min_sensor_falha,
            SUM(porta = 'aberta')            AS min_porta_aberta
       FROM leituras
      WHERE disp = ?3 AND ts >= ?4
      GROUP BY hora
      ORDER BY hora`
  ).bind(cfg.lim_min, cfg.lim_max, disp, desde).all();

  const total = await env.DB.prepare(
    `SELECT COUNT(*) AS leituras, ROUND(MIN(temp_c),1) AS min_c, ROUND(MAX(temp_c),1) AS max_c,
            ROUND(AVG(temp_c),2) AS media_c,
            SUM(temp_c < ?1 OR temp_c > ?2) AS min_fora_faixa,
            SUM(temp_c IS NULL) AS min_sensor_falha,
            SUM(origem = 1) AS vindas_da_fila
       FROM leituras WHERE disp = ?3 AND ts >= ?4`
  ).bind(cfg.lim_min, cfg.lim_max, disp, desde).first();

  const esperado = horas * 60; // 1 leitura por minuto
  return json({
    disp, horas, faixa: [cfg.lim_min, cfg.lim_max],
    total: { ...total, esperado, cobertura_pct: Math.round((1000 * (total.leituras || 0)) / esperado) / 10 },
    sempre_na_faixa: (total.leituras || 0) > 0 && !total.min_fora_faixa && !total.min_sensor_falha,
    por_hora: porHora.results,
  });
}

async function getAlertas(env, url) {
  const disp = parametroDisp(url);
  const dias = Math.min(90, Math.max(1, Number(url.searchParams.get("dias")) || 7));
  const { results } = await env.DB.prepare(
    `SELECT ts, tipo, valor, msg, notificado FROM alertas WHERE disp = ? AND ts >= ? ORDER BY ts DESC LIMIT 200`
  ).bind(disp, agora() - dias * 86400).all();
  return json(results);
}

async function getDispositivos(env) {
  const lista = await env.KV.list({ prefix: "ultimo:" });
  const t = agora();
  const out = [];
  for (const k of lista.keys) {
    const u = await env.KV.get(k.name, "json");
    if (u) out.push({ disp: u.disp, temp_c: u.temp_c, alarme: u.alarme, idade_s: t - u.recebido_em });
  }
  return json(out);
}

// ------------------------------------------------------------------
async function rotear(req, env) {
  const url = new URL(req.url);
  const p = url.pathname;
  const m = req.method;

  // Broker MQTT (WebSocket + TLS) num Durable Object unico do grupo
  if (p === "/mqtt") return env.BROKER.get(env.BROKER.idFromName("g01")).fetch(req);

  if (p === "/api/leituras" && m === "POST") return postLeituras(req, env);
  if (p === "/api/alertas" && m === "POST") return postAlerta(req, env);
  if (p === "/api/config" && m === "GET") return getConfig(req, env, url);
  if (p === "/api/config" && m === "PUT") return putConfig(req, env, url);

  if (m === "GET" && p.startsWith("/api/")) {
    if (!(await autenticarPainel(req, env))) return erro(401, "nao autorizado");
    try {
      if (p === "/api/ultimo") return await getUltimo(env, url);
      if (p === "/api/leituras") return await getLeituras(env, url);
      if (p === "/api/resumo") return await getResumo(env, url);
      if (p === "/api/alertas") return await getAlertas(env, url);
      if (p === "/api/dispositivos") return await getDispositivos(env);
    } catch (e) {
      if (e.message === "disp invalido") return erro(400, e.message);
      throw e;
    }
  }
  if (p.startsWith("/api/")) return erro(404, "rota nao encontrada");
  if (p === "/" || p === "/index.html") {
    return new Response(PAINEL_HTML, {
      headers: {
        "content-type": "text/html; charset=utf-8",
        "content-security-policy": "default-src 'self'; script-src 'self' 'unsafe-inline' https://unpkg.com https://cdn.jsdelivr.net; style-src 'unsafe-inline'; connect-src 'self' wss:",
        "x-frame-options": "DENY",
      },
    });
  }
  return erro(404, "nao encontrado");
}

async function vigiaOffline(env) {
  const limite = Number(env.OFFLINE_MIN || 10) * 60;
  const t = agora();
  const lista = await env.KV.list({ prefix: "ultimo:" });
  for (const k of lista.keys) {
    const u = await env.KV.get(k.name, "json");
    if (u && t - u.recebido_em > limite) {
      const min = Math.round((t - u.recebido_em) / 60);
      await registrarAlerta(env, u.disp, "offline", null, `sem dados ha ${min} min (Wi-Fi, energia ou ESP32)`, t);
    }
  }
}

async function limpeza(env) {
  const dias = Number(env.RETENCAO_DIAS || 1825);
  const corte = agora() - dias * 86400;
  await env.DB.prepare("DELETE FROM leituras WHERE ts < ?").bind(corte).run();
  await env.DB.prepare("DELETE FROM alertas WHERE ts < ?").bind(corte).run();
}

export default {
  async fetch(req, env) {
    try {
      return await rotear(req, env);
    } catch (e) {
      console.error("erro interno:", e.message);   // nunca loga token nem corpo inteiro
      return erro(500, "erro interno");
    }
  },
  async scheduled(evento, env, ctx) {
    if (evento.cron === "0 6 * * *") ctx.waitUntil(limpeza(env));
    else ctx.waitUntil(vigiaOffline(env));
  },
};
