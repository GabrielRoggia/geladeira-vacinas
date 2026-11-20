// Broker MQTT 3.1.1 dentro da Cloudflare (Durable Object), via WebSocket com TLS.
//
// Por que existe: o Cloudflare Pub/Sub (broker MQTT gerenciado) continua em beta
// fechado, so fala MQTT 5 e nao tem Last Will. Entao o broker roda aqui, no mesmo
// Worker do historico. Endpoint: wss://<worker>.workers.dev/mqtt (porta 443, TLS
// terminado pela Cloudflare). Subprotocolo WebSocket: "mqtt".
//
// Suporta: CONNECT com usuario/senha, PUBLISH QoS 0 e 1 (PUBACK), SUBSCRIBE com
// curingas + e #, UNSUBSCRIBE, PINGREQ, DISCONNECT, mensagens retidas (retain),
// Last Will (publicada se o cliente cair sem DISCONNECT ou estourar o keepalive),
// ACL por usuario (pub/sub) e troca de sessao com o mesmo client id.
// Entregas aos assinantes sao em QoS 0 (o SUBACK concede QoS 0).
//
// Usuarios ficam no KV: chave "mqtt:user:<nome>" =
//   {"sha256":"<hash da senha>","pub":["filtro",...],"sub":["filtro",...]}

const MAX_PACOTE = 8 * 1024;
const enc = new TextEncoder();
const dec = new TextDecoder();

async function sha256hex(t) {
  const b = await crypto.subtle.digest("SHA-256", enc.encode(t));
  return [...new Uint8Array(b)].map((x) => x.toString(16).padStart(2, "0")).join("");
}

// filtro MQTT (com + e #) casa com o topico?
export function casa(filtro, topico) {
  const f = filtro.split("/");
  const t = topico.split("/");
  for (let i = 0; i < f.length; i++) {
    if (f[i] === "#") return i !== 0 || !topico.startsWith("$");
    if (i >= t.length) return false;
    if (f[i] === "+") { if (i === 0 && t[0].startsWith("$")) return false; continue; }
    if (f[i] !== t[i]) return false;
  }
  return f.length === t.length;
}

// um filtro de assinatura pedido cabe dentro do que a ACL permite?
function filtroPermitido(pedido, permitidos) {
  if (permitidos.some((p) => p === pedido || p === "#")) return true;
  // pedido sem curinga: basta casar com algum permitido
  if (!pedido.includes("+") && !pedido.includes("#")) return permitidos.some((p) => casa(p, pedido));
  // pedido com curinga: so se algum permitido terminar em # e for prefixo dele
  return permitidos.some((p) => p.endsWith("/#") && pedido.startsWith(p.slice(0, -1)));
}

function filtroValido(f) {
  if (!f || f.length > 256) return false;
  const n = f.split("/");
  return n.every((x, i) => (x === "#" ? i === n.length - 1 : !x.includes("#") && (x === "+" || !x.includes("+"))));
}

// ---------- codificacao ----------
function tamanhoVar(n) {
  const out = [];
  do { let b = n % 128; n = Math.floor(n / 128); if (n > 0) b |= 128; out.push(b); } while (n > 0);
  return out;
}
function str(s) { const b = enc.encode(s); return [b.length >> 8, b.length & 255, ...b]; }
function pacote(cab, corpo) { return new Uint8Array([cab, ...tamanhoVar(corpo.length), ...corpo]); }
function pacotePublish(topico, payload, retain) {
  return pacote(0x30 | (retain ? 1 : 0), [...str(topico), ...payload]);
}

// ---------- decodificacao ----------
class Leitor {
  constructor(b) { this.b = b; this.i = 0; }
  u8() { if (this.i >= this.b.length) throw new Error("curto"); return this.b[this.i++]; }
  u16() { return (this.u8() << 8) | this.u8(); }
  bytes(n) { if (this.i + n > this.b.length) throw new Error("curto"); const r = this.b.slice(this.i, this.i + n); this.i += n; return r; }
  str() { return dec.decode(this.bytes(this.u16())); }
  resto() { return this.b.slice(this.i); }
  fim() { return this.i >= this.b.length; }
}

// separa pacotes completos de um buffer (um frame pode ter 0..n pacotes)
function separar(buf) {
  const pacotes = [];
  let i = 0;
  while (i < buf.length) {
    let mult = 1, tam = 0, j = i + 1, b;
    do {
      if (j >= buf.length) return { pacotes, sobra: buf.slice(i) };
      b = buf[j++]; tam += (b & 127) * mult; mult *= 128;
      if (mult > 128 ** 4) throw new Error("tamanho invalido");
    } while (b & 128);
    if (tam > MAX_PACOTE) throw new Error("pacote grande demais");
    if (j + tam > buf.length) return { pacotes, sobra: buf.slice(i) };
    pacotes.push({ tipo: buf[i] >> 4, flags: buf[i] & 15, corpo: buf.slice(j, j + tam) });
    i = j + tam;
  }
  return { pacotes, sobra: new Uint8Array(0) };
}

const b64 = (u8) => btoa(String.fromCharCode(...u8));
const deB64 = (s) => Uint8Array.from(atob(s), (c) => c.charCodeAt(0));

export class Broker {
  constructor(ctx, env) {
    this.ctx = ctx;
    this.env = env;
    this.sobras = new Map(); // bytes parciais por conexao (so em memoria)
  }

  async fetch(req) {
    if (req.headers.get("upgrade") !== "websocket") return new Response("use WebSocket (subprotocolo mqtt)", { status: 426 });
    const protos = (req.headers.get("sec-websocket-protocol") || "").split(",").map((s) => s.trim());
    const par = new WebSocketPair();
    this.ctx.acceptWebSocket(par[1]);
    par[1].serializeAttachment({ fase: "novo", desde: Date.now(), ultimo: Date.now() });
    await this.agendar();
    const h = new Headers();
    if (protos.includes("mqtt")) h.set("sec-websocket-protocol", "mqtt");
    return new Response(null, { status: 101, webSocket: par[0], headers: h });
  }

  async agendar() {
    if ((await this.ctx.storage.getAlarm()) == null) await this.ctx.storage.setAlarm(Date.now() + 15000);
  }

  // vigia keepalive: cliente calado por 1,5 x keepalive + FOLGA_S cai e dispara a Last Will.
  // A folga existe porque o Chrome segura os timers de uma aba em segundo plano
  // (o painel atras do Wokwi manda o PINGREQ so 1x por minuto) e o Wokwi lento
  // atrasa o ping do ESP32. Com keepalive 30 s, o cliente cai apos ~105 s calado.
  async alarm() {
    const FOLGA_S = 60;
    const agora = Date.now();
    const ws = this.ctx.getWebSockets();
    for (const s of ws) {
      const a = s.deserializeAttachment() || {};
      const limite = a.fase === "on" ? (Math.max(10, (a.ka || 60) * 1.5) + FOLGA_S) * 1000 : 30000; // 30 s para mandar o CONNECT
      if (agora - (a.ultimo || a.desde || 0) > limite) await this.derrubar(s, "keepalive estourado");
    }
    if (this.ctx.getWebSockets().length) await this.ctx.storage.setAlarm(Date.now() + 15000);
  }

  async webSocketMessage(ws, msg) {
    let dados = typeof msg === "string" ? enc.encode(msg) : new Uint8Array(msg);
    const sobra = this.sobras.get(ws);
    if (sobra?.length) { const j = new Uint8Array(sobra.length + dados.length); j.set(sobra); j.set(dados, sobra.length); dados = j; }
    let r;
    try { r = separar(dados); } catch (e) { return this.derrubar(ws, e.message); }
    this.sobras.set(ws, r.sobra);
    for (const p of r.pacotes) {
      try { await this.tratar(ws, p); } catch (e) { return this.derrubar(ws, "pacote invalido: " + e.message); }
      if (ws.readyState !== 1) return;
    }
  }

  async webSocketClose(ws) { await this.encerrar(ws, true); }
  async webSocketError(ws) { await this.encerrar(ws, true); }

  async derrubar(ws, motivo) {
    console.log("broker: desconectando", (ws.deserializeAttachment() || {}).cid, "-", motivo);
    await this.encerrar(ws, true);
    try { ws.close(1002, motivo.slice(0, 100)); } catch {}
  }

  // publica a Last Will se a saida nao foi um DISCONNECT limpo
  async encerrar(ws, publicarWill) {
    const a = ws.deserializeAttachment() || {};
    this.sobras.delete(ws);
    if (a.fase === "fim") return;
    ws.serializeAttachment({ ...a, fase: "fim" });
    if (publicarWill && a.will) await this.distribuir(a.will.t, deB64(a.will.p), a.will.r, ws);
  }

  enviar(ws, bytes) { try { ws.send(bytes); } catch {} }

  async tratar(ws, p) {
    const a = ws.deserializeAttachment() || {};
    const L = new Leitor(p.corpo);

    if (a.fase !== "on" && p.tipo !== 1) throw new Error("primeiro pacote deve ser CONNECT");

    switch (p.tipo) {
      case 1: { // CONNECT
        if (a.fase === "on") throw new Error("CONNECT repetido");
        const proto = L.str(), nivel = L.u8(), fl = L.u8(), ka = L.u16();
        if (proto !== "MQTT" || nivel !== 4) { this.enviar(ws, pacote(0x20, [0, 1])); return this.derrubar(ws, "so MQTT 3.1.1"); }
        const cid = L.str() || "anon-" + crypto.randomUUID().slice(0, 8);
        let will = null;
        if (fl & 4) {
          const t = L.str(); const pl = L.bytes(L.u16());
          will = { t, p: b64(pl), r: !!(fl & 32) };
        }
        const user = fl & 128 ? L.str() : "";
        const senha = fl & 64 ? L.str() : "";
        const cad = user ? await this.env.KV.get("mqtt:user:" + user, "json") : null;
        if (!cad || !senha || cad.sha256 !== (await sha256hex(senha))) {
          this.enviar(ws, pacote(0x20, [0, 4])); // usuario ou senha errados
          return this.derrubar(ws, "autenticacao recusada para '" + user + "'");
        }
        const acl = { pub: cad.pub || [], sub: cad.sub || [] };
        if (will && !acl.pub.some((f) => casa(f, will.t))) { this.enviar(ws, pacote(0x20, [0, 5])); return this.derrubar(ws, "will fora da ACL"); }
        // mesmo client id ja conectado: a sessao antiga cai (regra do MQTT)
        for (const o of this.ctx.getWebSockets()) {
          if (o !== ws && (o.deserializeAttachment() || {}).cid === cid) await this.derrubar(o, "sessao assumida por nova conexao");
        }
        ws.serializeAttachment({ fase: "on", cid, user, acl, subs: [], will, ka: ka || 60, ultimo: Date.now() });
        this.enviar(ws, pacote(0x20, [0, 0]));
        console.log("broker: conectado", cid, "usuario", user);
        return;
      }
      case 3: { // PUBLISH
        const qos = (p.flags >> 1) & 3, retain = !!(p.flags & 1);
        const topico = L.str();
        const id = qos > 0 ? L.u16() : 0;
        const payload = L.resto();
        if (qos === 2) throw new Error("QoS 2 nao suportado");
        if (!topico || topico.includes("+") || topico.includes("#")) throw new Error("topico invalido");
        this.toque(ws, a);
        if (qos === 1) this.enviar(ws, pacote(0x40, [id >> 8, id & 255]));
        if (!a.acl.pub.some((f) => casa(f, topico))) {
          console.log("broker: PUBLISH negado pela ACL", a.user, topico); // descarta (como o Mosquitto)
          return;
        }
        await this.distribuir(topico, payload, retain, null);
        return;
      }
      case 4: this.toque(ws, a); return; // PUBACK de algo nosso (entregamos em QoS 0)
      case 8: { // SUBSCRIBE
        const id = L.u16();
        const codigos = [];
        const novos = [];
        while (!L.fim()) {
          const f = L.str(); L.u8();
          if (filtroValido(f) && filtroPermitido(f, a.acl.sub)) { codigos.push(0); novos.push(f); }
          else { codigos.push(0x80); console.log("broker: SUBSCRIBE negado", a.user, f); }
        }
        const subs = [...new Set([...(a.subs || []), ...novos])];
        ws.serializeAttachment({ ...a, subs, ultimo: Date.now() });
        this.enviar(ws, pacote(0x90, [id >> 8, id & 255, ...codigos]));
        // entrega as mensagens retidas que casam
        if (novos.length) {
          const ret = await this.ctx.storage.list({ prefix: "ret:" });
          for (const [k, v] of ret) {
            const t = k.slice(4);
            if (novos.some((f) => casa(f, t))) this.enviar(ws, pacotePublish(t, deB64(v), true));
          }
        }
        return;
      }
      case 10: { // UNSUBSCRIBE
        const id = L.u16();
        const fora = [];
        while (!L.fim()) fora.push(L.str());
        ws.serializeAttachment({ ...a, subs: (a.subs || []).filter((s) => !fora.includes(s)), ultimo: Date.now() });
        this.enviar(ws, pacote(0xb0, [id >> 8, id & 255]));
        return;
      }
      case 12: this.toque(ws, a); this.enviar(ws, new Uint8Array([0xd0, 0])); return; // PINGREQ
      case 14: // DISCONNECT limpo: sem Last Will
        ws.serializeAttachment({ ...a, will: null });
        await this.encerrar(ws, false);
        try { ws.close(1000, "tchau"); } catch {}
        return;
      default:
        throw new Error("tipo " + p.tipo + " nao suportado");
    }
  }

  toque(ws, a) {
    // atualiza o "visto por ultimo" no maximo a cada 5 s (economiza escrita)
    if (Date.now() - (a.ultimo || 0) > 5000) ws.serializeAttachment({ ...a, ultimo: Date.now() });
  }

  async distribuir(topico, payload, retain, excluir) {
    if (retain) {
      if (payload.length === 0) await this.ctx.storage.delete("ret:" + topico);
      else await this.ctx.storage.put("ret:" + topico, b64(payload));
    }
    const pkt = pacotePublish(topico, payload, false);
    for (const s of this.ctx.getWebSockets()) {
      if (s === excluir) continue;
      const a = s.deserializeAttachment() || {};
      if (a.fase === "on" && (a.subs || []).some((f) => casa(f, topico))) this.enviar(s, pkt);
    }
  }
}
