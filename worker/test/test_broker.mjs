import mqtt from "mqtt";
const URL = process.env.URL || "ws://localhost:8787/mqtt";
const B = "sis1a/g01/posto-centro/gel01";
let ok = 0, fail = 0;
const check = (n, c, i = "") => { console.log((c ? "PASS " : "FAIL ") + n + (c ? "" : " -> " + i)); c ? ok++ : fail++; };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const con = (o) => new Promise((res) => {
  const c = mqtt.connect(URL, { protocolVersion: 4, reconnectPeriod: 0, connectTimeout: 5000, ...o });
  c.on("connect", () => res({ c, ok: true })); c.on("error", (e) => res({ c, ok: false, e: e.message }));
  c.on("close", () => res({ c, ok: false, e: "close" }));
});
const sub = (c, t) => new Promise((r) => c.subscribe(t, { qos: 1 }, (e, g) => r(e ? [{ qos: 128, erro: e.message }] : g)));

let r = await con({ username: "gel01", password: "errada", clientId: "x1" });
check("senha errada recusada", !r.ok, r.e); r.c.end(true);
r = await con({ clientId: "x2" });
check("sem usuario recusado", !r.ok, r.e); r.c.end(true);

const painel = (await con({ username: "painel", password: "senha-painel", clientId: "painel-1" })).c;
const recebidas = [];
painel.on("message", (t, m, p) => recebidas.push({ t, m: m.toString(), retain: p.retain }));
const g = await sub(painel, "sis1a/g01/#");
check("painel assina sis1a/g01/#", g[0].qos === 0, JSON.stringify(g));
const g2 = await sub(painel, "sis1a/#");
check("painel nao pode assinar sis1a/# (fora da ACL)", g2[0].qos === 128, JSON.stringify(g2));

const dev = await con({ username: "gel01", password: "senha-gel01", clientId: "gel01",
  will: { topic: B + "/status", payload: "offline", retain: true, qos: 1 } });
check("gel01 conecta com Last Will", dev.ok, dev.e);
const cmds = [];
dev.c.on("message", (t, m) => cmds.push(m.toString()));
await sub(dev.c, B + "/comando");
dev.c.publish(B + "/status", "online", { retain: true, qos: 1 });
dev.c.publish(B + "/telemetria", JSON.stringify({ temp_c: 5.1 }), { qos: 1 });
await sleep(600);
check("painel recebe telemetria", recebidas.some((x) => x.t === B + "/telemetria"), JSON.stringify(recebidas));
painel.publish(B + "/comando", JSON.stringify({ id: "abc123", acao: "silenciar" }));
await sleep(500);
check("comando do painel chega no ESP32", cmds.length === 1, JSON.stringify(cmds));
dev.c.publish(B + "/comando", "falso");                 // gel01 nao pode publicar em comando
dev.c.publish("sis1a/g01/posto-centro/gel02/telemetria", "x"); // nem em outra geladeira
await sleep(500);
check("ACL: gel01 nao publica em comando", cmds.length === 1, JSON.stringify(cmds));
check("ACL: gel01 nao publica como gel02", !recebidas.some((x) => x.t.includes("gel02")));

const p2 = (await con({ username: "painel", password: "senha-painel", clientId: "painel-2" })).c;
const ret = [];
p2.on("message", (t, m, p) => ret.push({ t, m: m.toString(), retain: p.retain }));
await sub(p2, B + "/status");
await sleep(400);
check("retido: painel novo recebe status=online", ret.some((x) => x.m === "online" && x.retain), JSON.stringify(ret));

// queda abrupta: fecha o socket sem DISCONNECT -> Last Will
dev.c.stream.destroy();
await sleep(1500);
check("Last Will: status=offline ao cair", recebidas.some((x) => x.t === B + "/status" && x.m === "offline"), JSON.stringify(recebidas.slice(-3)));

const p3 = (await con({ username: "painel", password: "senha-painel", clientId: "painel-3" })).c;
const ret3 = []; p3.on("message", (t, m) => ret3.push(m.toString()));
await sub(p3, B + "/status"); await sleep(400);
check("status retido agora e offline", ret3.includes("offline"), JSON.stringify(ret3));

// DISCONNECT limpo nao dispara will
const d2 = await con({ username: "gel01", password: "senha-gel01", clientId: "gel01", will: { topic: B + "/status", payload: "offline", retain: true } });
d2.c.publish(B + "/status", "online", { retain: true }); await sleep(300);
const antes = recebidas.filter((x) => x.m === "offline").length;
d2.c.end(); await sleep(800);
check("DISCONNECT limpo nao publica will", recebidas.filter((x) => x.m === "offline").length === antes);

// mesmo client id derruba a sessao antiga
const a1 = await con({ username: "gel01", password: "senha-gel01", clientId: "gel01" });
let caiu = false; a1.c.on("close", () => (caiu = true));
const a2 = await con({ username: "gel01", password: "senha-gel01", clientId: "gel01" });
await sleep(500);
check("client id duplicado derruba a sessao antiga", caiu && a2.ok);
for (const c of [painel, p2, p3, a2.c]) c.end(true);
console.log(`\n${ok} passaram, ${fail} falharam`); process.exit(fail ? 1 : 0);
