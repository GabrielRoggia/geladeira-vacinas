/*
  Geladeira de Vacinas - ESP32
  IoT G0813 - Trabalho N2

  Arquitetura do firmware (duas tarefas do FreeRTOS):
   - tarefaControle (nucleo 1, prioridade 2): SENTE, DECIDE e AGE. Nunca espera a rede.
     O alarme local funciona mesmo sem Wi-Fi.
   - tarefaRede (nucleo 1, prioridade 1): Wi-Fi, MQTT/TLS, HTTPS para o Worker e a fila
     offline em LittleFS. Se a internet cair, as leituras ficam na fila e
     sao reenviadas em lote quando ela voltar.

  Broker MQTT: roda na propria Cloudflare (Durable Object no Worker), acessado
  por MQTT 3.1.1 sobre WebSocket com TLS: wss://<worker>/mqtt, porta 443.

  Bibliotecas (Gerenciador de Bibliotecas / libraries.txt no Wokwi):
   - WebSockets (Markus Sattler) + MQTTPubSubClient (hideakitai)
   - ArduinoJson 7.x (Benoit Blanchon)
   - DHT sensor library (Adafruit) + Adafruit Unified Sensor
*/
#include <WiFi.h>
#include <WiFiClientSecure.h>
#include <HTTPClient.h>
#include <WebSocketsClient.h>   // incluir antes do MQTTPubSubClient
#include <MQTTPubSubClient.h>
#include <ArduinoJson.h>
#include <DHT.h>
#include <LittleFS.h>
#include <Preferences.h>
#include <time.h>

#include "config.h"
#include "secrets.h"   // copie de secrets.example.h (fora do Git)
#include "certs.h"

// ============================================================
//  Tipos e estado compartilhado
// ============================================================
struct Config {
  float lim_min = LIM_MIN_C;
  float lim_max = LIM_MAX_C;
  float hist = HISTERESE_C;
  uint32_t atraso_s = ATRASO_S;
  uint32_t porta_s = PORTA_MAX_S;
};

enum Alarme { AL_NORMAL = 0, AL_ALTA, AL_BAIXA, AL_SENSOR };
const char* nomeAlarme(Alarme a) {
  switch (a) {
    case AL_ALTA:   return "temp_alta";
    case AL_BAIXA:  return "temp_baixa";
    case AL_SENSOR: return "sensor_falha";
    default:        return "normal";
  }
}

struct Estado {
  float temp_c = NAN;
  float umid_pct = NAN;
  float dist_cm = -1;
  bool porta_aberta = false;
  bool porta_ok = true;
  bool alerta_porta = false;
  Alarme alarme = AL_NORMAL;
  uint32_t mudo_ate_ms = 0;
  uint32_t seq = 0;
  uint32_t fila_bytes = 0;
  bool online = false;
};

Config cfg;
Estado est;
SemaphoreHandle_t mtx;               // protege cfg e est
QueueHandle_t filaTelemetria;        // controle -> rede (linha JSON)
QueueHandle_t filaAlerta;            // controle -> rede (JSON do alerta)
QueueHandle_t filaCmd;               // rede -> controle (comando recebido)
QueueHandle_t filaAck;               // controle -> rede (ack do comando)
volatile bool estadoMudou = true;    // pede publicacao do estado (retained)
volatile bool pedirConfig = false;   // comando "recarregar_config"
volatile uint32_t testeAte_ms = 0;   // comando "testar_sirene"

typedef struct { char txt[320]; } Msg;

Preferences prefs;
DHT dht(PIN_DHT, DHT22);

// ============================================================
//  Utilidades
// ============================================================
bool relogioOk() { return time(nullptr) > 1700000000; }
uint32_t agoraEpoch() { return relogioOk() ? (uint32_t)time(nullptr) : 0; }
bool passou(uint32_t desde_ms, uint32_t ms) { return (uint32_t)(millis() - desde_ms) >= ms; }

void enviarFila(QueueHandle_t q, const String& s) {
  Msg m;
  strlcpy(m.txt, s.c_str(), sizeof(m.txt));
  xQueueSend(q, &m, 0);  // nao bloqueia o controle se a fila estiver cheia
}

void salvarConfig() {
  prefs.putFloat("min", cfg.lim_min);
  prefs.putFloat("max", cfg.lim_max);
  prefs.putFloat("hist", cfg.hist);
  prefs.putUInt("atraso", cfg.atraso_s);
  prefs.putUInt("porta", cfg.porta_s);
}

void carregarConfig() {
  cfg.lim_min = prefs.getFloat("min", LIM_MIN_C);
  cfg.lim_max = prefs.getFloat("max", LIM_MAX_C);
  cfg.hist = prefs.getFloat("hist", HISTERESE_C);
  cfg.atraso_s = prefs.getUInt("atraso", ATRASO_S);
  cfg.porta_s = prefs.getUInt("porta", PORTA_MAX_S);
}

// Config remota so e aceita se fizer sentido (evita "limite 50 C" por engano/ataque)
bool configValida(const Config& c) {
  return c.lim_min >= 0 && c.lim_max <= 15 && c.lim_min < c.lim_max &&
         c.hist >= 0.1f && c.hist <= 2.0f && (c.lim_max - c.lim_min) > 2 * c.hist &&
         c.atraso_s >= 30 && c.atraso_s <= 1800 && c.porta_s >= 15 && c.porta_s <= 600;
}

// ============================================================
//  Sensores
// ============================================================
float mediana3(float a, float b, float c) {
  if (a > b) { float t = a; a = b; b = t; }
  if (b > c) { float t = b; b = c; c = t; }
  if (a > b) { float t = a; a = b; b = t; }
  return b;
}

float lerDistanciaCm() {
  digitalWrite(PIN_TRIG, LOW);
  delayMicroseconds(2);
  digitalWrite(PIN_TRIG, HIGH);
  delayMicroseconds(10);
  digitalWrite(PIN_TRIG, LOW);
  unsigned long us = pulseIn(PIN_ECHO, HIGH, 30000);  // 30 ms ~ 5 m
  if (us == 0) return -1;                              // sem eco = leitura invalida
  return us / 58.0f;
}

// ============================================================
//  Alertas com cooldown (no proprio ESP32)
// ============================================================
struct Cooldown { const char* tipo; uint32_t ultimo_ms; bool usado; };
Cooldown cds[] = {
  {"temp_alta", 0, false}, {"temp_baixa", 0, false}, {"sensor_falha", 0, false},
  {"porta_aberta", 0, false}, {"porta_sensor", 0, false}, {"normalizado", 0, false},
  {"fila_cheia", 0, false}
};

bool podeAlertar(const char* tipo) {
  for (auto& c : cds) {
    if (strcmp(c.tipo, tipo) == 0) {
      if (c.usado && !passou(c.ultimo_ms, COOLDOWN_ALERTA_S * 1000UL)) return false;
      c.usado = true;
      c.ultimo_ms = millis();
      return true;
    }
  }
  return true;
}

void alertar(const char* tipo, float valor, const char* texto) {
  if (!podeAlertar(tipo)) return;
  JsonDocument d;
  d["disp"] = DISP;
  d["ts"] = agoraEpoch();
  d["tipo"] = tipo;
  if (!isnan(valor)) d["valor"] = serialized(String(valor, 1));
  d["msg"] = texto;
  String s;
  serializeJson(d, s);
  enviarFila(filaAlerta, s);
  Serial.printf("[ALERTA] %s %s\n", tipo, texto);
}

// ============================================================
//  Comandos remotos (validados aqui, nao na rede)
// ============================================================
String ultimosIds[8];
uint8_t idxId = 0;

void responderAck(const char* id, const char* acao, bool ok, const String& msg) {
  JsonDocument d;
  d["id"] = id;
  d["acao"] = acao;
  d["ok"] = ok;
  d["msg"] = msg;
  d["ts"] = agoraEpoch();
  String s;
  serializeJson(d, s);
  enviarFila(filaAck, s);
}

void processarComando(const char* json) {
  JsonDocument d;
  if (deserializeJson(d, json)) { Serial.println("[CMD] JSON invalido, ignorado"); return; }
  const char* id = d["id"] | "";
  const char* acao = d["acao"] | "";
  uint32_t ts = d["ts"] | 0;

  if (strlen(id) < 6 || strlen(id) > 36) { Serial.println("[CMD] sem id, ignorado"); return; }
  for (auto& u : ultimosIds) {
    if (u == id) { responderAck(id, acao, false, "id repetido (replay)"); return; }
  }
  ultimosIds[idxId] = id;
  idxId = (idxId + 1) % 8;

  // Anti-replay por tempo: comando com mais de 60 s e recusado
  if (relogioOk() && (ts == 0 || labs((long)agoraEpoch() - (long)ts) > 60)) {
    responderAck(id, acao, false, "comando expirado ou sem ts");
    return;
  }

  xSemaphoreTake(mtx, portMAX_DELAY);
  if (strcmp(acao, "silenciar") == 0) {
    int min = d["min"] | MUDO_PADRAO_MIN;
    min = constrain(min, 1, MUDO_MAX_MIN);
    est.mudo_ate_ms = millis() + min * 60000UL;
    if (est.mudo_ate_ms == 0) est.mudo_ate_ms = 1;
    xSemaphoreGive(mtx);
    responderAck(id, acao, true, "sirene silenciada por " + String(min) + " min (alarme continua registrado)");
  } else if (strcmp(acao, "reativar_som") == 0) {
    est.mudo_ate_ms = 0;
    xSemaphoreGive(mtx);
    responderAck(id, acao, true, "sirene reativada");
  } else if (strcmp(acao, "testar_sirene") == 0) {
    xSemaphoreGive(mtx);
    testeAte_ms = (millis() + 800) | 1;
    responderAck(id, acao, true, "sirene tocou 0,8 s");
  } else if (strcmp(acao, "recarregar_config") == 0) {
    xSemaphoreGive(mtx);
    pedirConfig = true;
    responderAck(id, acao, true, "config sera lida do servidor");
  } else {
    xSemaphoreGive(mtx);
    responderAck(id, acao, false, "acao desconhecida");  // lista branca
  }
  estadoMudou = true;
}

// ============================================================
//  TAREFA DE CONTROLE: sentir -> decidir -> agir
// ============================================================
void tarefaControle(void*) {
  float hist3[3] = {NAN, NAN, NAN};
  uint8_t nValidas = 0, falhasSeguidas = 0;
  uint32_t tDht = 0, tPorta = 0, tTelem = 0;
  uint32_t foraDesde = 0, dentroDesde = 0, portaAbertaDesde = 0;
  bool foraAlta = false;
  uint8_t falhasPorta = 0;
  uint32_t tBotao = 0;

  for (;;) {
    // ---- Botao local "silenciar" (funciona offline) ----
    if (digitalRead(PIN_BOTAO) == LOW && passou(tBotao, 1000)) {
      tBotao = millis();
      xSemaphoreTake(mtx, portMAX_DELAY);
      est.mudo_ate_ms = millis() + MUDO_PADRAO_MIN * 60000UL;
      xSemaphoreGive(mtx);
      estadoMudou = true;
      Serial.println("[LOCAL] sirene silenciada pelo botao");
    }

    // ---- Comandos vindos da rede ----
    Msg m;
    while (xQueueReceive(filaCmd, &m, 0) == pdTRUE) processarComando(m.txt);

    xSemaphoreTake(mtx, portMAX_DELAY);
    Config c = cfg;
    xSemaphoreGive(mtx);

    // ---- SENTIR: porta ----
    if (passou(tPorta, LEITURA_PORTA_MS)) {
      tPorta = millis();
      float d = lerDistanciaCm();
      xSemaphoreTake(mtx, portMAX_DELAY);
      if (d < 0 || d > 400) {
        if (++falhasPorta >= 5 && est.porta_ok) {
          est.porta_ok = false; estadoMudou = true;
          xSemaphoreGive(mtx);
          alertar("porta_sensor", NAN, "Sensor da porta sem leitura");
          xSemaphoreTake(mtx, portMAX_DELAY);
        }
      } else {
        falhasPorta = 0;
        if (!est.porta_ok) { est.porta_ok = true; estadoMudou = true; }
        est.dist_cm = d;
        bool aberta = d > PORTA_LIMIAR_CM;
        if (aberta != est.porta_aberta) {
          est.porta_aberta = aberta; estadoMudou = true;
          portaAbertaDesde = millis();
          if (!aberta) est.alerta_porta = false;
        }
        if (aberta && !est.alerta_porta && passou(portaAbertaDesde, c.porta_s * 1000UL)) {
          est.alerta_porta = true; estadoMudou = true;
          xSemaphoreGive(mtx);
          alertar("porta_aberta", d, "Porta aberta ha mais do que o limite");
          xSemaphoreTake(mtx, portMAX_DELAY);
        }
      }
      xSemaphoreGive(mtx);
    }

    // ---- SENTIR + DECIDIR: temperatura ----
    if (passou(tDht, LEITURA_DHT_MS)) {
      tDht = millis();
      float t = dht.readTemperature();
      float h = dht.readHumidity();
      bool valida = !isnan(t) && t > -40 && t < 80;

      xSemaphoreTake(mtx, portMAX_DELAY);
      Alarme antes = est.alarme;

      if (!valida) {
        falhasSeguidas++;
        if (falhasSeguidas >= FALHAS_P_SENSOR) {
          // ESTADO SEGURO: sem leitura nao ha como provar que a vacina esta boa
          est.alarme = AL_SENSOR;
          est.temp_c = NAN;
          nValidas = 0;
        }
      } else {
        if (falhasSeguidas >= FALHAS_P_SENSOR && est.alarme == AL_SENSOR) {
          est.alarme = AL_NORMAL;   // sensor voltou; a logica abaixo reavalia
          foraDesde = 0; dentroDesde = 0;
        }
        falhasSeguidas = 0;
        hist3[0] = hist3[1]; hist3[1] = hist3[2]; hist3[2] = t;
        if (nValidas < 3) nValidas++;
        float tf = (nValidas == 3) ? mediana3(hist3[0], hist3[1], hist3[2]) : t;  // filtra picos
        est.temp_c = tf;
        est.umid_pct = isnan(h) ? NAN : h;

        bool acima = tf > c.lim_max;
        bool abaixo = tf < c.lim_min;
        bool zonaSegura = tf >= c.lim_min + c.hist && tf <= c.lim_max - c.hist;

        // Fora da faixa: so dispara depois de ATRASO_S continuos
        if (acima || abaixo) {
          if (foraDesde == 0 || foraAlta != acima) { foraDesde = millis(); foraAlta = acima; if (foraDesde == 0) foraDesde = 1; }
          dentroDesde = 0;
          if (passou(foraDesde, c.atraso_s * 1000UL)) est.alarme = acima ? AL_ALTA : AL_BAIXA;
        } else {
          foraDesde = 0;
        }
        // Histerese: desarma so na zona segura e depois de NORMALIZA_S
        if ((est.alarme == AL_ALTA || est.alarme == AL_BAIXA) && zonaSegura) {
          if (dentroDesde == 0) dentroDesde = millis() | 1;
          if (passou(dentroDesde, NORMALIZA_S * 1000UL)) est.alarme = AL_NORMAL;
        } else if (!zonaSegura) {
          dentroDesde = 0;
        }
      }
      Alarme depois = est.alarme;
      float tAtual = est.temp_c;
      if (depois != antes) estadoMudou = true;
      xSemaphoreGive(mtx);

      if (depois != antes) {
        if (depois == AL_ALTA) alertar("temp_alta", tAtual, "Temperatura acima da faixa por mais de 5 min");
        else if (depois == AL_BAIXA) alertar("temp_baixa", tAtual, "Temperatura abaixo da faixa (risco de congelar)");
        else if (depois == AL_SENSOR) alertar("sensor_falha", NAN, "Sensor de temperatura sem leitura valida");
        else alertar("normalizado", tAtual, "Temperatura voltou para a faixa");
        tTelem = 0;  // grava ja a leitura da mudanca
      } else if (depois != AL_NORMAL) {
        alertar(nomeAlarme(depois), tAtual, "Alarme continua ativo (lembrete)");  // cooldown segura
      }
    }

    // ---- AGIR: LEDs e sirene ----
    xSemaphoreTake(mtx, portMAX_DELAY);
    bool mudo = est.mudo_ate_ms != 0 && (int32_t)(est.mudo_ate_ms - millis()) > 0;
    if (est.mudo_ate_ms != 0 && !mudo) { est.mudo_ate_ms = 0; estadoMudou = true; }
    Alarme al = est.alarme;
    bool portaAlerta = est.alerta_porta;
    xSemaphoreGive(mtx);

    bool algumAlarme = al != AL_NORMAL || portaAlerta;
    uint32_t fase = millis() % 1000;
    digitalWrite(PIN_LED_GRN, algumAlarme ? LOW : HIGH);
    digitalWrite(PIN_LED_RED, algumAlarme ? (al == AL_SENSOR ? (fase < 250 || (fase > 500 && fase < 750)) : HIGH) : LOW);
    if (testeAte_ms != 0 && (int32_t)(testeAte_ms - millis()) > 0) {
      tone(PIN_BUZZER, 2500);
    } else if (algumAlarme && !mudo) {
      testeAte_ms = 0;
      // padroes diferentes: temperatura = 500 ms on/off, sensor = rapido, porta = bip curto
      bool on = (al == AL_ALTA || al == AL_BAIXA) ? fase < 500
              : (al == AL_SENSOR) ? (fase % 250) < 125
              : fase < 120;
      if (on) tone(PIN_BUZZER, al == AL_BAIXA ? 1500 : 2500); else noTone(PIN_BUZZER);
    } else {
      testeAte_ms = 0;
      noTone(PIN_BUZZER);
    }

    // ---- TELEMETRIA (1/min ou na mudanca de alarme) ----
    bool temLeitura = nValidas > 0 || falhasSeguidas >= FALHAS_P_SENSOR;
    if (temLeitura && (tTelem == 0 || passou(tTelem, TELEMETRIA_S * 1000UL))) {
      tTelem = millis() | 1;
      JsonDocument d;
      xSemaphoreTake(mtx, portMAX_DELAY);
      d["disp"] = DISP;
      d["seq"] = ++est.seq;
      d["ts"] = agoraEpoch();               // 0 = relogio ainda nao sincronizado
      if (!isnan(est.temp_c)) d["temp_c"] = serialized(String(est.temp_c, 1));
      else d["temp_c"] = nullptr;
      if (!isnan(est.umid_pct)) d["umid_pct"] = serialized(String(est.umid_pct, 0));
      d["dist_cm"] = serialized(String(est.dist_cm, 1));
      d["porta"] = !est.porta_ok ? "desconhecida" : (est.porta_aberta ? "aberta" : "fechada");
      d["alarme"] = nomeAlarme(est.alarme);
      d["mudo"] = mudo;
      prefs.putUInt("seq", est.seq);        // seq sobrevive ao reinicio: buracos = dado perdido
      xSemaphoreGive(mtx);
      String s;
      serializeJson(d, s);
      enviarFila(filaTelemetria, s);
      Serial.println("[TEL] " + s);
    }

    vTaskDelay(pdMS_TO_TICKS(20));
  }
}

// ============================================================
//  TAREFA DE REDE: Wi-Fi, MQTT/TLS, HTTPS, fila offline
// ============================================================
WebSocketsClient wsMqtt;                       // WebSocket + TLS (porta 443)
arduino::mqtt::PubSubClient<512> mqtt;         // MQTT 3.1.1 por cima do WebSocket
bool wsIniciado = false;

const char* T_TELEM  = TOPICO_BASE "/telemetria";
const char* T_ESTADO = TOPICO_BASE "/estado";
const char* T_STATUS = TOPICO_BASE "/status";
const char* T_CMD    = TOPICO_BASE "/comando";
const char* T_ACK    = TOPICO_BASE "/ack";
const char* T_ALERTA = TOPICO_BASE "/alerta";

void aoReceberComando(const String& payload, const size_t len) {
  if (len == 0 || len >= sizeof(Msg::txt)) return;
  Msg m;
  memcpy(m.txt, payload.c_str(), len);
  m.txt[len] = 0;
  xQueueSend(filaCmd, &m, 0);
}

bool conectarMqtt() {
  if (!wsIniciado) {
    // TLS com o certificado raiz conferido (CA_HTTPS): nada de setInsecure()
    wsMqtt.beginSslWithCA(MQTT_HOST, MQTT_PORT, "/mqtt", CA_HTTPS, "mqtt");
    wsMqtt.setReconnectInterval(5000);
    wsIniciado = true;
  }
  // Last Will: se o ESP32 sumir, o broker publica "offline" sozinho (retido)
  mqtt.setWill(T_STATUS, "offline", true, 1);
  if (!mqtt.connect(DISP, MQTT_USER, MQTT_PASS)) {
    Serial.printf("[MQTT] falhou (erro %d, rc %d)\n", (int)mqtt.getLastError(), (int)mqtt.getReturnCode());
    return false;
  }
  mqtt.publish(T_STATUS, "online", true, 1);
  mqtt.subscribe(T_CMD, 1, aoReceberComando);
  estadoMudou = true;
  Serial.println("[MQTT] conectado: wss 443 com TLS");
  return true;
}

void publicarEstado() {
  JsonDocument d;
  xSemaphoreTake(mtx, portMAX_DELAY);
  bool mudo = est.mudo_ate_ms != 0 && (int32_t)(est.mudo_ate_ms - millis()) > 0;
  d["alarme"] = nomeAlarme(est.alarme);
  d["porta"] = !est.porta_ok ? "desconhecida" : (est.porta_aberta ? "aberta" : "fechada");
  d["alerta_porta"] = est.alerta_porta;
  d["mudo"] = mudo;
  d["mudo_restante_s"] = mudo ? (int32_t)(est.mudo_ate_ms - millis()) / 1000 : 0;
  d["lim_min"] = cfg.lim_min;
  d["lim_max"] = cfg.lim_max;
  d["hist"] = cfg.hist;
  d["atraso_s"] = cfg.atraso_s;
  d["fila_bytes"] = est.fila_bytes;
  d["ts"] = agoraEpoch();
  xSemaphoreGive(mtx);
  String s;
  serializeJson(d, s);
  mqtt.publish(T_ESTADO, s, true);  // retained: painel ve o ultimo estado ao abrir
}

// ---- Fila offline ----
// Normal: arquivo em LittleFS (append + offset salvo na NVS), sobrevive a reinicio.
// Se a flash nao tiver particao de arquivos (ex.: Wokwi), usa uma fila em RAM.
uint32_t filaOffset = 0;
bool fsOk = false;
#define FILA_RAM_MAX 240                 // 4 h de leituras em RAM
String filaRam[FILA_RAM_MAX];
uint16_t ramIni = 0, ramQtd = 0;

void filaAnexar(const char* linha) {
  if (!fsOk) {
    if (ramQtd == FILA_RAM_MAX) {        // cheia: descarta a mais antiga (o seq mostra o buraco)
      ramIni = (ramIni + 1) % FILA_RAM_MAX; ramQtd--;
      alertar("fila_cheia", NAN, "Fila offline (RAM) cheia: leitura mais antiga descartada");
    }
    filaRam[(ramIni + ramQtd) % FILA_RAM_MAX] = linha;
    ramQtd++;
    est.fila_bytes = ramQtd * 130;
    return;
  }
  File f = LittleFS.open("/fila.txt", FILE_APPEND);
  if (!f) return;
  if (f.size() > FILA_MAX_BYTES) {
    f.close();
    alertar("fila_cheia", NAN, "Fila offline cheia: leituras novas descartadas");
    return;
  }
  f.println(linha);
  est.fila_bytes = f.size() - filaOffset;
  f.close();
}

bool enviarLote(const String& corpo, int n, int& code) {
  WiFiClientSecure tls;
  tls.setCACert(CA_HTTPS);                 // confere o certificado do Worker
  HTTPClient http;
  http.begin(tls, String(WORKER_URL) + "/api/leituras");
  http.addHeader("Content-Type", "application/json");
  http.addHeader("Authorization", String("Bearer ") + DEVICE_TOKEN);
  code = http.POST(corpo);
  http.end();
  if (code == 200 || code == 201) { Serial.printf("[HTTP] %d leituras gravadas no D1\n", n); return true; }
  if (code == 401) Serial.println("[HTTP] 401: token do dispositivo recusado");
  else if (code == 400) Serial.println("[HTTP] 400: lote invalido descartado");
  else Serial.printf("[HTTP] falhou (%d), tenta de novo depois\n", code);
  return false;
}

// Envia ate LOTE_HTTP linhas pendentes. Retorna true se a fila esvaziou.
bool filaDrenar() {
  if (!fsOk) {
    if (ramQtd == 0) { est.fila_bytes = 0; return true; }
    int n = min((int)ramQtd, LOTE_HTTP);
    String corpo = "[";
    for (int i = 0; i < n; i++) { if (i) corpo += ","; corpo += filaRam[(ramIni + i) % FILA_RAM_MAX]; }
    corpo += "]";
    int code;
    if (enviarLote(corpo, n, code) || code == 400) {
      for (int i = 0; i < n; i++) filaRam[(ramIni + i) % FILA_RAM_MAX] = String();
      ramIni = (ramIni + n) % FILA_RAM_MAX; ramQtd -= n;
    }
    est.fila_bytes = ramQtd * 130;
    return ramQtd == 0;
  }
  File f = LittleFS.open("/fila.txt", FILE_READ);
  if (!f) return true;
  if (filaOffset >= f.size()) {
    f.close();
    LittleFS.remove("/fila.txt");
    filaOffset = 0; prefs.putUInt("off", 0); est.fila_bytes = 0;
    return true;
  }
  f.seek(filaOffset);
  String corpo = "[";
  int n = 0;
  while (f.available() && n < LOTE_HTTP) {
    String l = f.readStringUntil('\n');
    l.trim();
    if (l.length() == 0) continue;
    if (n++) corpo += ",";
    corpo += l;
  }
  uint32_t novoOffset = f.position();
  uint32_t tamanho = f.size();
  f.close();
  corpo += "]";
  if (n == 0) { filaOffset = novoOffset; return true; }

  int code;
  if (enviarLote(corpo, n, code) || code == 400) {   // 400: lote invalido e pulado para nao travar a fila
    filaOffset = novoOffset;
    prefs.putUInt("off", filaOffset);
    est.fila_bytes = tamanho - filaOffset;
    return filaOffset >= tamanho;
  }
  return false;
}

void enviarAlertaHttp(const char* json) {
  WiFiClientSecure tls;
  tls.setCACert(CA_HTTPS);
  HTTPClient http;
  http.begin(tls, String(WORKER_URL) + "/api/alertas");
  http.addHeader("Content-Type", "application/json");
  http.addHeader("Authorization", String("Bearer ") + DEVICE_TOKEN);
  http.POST(json);
  http.end();
}

void buscarConfig() {
  WiFiClientSecure tls;
  tls.setCACert(CA_HTTPS);
  HTTPClient http;
  http.begin(tls, String(WORKER_URL) + "/api/config?disp=" DISP);
  http.addHeader("Authorization", String("Bearer ") + DEVICE_TOKEN);
  int code = http.GET();
  if (code == 200) {
    JsonDocument d;
    if (!deserializeJson(d, http.getString())) {
      Config n;
      n.lim_min = d["lim_min"] | LIM_MIN_C;
      n.lim_max = d["lim_max"] | LIM_MAX_C;
      n.hist = d["hist"] | HISTERESE_C;
      n.atraso_s = d["atraso_s"] | ATRASO_S;
      n.porta_s = d["porta_s"] | PORTA_MAX_S;
      if (configValida(n)) {
        xSemaphoreTake(mtx, portMAX_DELAY);
        bool mudou = n.lim_min != cfg.lim_min || n.lim_max != cfg.lim_max || n.hist != cfg.hist ||
                     n.atraso_s != cfg.atraso_s || n.porta_s != cfg.porta_s;
        cfg = n;
        if (mudou) salvarConfig();   // guarda na flash: vale mesmo se reiniciar offline
        xSemaphoreGive(mtx);
        if (mudou) { estadoMudou = true; Serial.println("[CFG] limites atualizados pelo KV"); }
      } else {
        Serial.println("[CFG] config remota fora dos limites aceitaveis, ignorada");
      }
    }
  }
  http.end();
}

void tarefaRede(void*) {
  WiFi.mode(WIFI_STA);
  WiFi.begin(WIFI_SSID, WIFI_PASS);
  configTime(0, 0, "pool.ntp.org", "time.google.com");   // ts em UTC

  mqtt.begin(wsMqtt);
  mqtt.setKeepAliveTimeout(30);        // broker derruba (e solta a Will) apos ~105 s calado (45 s + folga)
  mqtt.setKeepAliveSendInterval(1000);
  mqtt.setTimeout(5000);

  uint32_t tMqtt = 0, tHttp = 0, tCfg = 0;
  bool cfgLida = false, avisouWifi = false, avisouNtp = false;

  for (;;) {
    // sem relogio o TLS nao consegue validar o certificado -> trata como offline
    if (WiFi.status() == WL_CONNECTED && !avisouWifi) { avisouWifi = true; Serial.println("[WIFI] conectado, IP " + WiFi.localIP().toString()); }
    if (WiFi.status() != WL_CONNECTED && avisouWifi) { avisouWifi = false; Serial.println("[WIFI] caiu: alarme local segue, leituras vao para a fila"); }
    if (relogioOk() && !avisouNtp) { avisouNtp = true; Serial.println("[NTP] relogio sincronizado"); }
    bool wifi = WiFi.status() == WL_CONNECTED && relogioOk();
    Msg m;

    // 1) Toda telemetria vai para a fila em flash (sobrevive a queda e a reinicio)
    while (xQueueReceive(filaTelemetria, &m, 0) == pdTRUE) {
      filaAnexar(m.txt);
      if (mqtt.isConnected()) mqtt.publish(T_TELEM, m.txt);  // tempo real para o painel
    }

    // TLS confere a validade do certificado: precisa do relogio (NTP) antes
    if (mqtt.isConnected()) {
      mqtt.update();                     // recebe comandos e manda PINGREQ
    } else {
      if (wsIniciado) wsMqtt.loop();
      if (wifi && passou(tMqtt, 5000)) { tMqtt = millis(); conectarMqtt(); }
    }
    est.online = wifi && mqtt.isConnected();

    // 2) Alertas: MQTT (painel) + HTTPS (Worker grava e avisa no Telegram)
    while (xQueueReceive(filaAlerta, &m, 0) == pdTRUE) {
      if (mqtt.isConnected()) mqtt.publish(T_ALERTA, m.txt);
      if (wifi) enviarAlertaHttp(m.txt);
    }
    // 3) Acks dos comandos
    while (xQueueReceive(filaAck, &m, 0) == pdTRUE) {
      if (mqtt.isConnected()) mqtt.publish(T_ACK, m.txt);
    }
    if (estadoMudou && mqtt.isConnected()) { estadoMudou = false; publicarEstado(); }

    // 4) Historico: drena a fila a cada 60 s; se houver atraso, drena rapido
    if (wifi && (passou(tHttp, TELEMETRIA_S * 1000UL) || (tHttp != 0 && est.fila_bytes > 0 && passou(tHttp, 2000)))) {
      tHttp = millis() | 1;
      filaDrenar();
    }

    // 5) Limites remotos (KV)
    if (wifi && (!cfgLida || pedirConfig || passou(tCfg, CONFIG_S * 1000UL))) {
      tCfg = millis(); cfgLida = true; pedirConfig = false;
      buscarConfig();
    }

    vTaskDelay(pdMS_TO_TICKS(10));
  }
}

// ============================================================
void setup() {
  Serial.begin(115200);
  pinMode(PIN_TRIG, OUTPUT);
  pinMode(PIN_ECHO, INPUT);
  pinMode(PIN_BUZZER, OUTPUT);
  pinMode(PIN_LED_RED, OUTPUT);
  pinMode(PIN_LED_GRN, OUTPUT);
  pinMode(PIN_BOTAO, INPUT_PULLUP);
  dht.begin();

  fsOk = LittleFS.begin(true);
  if (!fsOk) Serial.println("[FS] sem particao de arquivos: fila offline vai ficar em RAM");
  prefs.begin("geladeira", false);
  carregarConfig();                       // ultimos limites validos (funciona offline)
  if (!configValida(cfg)) cfg = Config();
  est.seq = prefs.getUInt("seq", 0);
  filaOffset = prefs.getUInt("off", 0);

  // Nunca imprimir senha/token no Serial (buraco 5)
  Serial.printf("\nGeladeira %s | faixa %.1f-%.1f C | topico %s\n", DISP, cfg.lim_min, cfg.lim_max, TOPICO_BASE);

  mtx = xSemaphoreCreateMutex();
  filaTelemetria = xQueueCreate(10, sizeof(Msg));
  filaAlerta = xQueueCreate(6, sizeof(Msg));
  filaCmd = xQueueCreate(4, sizeof(Msg));
  filaAck = xQueueCreate(4, sizeof(Msg));

  xTaskCreatePinnedToCore(tarefaControle, "controle", 8192, nullptr, 2, nullptr, 1);
  // Rede no nucleo 1 com prioridade MENOR que o controle: o handshake TLS e a espera
  // do MQTT podem ocupar a CPU por segundos, mas o controle (prioridade 2) sempre
  // interrompe a rede. No nucleo 0 ficam so o Wi-Fi do sistema e o IDLE0 (watchdog).
  xTaskCreatePinnedToCore(tarefaRede, "rede", 12288, nullptr, 1, nullptr, 1);
}

void loop() { vTaskDelay(portMAX_DELAY); }
