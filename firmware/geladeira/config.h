#pragma once
// Configuracao NAO secreta (pode ir para o GitHub).
// Senhas e tokens ficam em secrets.h, que esta no .gitignore.

// ---------- Identidade e topicos ----------
#define GRUPO   "g01"            // troque pelo numero do grupo
#define LOCAL   "posto-centro"   // posto de saude
#define DISP    "gel01"          // geladeira dentro do posto
// Raiz: sis1a/<grupo>/<local>/<dispositivo>/...
#define TOPICO_BASE "sis1a/" GRUPO "/" LOCAL "/" DISP

// ---------- Pinos (ESP32 DevKit) ----------
#define PIN_DHT      15   // DHT22 - temperatura/umidade dentro da geladeira
#define PIN_TRIG      5   // HC-SR04 - sensor da porta
#define PIN_ECHO     18
#define PIN_BUZZER   26   // buzzer = sirene do posto
#define PIN_LED_RED  25   // LED vermelho = alarme ativo
#define PIN_LED_GRN  27   // LED verde = tudo normal
#define PIN_BOTAO    14   // botao local "silenciar" (funciona sem internet)

// ---------- Padroes (sobrescritos pela config remota no KV) ----------
#define LIM_MIN_C        2.0f   // faixa da vacina: 2 a 8 C
#define LIM_MAX_C        8.0f
#define HISTERESE_C      0.5f   // so desarma entre 2,5 e 7,5 C
#define ATRASO_S         300    // fora da faixa por 5 min -> alarme
#define NORMALIZA_S      60     // dentro da faixa por 1 min -> desarma
#define PORTA_MAX_S      60     // porta aberta > 60 s -> alerta
#define PORTA_LIMIAR_CM  10.0f  // distancia ate a porta < 10 cm = fechada

// ---------- Tempos ----------
#define LEITURA_DHT_MS   2000   // DHT22 aceita 1 leitura a cada 2 s
#define LEITURA_PORTA_MS 500
#define TELEMETRIA_S     60     // 1 leitura por minuto no historico
#define COOLDOWN_ALERTA_S 900   // mesmo alerta nao se repete antes de 15 min
#define MUDO_PADRAO_MIN  10
#define MUDO_MAX_MIN     30
#define FALHAS_P_SENSOR  3      // 3 leituras invalidas seguidas = sensor em falha
#define CONFIG_S         600    // le limites do KV a cada 10 min
#define LOTE_HTTP        20     // leituras por POST ao reenviar a fila
#define FILA_MAX_BYTES   600000 // ~3 dias de leituras guardadas offline
