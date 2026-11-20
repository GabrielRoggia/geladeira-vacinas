#pragma once
// Copie este arquivo para secrets.h e preencha.
// secrets.h esta no .gitignore: NUNCA suba senhas para o GitHub.

#define WIFI_SSID     "Wokwi-GUEST"   // no Wokwi a senha e vazia
#define WIFI_PASS     ""

// Tudo na Cloudflare: o mesmo Worker e o broker MQTT (wss://HOST/mqtt) e a API
#define WORKER_HOST   "geladeira-vacinas.SEU-USUARIO.workers.dev"
#define WORKER_URL    "https://" WORKER_HOST

// Broker MQTT: MQTT 3.1.1 sobre WebSocket com TLS, porta 443
#define MQTT_HOST     WORKER_HOST
#define MQTT_PORT     443
#define MQTT_USER     "gel01"         // usuario so desta geladeira (ACL no broker)
#define MQTT_PASS     "troque-me"

// Token HTTP desta geladeira (o Worker guarda so o hash SHA-256)
#define DEVICE_TOKEN  "gere-com: openssl rand -hex 32"
