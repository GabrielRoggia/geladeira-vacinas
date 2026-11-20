# Teste do Worker local. Uso:
#   DEVICE_TOKEN=<token registrado no KV local> PAINEL_TOKEN=<o do .dev.vars> python3 test_worker.py
# Rode com o banco local recem-criado (os contadores esperam um banco vazio).
import json, os, time, urllib.request, sys
B=os.environ.get("BASE","http://localhost:8787"); DEV=os.environ["DEVICE_TOKEN"]; PAI=os.environ.get("PAINEL_TOKEN","painel-teste-123456")
def req(m,p,body=None,tok=None):
    d=json.dumps(body).encode() if body is not None else None
    r=urllib.request.Request(B+p,data=d,method=m,headers={"content-type":"application/json",**({"authorization":"Bearer "+tok} if tok else {})})
    try:
        with urllib.request.urlopen(r) as x: return x.status, x.read().decode()
    except urllib.error.HTTPError as e: return e.code, e.read().decode()
ok=0;fail=0
def check(nome,cond,info=""):
    global ok,fail
    print(("PASS " if cond else "FAIL ")+nome+("" if cond else "  -> "+info)); ok+=cond; fail+=(not cond)
now=int(time.time())
L=lambda seq,ts,t,**k: {"disp":"gel01","seq":seq,"ts":ts,"temp_c":t,"umid_pct":60,"dist_cm":4.2,"porta":"fechada","alarme":"normal","mudo":False,**k}
s,b=req("POST","/api/leituras",L(1,now,5.0)); check("sem token -> 401",s==401,b)
s,b=req("POST","/api/leituras",L(1,now,5.0),"x"*64); check("token falso -> 401",s==401,b)
s,b=req("POST","/api/leituras",{**L(1,now,5.0),"disp":"gel99"},DEV); check("disp diferente do token -> 400",s==400,b)
s,b=req("POST","/api/leituras",L(1,now,200),DEV); check("temp 200 C -> 400",s==400,b)
s,b=req("POST","/api/leituras",L(1,now+3600,5),DEV); check("ts no futuro -> 400",s==400,b)
s,b=req("POST","/api/leituras",L(1,now,5.0),DEV); check("leitura valida -> 200",s==200 and json.loads(b)["gravadas"]==1,b)
# lote "fila offline": 90 min atras, com 7 min acima de 8 C e 2 leituras com sensor em falha
lote=[]
for i in range(90):
    t=5.0+ (4.0 if 40<=i<47 else 0)
    lote.append(L(100+i, now-90*60+i*60, None if i in (60,61) else t, alarme="temp_alta" if 45<=i<47 else "normal"))
for k in range(0,90,20):
    s,b=req("POST","/api/leituras",lote[k:k+20],DEV)
check("lote da fila gravado",s==200,b)
s,b=req("POST","/api/leituras",lote[:20],DEV); j=json.loads(b); check("reenvio nao duplica",s==200 and j["duplicadas"]==20,b)
s,b=req("POST","/api/leituras",[L(i,now,5) for i in range(101)],DEV); check("lote >100 -> 400",s==400,b)
s,b=req("GET","/api/resumo?disp=gel01&horas=24"); check("resumo sem token -> 401",s==401,b)
s,b=req("GET","/api/resumo?disp=gel01&horas=24",tok=PAI); j=json.loads(b)
check("resumo: 7 min fora e 2 falhas",s==200 and j["total"]["min_fora_faixa"]==7 and j["total"]["min_sensor_falha"]==2,b[:300])
check("resumo: sempre_na_faixa = false",j["sempre_na_faixa"] is False)
check("resumo: vindas da fila",j["total"]["vindas_da_fila"]>=80,str(j["total"]))
print("   total:",j["total"]); print("   1a hora:",j["por_hora"][0])
s,b=req("GET","/api/resumo?disp=gel01;DROP%20TABLE%20leituras&horas=24",tok=PAI); check("disp com SQL -> 400",s==400,b)
a={"disp":"gel01","ts":now,"tipo":"temp_alta","valor":9.1,"msg":"Temperatura acima"}
s1,b1=req("POST","/api/alertas",a,DEV); s2,b2=req("POST","/api/alertas",a,DEV)
check("alerta 1 passa, 2o segurado pelo cooldown",s1==200 and s2==200 and json.loads(b2)["cooldown"] is True,b1+b2)
s,b=req("POST","/api/alertas",{**a,"tipo":"abrir_tudo"},DEV); check("tipo de alerta desconhecido -> 400",s==400,b)
s,b=req("GET","/api/alertas?disp=gel01",tok=PAI); check("alertas gravados no D1 (2)",s==200 and len(json.loads(b))==2,b)
s,b=req("PUT","/api/config?disp=gel01",{"lim_min":2,"lim_max":50},PAI); check("config absurda -> 400",s==400,b)
s,b=req("PUT","/api/config?disp=gel01",{"lim_min":2,"lim_max":8},DEV); check("dispositivo nao altera config -> 401",s==401,b)
s,b=req("PUT","/api/config?disp=gel01",{"lim_min":2.0,"lim_max":7.8,"hist":0.4,"atraso_s":240},PAI); check("config valida -> 200",s==200,b)
s,b=req("GET","/api/config",tok=DEV); check("ESP32 le config do KV",s==200 and json.loads(b)["lim_max"]==7.8,b)
s,b=req("GET","/api/ultimo?disp=gel01",tok=PAI); check("ultimo estado no KV",s==200 and "idade_s" in b,b)
s,b=req("GET","/"); check("painel servido em /",s==200 and "Geladeira de Vacinas" in b,b[:80])
s,b=req("GET","/__scheduled?cron=*/5+*+*+*+*"); check("cron vigia offline roda",s==200,b)
print(f"\n{ok} passaram, {fail} falharam"); sys.exit(1 if fail else 0)
