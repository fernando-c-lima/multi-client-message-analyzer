import os
import time
import json
import psycopg2
import pandas as pd
from collections import Counter
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import openai
from datetime import datetime

# -----------------------------
# CONFIGURAÇÕES
# -----------------------------
load_dotenv()

openai.api_key = os.getenv("OPENAI_API_KEY")
DATA_INICIO = os.getenv("DATA_INICIO")
CLIENT_ID = os.getenv("CLIENT_ID_IATE")

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")



BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # Pasta do cliente (Vamo, Fantastico, etc.)
DATA_DIR = os.path.join(BASE_DIR, "data")

# Sufixo com a data de hoje
DATA_HOJE = datetime.now().strftime("%Y%m%d")

ARQUIVO_INPUT = os.path.join(DATA_DIR, f"batch_input_{DATA_HOJE}.jsonl")
ARQUIVO_RESULTADO_JSONL = os.path.join(DATA_DIR, f"analise_mensagens_Iate_{DATA_HOJE}.jsonl")
ARQUIVO_EXCEL = os.path.join(DATA_DIR, f"analise_mensagens_Iate_{DATA_HOJE}.xlsx")

load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")

SQL = """
SELECT 
    "leadId",
    STRING_AGG(message, ' || ') AS mensagens
FROM ideia_message_db
WHERE "clientId" = %s
AND "createdAt" >= %s
GROUP BY "leadId"
limit 3
;

"""

SYSTEM_PROMPT = """
Você é uma IA que atua como classificadora de assuntos de mensagens.

Você receberá uma sequência de mensagens concatenadas do mesmo cliente (leadId), representando toda a conversa desse cliente.

Sua tarefa é identificar **todos os assuntos** que aparecem nas mensagens dessa conversa, e listá-los.

Os assuntos possíveis são:

- contato
- endereço
- locais do clube: (centro esportivo,centro de força kyra gracie, quadra poliesportiva, quadra de areia, pavilhão japonês, quiosque praia nova, campo Society, náutica, piscina social, quadra de tênis) 
- associação e planos (valores, dependentes)
- regras
- serviços
- eventos
- atividades
- reclamações 
- assuntos gerais

⚠️ IMPORTANTE:
- Responda **apenas** com a linha:  
  Assuntos: [assunto1], [assunto2], [assunto3], ...  
- Liste todos os assuntos que aparecem na conversa, separados por vírgula.
- Não repita assuntos iguais.
- Não explique nada além disso.
- Não escreva mais nada que não essa linha.
"""

# -----------------------------
# 1. GERA O ARQUIVO DE INPUT
# -----------------------------
def gerar_input():
    print("📝 Gerando arquivo de entrada...")
    conn = psycopg2.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASSWORD, dbname=DB_NAME
    )
    with conn.cursor(cursor_factory=RealDictCursor) as cursor, open(ARQUIVO_INPUT, "w", encoding="utf-8") as f:
         cursor.execute(SQL, (CLIENT_ID, DATA_INICIO))
        for row in cursor.fetchall():
            mensagens = str(row.get("mensagens", "")).strip()
            user_prompt = f"Mensagem do cliente:\n{mensagens}"
            entrada = {
                "custom_id": f"id-{row['leadId']}",
                "method": "POST",
                "url": "/v1/chat/completions",
                "body": {
                    "model": "gpt-4o-mini",
                    "messages": [
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {"role": "user", "content": user_prompt},
                    ],
                    "temperature": 0.0,
                },
            }
            f.write(json.dumps(entrada, ensure_ascii=False) + "\n")
    conn.close()
    print(f"✅ Arquivo '{ARQUIVO_INPUT}' gerado com sucesso.")


# -----------------------------
# 2. ENVIA ARQUIVO E CRIA JOB
# -----------------------------
def criar_job():
    print("📤 Enviando arquivo...")
    upload = openai.files.create(file=open(ARQUIVO_INPUT, "rb"), purpose="batch")
    print("✅ Arquivo enviado. File ID:", upload.id)

    print("🚀 Criando job...")
    batch = openai.batches.create(
        input_file_id=upload.id,
        endpoint="/v1/chat/completions",
        completion_window="24h",
    )
    print("✅ Job criado. Job ID:", batch.id)
    return batch.id


# -----------------------------
# 3. MONITORA O STATUS
# -----------------------------
def aguardar_job(job_id):
    print("⏳ Monitorando job...")
    while True:
        batch = openai.batches.retrieve(job_id)
        status = batch.status
        print(f"   Status: {status}")
        if status in ["completed", "failed", "expired", "cancelled"]:
            return batch
        time.sleep(10)


# -----------------------------
# 4. BAIXA E PROCESSA RESULTADO
# -----------------------------
def aguardar_e_processar_batch(job_id, intervalo=10, max_tentativas=60):
    from openai import APIError, APIConnectionError, APITimeoutError  

    tentativas = 0
    arquivo_saida = None

    while tentativas < max_tentativas:
        try:
            batch = openai.batches.retrieve(job_id)
            print(f"Status do batch: {batch.status}")

            if batch.status == "completed":
                # ✅ pega o arquivo de saída
                if batch.output_file_id:
                    print("🔽 Baixando resultado final do batch...")
                    response = openai.files.retrieve_content(batch.output_file_id)
                    arquivo_saida = os.path.join(DATA_DIR, f"resultado_batch_{DATA_HOJE}.jsonl")
                    with open(arquivo_saida, "w", encoding="utf-8") as f:
                        f.write(response)
                    print(f"✅ Resultado salvo em {arquivo_saida}")
                break

            elif batch.status in ["failed", "expired", "cancelled"]:
                print(f"❌ Batch terminou com status: {batch.status}")
                break

        except (APITimeoutError, APIConnectionError) as e:
            print(f"⚠️ Timeout ou erro de conexão. Tentando novamente em {intervalo}s... ({e})")
        except APIError as e:
            print(f"⚠️ Erro na API: {e}. Tentando novamente em {intervalo}s...")
        except OpenAIError as e:
            print(f"⚠️ Erro da OpenAI: {e}. Tentando novamente em {intervalo}s...")
        except Exception as e:
            print(f"⚠️ Erro inesperado: {e}. Tentando novamente em {intervalo}s...")

        tentativas += 1
        time.sleep(intervalo)

    if not arquivo_saida:
        print("⛔ Nenhum resultado disponível para este batch.")
        return

    # -----------------------------
    # Processar resultados do JSONL
    # -----------------------------
    print("📊 Processando resultados e gerando Excel...")

    mensagens_originais = {}
    with open(ARQUIVO_INPUT, "r", encoding="utf-8") as f:
        for linha in f:
            try:
                item = json.loads(linha)
                mensagens_originais[item["custom_id"]] = " ".join(
                    [m["content"] for m in item["body"]["messages"] if m["role"] == "user"]
                )
            except Exception:
                continue

    dados, temas = [], []
    total_tokens = 0

    with open(arquivo_saida, "r", encoding="utf-8") as f:
        for linha in f:
            try:
                item = json.loads(linha)
                custom_id = item.get("custom_id", "")
                body = item.get("response", {}).get("body", {})
                choices = body.get("choices", [])
                usage = body.get("usage", {})
                tokens = usage.get("total_tokens", 0)

                resposta = ""
                if choices:
                    resposta = choices[0].get("message", {}).get("content", "")

                assunto = ""
                for l in resposta.splitlines():
                    if l.lower().startswith("assunto:") or l.lower().startswith("assuntos:"):
                        assunto = l.split(":", 1)[-1].strip()

                mensagem_original = mensagens_originais.get(custom_id, "")
                dados.append({
                    "ID": custom_id,
                    "Mensagem Original": mensagem_original,
                    "Assunto": assunto,
                    "Tokens": tokens
                })

                for a in assunto.split(","):
                    a_limpo = a.strip()
                    if a_limpo:
                        temas.append(a_limpo)

                total_tokens += tokens
            except Exception:
                continue

    # Estatísticas
    contagem = Counter(temas)
    total_msgs = len(temas)
    df_estatisticas = pd.DataFrame([
        {"Assunto": a, "Quantidade": contagem[a], "Porcentagem (%)": round(contagem[a]/total_msgs*100, 2)}
        for a in contagem
    ])
    df_estatisticas.loc[len(df_estatisticas)] = {"Assunto": "TOTAL", "Quantidade": total_msgs, "Porcentagem (%)": 100.0}
    df_estatisticas.loc[len(df_estatisticas)] = {"Assunto": "TOTAL TOKENS", "Quantidade": total_tokens, "Porcentagem (%)": "-"}

    # Exportar Excel
    df_detalhado = pd.DataFrame(dados)
    with pd.ExcelWriter(ARQUIVO_EXCEL, engine="openpyxl") as writer:
        df_detalhado.to_excel(writer, sheet_name="Mensagens Classificadas", index=False)
        df_estatisticas.to_excel(writer, sheet_name="Estatísticas", index=False)

    print(f"📊 Análise concluída. Arquivo Excel gerado: {ARQUIVO_EXCEL}")


# -----------------------------
# MAIN
# -----------------------------
if __name__ == "__main__":
    gerar_input()
    job_id = criar_job()
    batch = aguardar_job(job_id)
    baixar_e_analisar(batch)
