import openai
import os
import time
import json
import pandas as pd
from collections import Counter
from dotenv import load_dotenv
from datetime import datetime

# -----------------------------
# Configurações
# -----------------------------
load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")

# -----------------------------
# Pastas e arquivos
# -----------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
DATA_HOJE = datetime.now().strftime("%Y%m%d")

# Batch que você quer processar
JOB_ID = "batch_68a5c47af5c881908bdf5704fa6fa836"

# Arquivos
ARQUIVO_INPUT = os.path.join(DATA_DIR, f"batch_input_{DATA_HOJE}.jsonl")
ARQUIVO_JSONL = os.path.join(DATA_DIR, f"resultado_batch_{DATA_HOJE}.jsonl")
ARQUIVO_EXCEL = os.path.join(DATA_DIR, f"resultado_batch_{DATA_HOJE}.xlsx")

# -----------------------------
# 1. Aguardar batch terminar
# -----------------------------
print("⏳ Acompanhando status do batch...")
while True:
    batch = openai.batches.retrieve(JOB_ID)
    status = batch.status
    print(f"Status atual: {status}")
    if status in ["completed", "failed", "expired", "cancelled"]:
        break
    time.sleep(10)

if status != "completed":
    print(f"❌ O batch terminou com status: {status}")
    exit(1)

print("✅ Batch finalizado com sucesso!")
FILE_ID = batch.output_file_id
print("📁 ID do arquivo de saída:", FILE_ID)

# -----------------------------
# 2. Ler mensagens originais do batch_input
# -----------------------------
mensagens_originais = {}
with open(ARQUIVO_INPUT, "r", encoding="utf-8") as f:
    for linha in f:
        item = json.loads(linha)
        custom_id = item.get("custom_id")
        mensagens = [
            m.get("content", "")
            for m in item.get("body", {}).get("messages", [])
            if m.get("role") == "user"
        ]
        mensagens_originais[custom_id] = " ".join(mensagens)

# -----------------------------
# 3. Baixar resultado do batch
# -----------------------------
print("🔽 Baixando conteúdo do arquivo batch...")
response = openai.files.retrieve_content(FILE_ID)
with open(ARQUIVO_JSONL, "w", encoding="utf-8") as f:
    f.write(response)
print(f"✅ Arquivo JSONL salvo: {ARQUIVO_JSONL}")

# -----------------------------
# 4. Processar resultados
# -----------------------------
dados = []
temas = []
total_tokens = 0

with open(ARQUIVO_JSONL, "r", encoding="utf-8") as f:
    for linha in f:
        item = json.loads(linha)
        custom_id = item.get("custom_id", "")
        body = item.get("response", {}).get("body", {})
        choices = body.get("choices", [])
        usage = body.get("usage", {})
        tokens = int(usage.get("total_tokens", 0))

        resposta = ""
        if choices:
            resposta = choices[0].get("message", {}).get("content", "")

        # Extrair assunto(s)
        assuntos = []
        for linha_resp in resposta.splitlines():
            linha_resp_lower = linha_resp.lower()
            if linha_resp_lower.startswith("assunto:") or linha_resp_lower.startswith("assuntos:"):
                assuntos.append(linha_resp.split(":", 1)[-1].strip())
        assunto = ", ".join(assuntos)

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

# -----------------------------
# 5. Estatísticas
# -----------------------------
contagem = Counter(temas)
total_msgs = len(temas)

df_estatisticas = pd.DataFrame([
    {"Assunto": assunto, "Quantidade": contagem[assunto], "Porcentagem (%)": round(contagem[assunto]/total_msgs*100, 2)}
    for assunto in contagem
])

df_estatisticas.loc[len(df_estatisticas)] = {
    "Assunto": "TOTAL",
    "Quantidade": total_msgs,
    "Porcentagem (%)": 100.0
}
df_estatisticas.loc[len(df_estatisticas)] = {
    "Assunto": "TOTAL TOKENS",
    "Quantidade": total_tokens,
    "Porcentagem (%)": "-"
}

# -----------------------------
# 5.1 Estatísticas por unidade
# -----------------------------
# Supondo que a unidade está mencionada na "Mensagem Original"
unidades = ['Lagoa', 'Downtown', 'Bossa Nova', 'Abelardo Bueno']
estat_unidades = []

for unidade in unidades:
    # Filtrar mensagens da unidade
    dados_unidade = [d for d in dados if unidade.lower() in d["Mensagem Original"].lower()]
    temas_unidade = []
    for d in dados_unidade:
        for a in d["Assunto"].split(","):
            a_limpo = a.strip()
            if a_limpo:
                temas_unidade.append(a_limpo)
    
    contagem = Counter(temas_unidade)
    total_msgs = len(temas_unidade)
    
    if contagem:
        mais_frequente = contagem.most_common(1)[0][0]
    else:
        mais_frequente = "N/A"
    
    estat_unidades.append({
        "Unidade": unidade,
        "Total de Assuntos": total_msgs,
        "Assunto Mais Buscado": mais_frequente,
        "Detalhes": dict(contagem)
    })

df_estat_unidades = pd.DataFrame(estat_unidades)

# -----------------------------
# 6. Gerar Excel
# -----------------------------
df_detalhado = pd.DataFrame(dados)

with pd.ExcelWriter(ARQUIVO_EXCEL, engine="openpyxl") as writer:
    df_detalhado.to_excel(writer, sheet_name="Mensagens Classificadas", index=False)
    df_estatisticas.to_excel(writer, sheet_name="Estatísticas", index=False)
    df_estat_unidades.to_excel(writer, sheet_name="Estatísticas por unidade", index=False)

print(f"✅ Excel gerado: {ARQUIVO_EXCEL}")
