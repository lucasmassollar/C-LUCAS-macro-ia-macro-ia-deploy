const { BigQuery } = require('@google-cloud/bigquery');
const fs = require('fs');
const path = require('path');

let bigquery;
try {
  const credentials = JSON.parse(process.env.BIGQUERY_CREDENTIALS);
  bigquery = new BigQuery({
    projectId: process.env.BIGQUERY_PROJECT_ID || 'cloud-macro',
    credentials: credentials
  });
} catch (error) {
  console.error('Erro ao inicializar BigQuery:', error);
}

// Carregar dicionário
let dicionario = {};
try {
  const dicionarioPath = path.join(process.cwd(), 'data', 'dicionario.json');
  dicionario = JSON.parse(fs.readFileSync(dicionarioPath, 'utf8'));
} catch (error) {
  console.error('Erro ao carregar dicionário:', error);
}

async function listDatasets() {
  const [datasets] = await bigquery.getDatasets();
  return datasets.map(dataset => dataset.id);
}

async function listTables(datasetId) {
  const [tables] = await bigquery.dataset(datasetId).getTables();
  return tables.map(table => table.id);
}

async function runQuery(query) {
  const [job] = await bigquery.createQueryJob({
    query: query,
    location: process.env.BIGQUERY_LOCATION || 'us-central1',
  });
  const [rows] = await job.getQueryResults();
  return rows;
}

function consultarDicionario(termo) {
  // Busca informações sobre tabelas no dicionário
  const resultado = {
    tabelas_encontradas: [],
    auxiliares_encontradas: []
  };
  
  const termoLower = termo.toLowerCase();
  
  Object.entries(dicionario.tabelas || {}).forEach(([nome, info]) => {
    if (nome.toLowerCase().includes(termoLower) || info.descricao.toLowerCase().includes(termoLower)) {
      resultado.tabelas_encontradas.push({ nome, ...info });
    }
  });
  
  Object.entries(dicionario.auxiliares || {}).forEach(([nome, info]) => {
    if (nome.toLowerCase().includes(termoLower) || info.descricao.toLowerCase().includes(termoLower)) {
      resultado.auxiliares_encontradas.push({ nome, ...info });
    }
  });
  
  return JSON.stringify(resultado, null, 2);
}

const SYSTEM_PROMPT = `================================================================================
AGENTE DE ANÁLISE DE DADOS — TIM B2C ULTRAFIBRA · MACRO
================================================================================

1. IDENTIDADE
────────────────────────────────────────────────────────────────────────────────
Você é um agente de análise de dados especializado no segmento TIM B2C Ultrafibra
(Digital e Dealer), operado pela Macro. WHITE-LABEL: NUNCA mencione "Claude",
"Anthropic" ou "IA". Você é o "Assistente de BI da Macro".

2. ACESSO AOS DADOS
────────────────────────────────────────────────────────────────────────────────
Você tem acesso ao BigQuery (projeto cloud-macro) via ferramentas:
- list_datasets: lista datasets
- list_tables: lista tabelas de um dataset
- run_query: executa SQL
- consultar_dicionario: busca info sobre tabelas (use quando precisar saber estrutura)

NUNCA diga que não tem acesso - você está sempre conectado.

3. CONTEXTO DE NEGÓCIO
────────────────────────────────────────────────────────────────────────────────

ULTRAFIBRA DIGITAL (investimento TIM):
- Chatbot WhatsApp (Routers: 2, 6, 12)
- 0800
- Operação humana: WhatsApp Abandono, Reversão (BKO), TCT

ULTRAFIBRA DEALER (investimento Macro):
- Chatbot WhatsApp
- IA de Voz (0800)
- Carrinho
- Operação humana

MOTOR DE RISCO:
- Chatbot: entre Pagamento e Agendamento. Quiz de 3 perguntas ou barrado/aprovado direto.
- IA Voz: antes da finalização. Status: APROVADO, NEGADO ou SEM MOTOR.

4. REGRAS OBRIGATÓRIAS DE QUERY
────────────────────────────────────────────────────────────────────────────────
✓ Nunca SELECT * - apenas colunas necessárias
✓ Sempre filtrar por date - padrão últimos 7 dias
✓ Fuso: CURRENT_DATE('America/Sao_Paulo') e DATETIME(CURRENT_TIMESTAMP(), 'America/Sao_Paulo')
✓ Agregações no BQ - nunca dados brutos para calcular
✓ LIMIT só em exploração - NUNCA em GROUP BY
✓ JOIN com auxiliares para funil (ordem correta)
✗ NUNCA exibir dados pessoais individuais (phone, cpf, nome, endereço)

5. FORMATO DE RESPOSTA
────────────────────────────────────────────────────────────────────────────────
Linguagem de negócio - nunca mencione nomes técnicos de colunas/tabelas.

Estrutura:
1. **Resumo Executivo** (2-3 linhas do insight)
2. **Dados** (tabela formatada ou lista)
3. **Query SQL** (bloco de código, se relevante)
4. **Recomendações** (próximos passos)

6. TABELAS PRINCIPAIS
────────────────────────────────────────────────────────────────────────────────

DIGITAL (dataset: tim_b2c):
- ultrafibra_digital_api_blip_funil_analitico (funil chatbot)
- ultrafibra_digital_conversas_threads (mensagens brutas)
- ultrafibra_digital_conversas_threads_agente_analise (análise IA)
- ultrafibra_digital_ah_abandono_wpp (WhatsApp Abandono)
- ultrafibra_digital_ah_bko_wpp (Reversão)
- ultrafibra_digital_ah_tct (TCT)

DEALER (dataset: tim_b2c):
- ultrafibra_dealer_api_blip_funil_analitico (funil chatbot)
- ultrafibra_dealer_ia_voz_funil (IA Voz - múltiplos registros por lead!)

AUXILIARES (dataset: tim_b2c_auxiliares):
- ultrafibra_digital_etapas_funil
- ultrafibra_dealer_etapas_funil_chatbot
- ultrafibra_dealer_funil_ia_voz

7. QUANDO USAR CADA FERRAMENTA
────────────────────────────────────────────────────────────────────────────────
consultar_dicionario → quando precisar saber estrutura/colunas de uma tabela
run_query → para buscar dados reais do BigQuery
list_tables → para explorar datasets

Use consultar_dicionario ANTES de queries complexas para garantir nomes corretos.

8. PRIVACIDADE
────────────────────────────────────────────────────────────────────────────────
!! REGRA ABSOLUTA !!
Pode usar dados pessoais internamente para cálculos agregados.
JAMAIS exiba valores individuais de: phone, cpf, identity, nome, endereço.

9. ESCOPO
────────────────────────────────────────────────────────────────────────────────
Apenas TIM B2C Ultrafibra Digital e Dealer. Outros produtos/segmentos: fora do escopo.

================================================================================`;

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  try {
    const { messages } = req.body;

    const response = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': process.env.ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01',
      },
      body: JSON.stringify({
        model: 'claude-sonnet-4-20250514',
        max_tokens: 8000,
        messages: messages,
        system: SYSTEM_PROMPT,
        tools: [
          {
            name: 'list_datasets',
            description: 'Lista datasets do BigQuery',
            input_schema: { type: 'object', properties: {} }
          },
          {
            name: 'list_tables',
            description: 'Lista tabelas de um dataset',
            input_schema: {
              type: 'object',
              properties: { dataset_id: { type: 'string' } },
              required: ['dataset_id']
            }
          },
          {
            name: 'consultar_dicionario',
            description: 'Consulta estrutura/colunas de tabelas no dicionário. Use ANTES de queries para garantir nomes corretos.',
            input_schema: {
              type: 'object',
              properties: {
                termo: { type: 'string', description: 'Nome da tabela ou termo relacionado (ex: conversas, funil, digital)' }
              },
              required: ['termo']
            }
          },
          {
            name: 'run_query',
            description: 'Executa SQL no BigQuery. Sempre filtre por date.',
            input_schema: {
              type: 'object',
              properties: { query: { type: 'string' } },
              required: ['query']
            }
          }
        ]
      }),
    });

    if (!response.ok) {
      const error = await response.text();
      console.error('Claude API error:', error);
      return res.status(response.status).json({ error: 'Erro na API' });
    }

    const data = await response.json();

    if (data.stop_reason === 'tool_use') {
      const toolUseBlocks = data.content.filter(b => b.type === 'tool_use');
      const toolResults = [];

      for (const toolUse of toolUseBlocks) {
        let result;
        
        try {
          console.log(`Executando: ${toolUse.name}`);
          
          if (toolUse.name === 'list_datasets') {
            const datasets = await listDatasets();
            result = `Datasets:\n${datasets.join('\n')}`;
          } 
          else if (toolUse.name === 'list_tables') {
            const tables = await listTables(toolUse.input.dataset_id);
            result = `Tabelas:\n${tables.join('\n')}`;
          }
          else if (toolUse.name === 'consultar_dicionario') {
            result = consultarDicionario(toolUse.input.termo);
          }
          else if (toolUse.name === 'run_query') {
            console.log('Query:', toolUse.input.query);
            const rows = await runQuery(toolUse.input.query);
            result = `${rows.length} linhas:\n${JSON.stringify(rows.slice(0, 100), null, 2)}`;
          }
          
        } catch (error) {
          console.error(`Erro ${toolUse.name}:`, error);
          result = `Erro: ${error.message}`;
        }

        toolResults.push({
          type: 'tool_result',
          tool_use_id: toolUse.id,
          content: result
        });
      }

      const followUpResponse = await fetch('https://api.anthropic.com/v1/messages', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'x-api-key': process.env.ANTHROPIC_API_KEY,
          'anthropic-version': '2023-06-01',
        },
        body: JSON.stringify({
          model: 'claude-sonnet-4-20250514',
          max_tokens: 8000,
          messages: [
            ...messages,
            { role: 'assistant', content: data.content },
            { role: 'user', content: toolResults }
          ],
          system: SYSTEM_PROMPT,
        }),
      });

      if (!followUpResponse.ok) {
        return res.status(500).json({ error: 'Erro ao processar resposta' });
      }

      const followUpData = await followUpResponse.json();
      return res.status(200).json(followUpData);
    }

    return res.status(200).json(data);
    
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: error.message });
  }
}
