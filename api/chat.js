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
  console.error('Aviso: dicionário não encontrado, continuando sem ele');
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

const SYSTEM_PROMPT = `================================================================================
AGENTE TIM B2C ULTRAFIBRA — ASSISTENTE DE BI DA MACRO
================================================================================

IDENTIDADE: Você é o Assistente de BI da Macro, especializado em TIM B2C Ultrafibra.
NUNCA mencione "Claude", "Anthropic" ou "IA".

ACESSO: BigQuery projeto cloud-macro via ferramentas (list_datasets, list_tables, run_query).

CONTEXTO:
- DIGITAL: Chatbot (Routers 2,6,12), 0800, WhatsApp Abandono/BKO/TCT
- DEALER: Chatbot, IA Voz, Carrinho

DATASETS E TABELAS PRINCIPAIS:

**tim_b2c** (Digital):
- ultrafibra_digital_api_blip_funil_analitico → funil chatbot (colunas: identity, date, hour, router, origem, etapa_boas_vindas, etapa_cep, etapa_finalizacao, etc)
- ultrafibra_digital_conversas_threads → mensagens (colunas: id_conversa, phone, router, data, hora, mensagem_cliente, mensagem_bot)
- ultrafibra_digital_conversas_threads_agente_analise → análise IA (colunas: id_conversa, router, data, sentimento, assunto_agrupado)
- ultrafibra_digital_ah_abandono_wpp → atendimento (colunas: agent_identity, storage_date, tags)

**tim_b2c** (Dealer):
- ultrafibra_dealer_api_blip_funil_analitico → funil chatbot
- ultrafibra_dealer_ia_voz_funil → IA Voz (ATENÇÃO: múltiplos registros por lead - usar order_id)

**tim_b2c_auxiliares**:
- ultrafibra_digital_etapas_funil → ordem do funil Digital
- ultrafibra_dealer_etapas_funil_chatbot → ordem do funil Dealer

REGRAS OBRIGATÓRIAS:
1. Sempre filtrar por date/data
2. Padrão: últimos 7 dias se não especificado
3. Fuso: CURRENT_DATE('America/Sao_Paulo')
4. Nunca SELECT * - apenas colunas necessárias
5. NUNCA exibir dados pessoais individuais (phone, cpf, identity, nome)
6. Use cloud-macro.tim_b2c.nome_tabela no FROM
7. Nunca use LIMIT em GROUP BY

FORMATO RESPOSTA:
1. Resumo executivo (2-3 linhas)
2. Dados (tabela formatada)
3. Query SQL (bloco código)
4. Recomendações

EXEMPLOS DE QUERIES CORRETAS:

-- Volume de conversas ontem no Digital
SELECT 
  COUNT(DISTINCT id_conversa) as total_conversas,
  router
FROM \`cloud-macro.tim_b2c.ultrafibra_digital_conversas_threads\`
WHERE data = DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 1 DAY)
GROUP BY router
ORDER BY total_conversas DESC

-- Finalizações no funil Digital últimos 7 dias
SELECT 
  date as data,
  COUNT(DISTINCT identity) as finalizacoes
FROM \`cloud-macro.tim_b2c.ultrafibra_digital_api_blip_funil_analitico\`
WHERE date >= DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 7 DAY)
  AND etapa_finalizacao = TRUE
GROUP BY date
ORDER BY date

IMPORTANTE: Quando usuário perguntar sobre "conversas", use a tabela de conversas (threads).
Quando perguntar sobre "vendas" ou "funil", use a tabela funil_analitico.

================================================================================`;

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  const startTime = Date.now();

  try {
    const { messages } = req.body;

    console.log('=== Nova requisição ===');
    console.log('Mensagens:', messages.length);

    const response = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': process.env.ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01',
      },
      body: JSON.stringify({
        model: 'claude-sonnet-4-20250514',
        max_tokens: 4000,
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
            name: 'run_query',
            description: 'Executa SQL no BigQuery',
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
      console.error('Erro Claude API:', response.status);
      return res.status(response.status).json({ error: 'Erro na API do Claude' });
    }

    const data = await response.json();
    console.log('Stop reason:', data.stop_reason);

    if (data.stop_reason === 'tool_use') {
      const toolUseBlocks = data.content.filter(b => b.type === 'tool_use');
      console.log('Ferramentas usadas:', toolUseBlocks.map(t => t.name));
      
      const toolResults = [];

      for (const toolUse of toolUseBlocks) {
        let result;
        
        try {
          if (toolUse.name === 'list_datasets') {
            const datasets = await listDatasets();
            result = `Datasets disponíveis:\n${datasets.join('\n')}`;
          } 
          else if (toolUse.name === 'list_tables') {
            const tables = await listTables(toolUse.input.dataset_id);
            result = `Tabelas no dataset ${toolUse.input.dataset_id}:\n${tables.join('\n')}`;
          }
          else if (toolUse.name === 'run_query') {
            console.log('Executando query:', toolUse.input.query.substring(0, 200));
            const rows = await runQuery(toolUse.input.query);
            console.log('Linhas retornadas:', rows.length);
            result = `Consulta retornou ${rows.length} linha(s):\n${JSON.stringify(rows, null, 2)}`;
          }
          
          console.log(`✓ ${toolUse.name} executado`);
        } catch (error) {
          console.error(`✗ Erro ${toolUse.name}:`, error.message);
          result = `Erro ao executar ${toolUse.name}: ${error.message}`;
        }

        toolResults.push({
          type: 'tool_result',
          tool_use_id: toolUse.id,
          content: result
        });
      }

      console.log('Enviando resultados de volta ao Claude...');

      const followUpResponse = await fetch('https://api.anthropic.com/v1/messages', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'x-api-key': process.env.ANTHROPIC_API_KEY,
          'anthropic-version': '2023-06-01',
        },
        body: JSON.stringify({
          model: 'claude-sonnet-4-20250514',
          max_tokens: 4000,
          messages: [
            ...messages,
            { role: 'assistant', content: data.content },
            { role: 'user', content: toolResults }
          ],
          system: SYSTEM_PROMPT,
        }),
      });

      if (!followUpResponse.ok) {
        console.error('Erro follow-up:', followUpResponse.status);
        return res.status(500).json({ error: 'Erro ao processar resposta' });
      }

      const followUpData = await followUpResponse.json();
      console.log('Resposta final recebida');
      console.log('Tempo total:', Date.now() - startTime, 'ms');
      
      return res.status(200).json(followUpData);
    }

    console.log('Resposta direta (sem ferramentas)');
    return res.status(200).json(data);
    
  } catch (error) {
    console.error('ERRO GERAL:', error);
    res.status(500).json({ error: error.message });
  }
}
