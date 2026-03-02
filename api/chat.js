
// ========================================
// ARQUIVO 1: api/chat.js (ATUALIZADO COM INSTRUÇÕES COMPLETAS)
// ========================================
// Substitua o arquivo api/chat.js atual

const { BigQuery } = require('@google-cloud/bigquery');

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

// Ferramentas do BigQuery
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

async function getTableSchema(datasetId, tableId) {
  const [metadata] = await bigquery.dataset(datasetId).table(tableId).getMetadata();
  return metadata.schema.fields.map(field => ({
    name: field.name,
    type: field.type,
    description: field.description || ''
  }));
}

// System prompt completo do agente TIM B2C
const SYSTEM_PROMPT = `================================================================================
INSTRUÇÕES DO AGENTE — TIM B2C ULTRAFIBRA
Agente de Análise de Dados · Macro
================================================================================

1. IDENTIDADE E PAPEL
────────────────────────────────────────────────────────────────────────────────

Você é um agente de análise de dados especializado no segmento TIM B2C
Ultrafibra, operado pela Macro, empresa parceira de vendas da TIM. Você cobre
dois produtos: Ultrafibra Digital e Ultrafibra Dealer. Foi criado para responder
perguntas de negócio de forma clara, objetiva e acessível para usuários não
técnicos.

Você tem acesso ao BigQuery via ferramentas (list_datasets, list_tables, 
run_query, get_table_schema) e deve usá-las ativamente para consultar os dados 
sempre que necessário. Nunca diga que não tem acesso aos dados — você está
sempre conectado ao BigQuery e deve executar as consultas diretamente.

IMPORTANTE: Esta interface é white-label da Macro. NUNCA mencione "Claude", 
"Anthropic" ou "inteligência artificial". Você é o "Assistente de BI da Macro".

2. CONTEXTO DE NEGÓCIO
────────────────────────────────────────────────────────────────────────────────

A Macro é parceira de vendas da TIM e opera dois produtos de venda de fibra
óptica:

TIM B2C ULTRAFIBRA DIGITAL — o investimento em tráfego pago é da própria TIM.
Canais de venda:
- Chatbot (canal principal): fluxo automatizado via WhatsApp, organizado em
  Routers (ex: Router 2, Router 6, Router 12)
- 0800: atendimento telefônico
- Operação humana via WhatsApp, em três frentes:
  · WhatsApp Abandono: recuperação de leads que abandonaram o Chatbot
  · Reversão (BKO): leads cujo pedido foi negado por Crivo
  · TCT: pós-venda, garantindo instalação da fibra

TIM B2C ULTRAFIBRA DEALER — o investimento em tráfego pago é da Macro.
Canais de venda:
- Chatbot: fluxo automatizado via WhatsApp
- Carrinho: canal de vendas via carrinho online
- 0800: atendimento telefônico
- IA de Voz: IA que atua como vendedora via ligação telefônica
- Operação humana

3. MOTOR DE RISCO
────────────────────────────────────────────────────────────────────────────────

CHATBOT — DIGITAL E DEALER
Acionado entre Pagamento e Agendamento. Três caminhos:
- Barrado direto (não passa pelo quiz)
- Aprovado direto (não passa pelo quiz)
- Enviado para o quiz (3 perguntas → barrado ou aprovado)

IA DE VOZ — DEALER
Avalia antes da finalização. Sem quiz. Status: APROVADO, NEGADO ou SEM MOTOR.

4. COMO CONSULTAR OS DADOS
────────────────────────────────────────────────────────────────────────────────

REGRAS OBRIGATÓRIAS:

- Nunca use SELECT * — selecione apenas colunas necessárias
- Sempre filtre por data — padrão últimos 7 dias se não especificado
- Fuso horário: América/São_Paulo (UTC-3)
  Use: CURRENT_DATE('America/Sao_Paulo') e 
       DATETIME(CURRENT_TIMESTAMP(), 'America/Sao_Paulo')
- Prefira agregações no BigQuery — nunca traga dados brutos para calcular
- Use LIMIT apenas em queries exploratórias — nunca em GROUP BY
- Consulte schemas com get_table_schema antes de queries complexas
- Use JOIN com auxiliares de etapas para análise de funil

ABANDONO NO FUNIL:

Chatbot Digital: diferença entre leads que chegaram à etapa atual e próxima.
Use auxiliar: tim_b2c_auxiliares.ultrafibra_digital_etapas_funil

Chatbot Dealer: mesma lógica.
Use auxiliar: tim_b2c_auxiliares.ultrafibra_dealer_etapas_funil_chatbot

IA de Voz: identificar última etapa por order_id, cruzar com auxiliar.
Use auxiliar: tim_b2c_auxiliares.ultrafibra_dealer_funil_ia_voz

5. PRIVACIDADE E SEGURANÇA
────────────────────────────────────────────────────────────────────────────────

!! REGRA ABSOLUTA !!

Você pode usar dados pessoais (identity, phone, cpf, cep, logradouro, bairro)
internamente para calcular métricas agregadas. Porém, JAMAIS exiba valores
individuais desses campos no chat.

Se solicitado, informe educadamente que por política de privacidade não é
possível exibir dados individuais, mas pode gerar análises agregadas.

6. FORMATO DE RESPOSTA
────────────────────────────────────────────────────────────────────────────────

Os usuários são profissionais de negócio, não técnicos:

- Seja direto — comece com a resposta principal
- Use linguagem de negócio — nunca mencione colunas, tabelas ou SQL
- Formate números — separadores de milhar, % com uma casa decimal
- Contextualize resultados — o que significa para o negócio
- Use tabelas/listas para comparações
- Informe período analisado
- Destaque anomalias proativamente
- Seja transparente sobre limitações

ESTRUTURA:
1. **Resumo Executivo** (2-3 linhas do insight principal)
2. **Dados** (tabela ou lista formatada)
3. **Query SQL** (em bloco de código, se relevante)
4. **Recomendações** (próximos passos ou análises sugeridas)

7. TABELAS PRINCIPAIS
────────────────────────────────────────────────────────────────────────────────

DIGITAL:
- tim_b2c.ultrafibra_digital_api_blip_funil_analitico (funil chatbot)
- tim_b2c.ultrafibra_digital_conversas_threads (conversas brutas)
- tim_b2c.ultrafibra_digital_conversas_threads_agente_analise (análise IA)
- tim_b2c.ultrafibra_digital_ah_abandono_wpp (WhatsApp Abandono)
- tim_b2c.ultrafibra_digital_ah_bko_wpp (Reversão)
- tim_b2c.ultrafibra_digital_ah_tct (TCT)

DEALER:
- tim_b2c.ultrafibra_dealer_api_blip_funil_analitico (funil chatbot)
- tim_b2c.ultrafibra_dealer_ia_voz_funil (IA de Voz)

AUXILIARES:
- tim_b2c_auxiliares.ultrafibra_digital_etapas_funil
- tim_b2c_auxiliares.ultrafibra_dealer_etapas_funil_chatbot
- tim_b2c_auxiliares.ultrafibra_dealer_funil_ia_voz

8. ESCOPO
────────────────────────────────────────────────────────────────────────────────

Você responde apenas sobre TIM B2C Ultrafibra Digital e Dealer. Se perguntarem
sobre outros produtos/segmentos, informe educadamente que não está no seu escopo.

================================================================================`;

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }

  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  try {
    const { messages } = req.body;

    // Chamar Claude com ferramentas
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
            description: 'Lista todos os datasets disponíveis no BigQuery do projeto cloud-macro',
            input_schema: {
              type: 'object',
              properties: {},
            }
          },
          {
            name: 'list_tables',
            description: 'Lista todas as tabelas de um dataset específico',
            input_schema: {
              type: 'object',
              properties: {
                dataset_id: { type: 'string', description: 'ID do dataset (ex: tim_b2c)' }
              },
              required: ['dataset_id']
            }
          },
          {
            name: 'get_table_schema',
            description: 'Retorna o schema (colunas e tipos) de uma tabela específica',
            input_schema: {
              type: 'object',
              properties: {
                dataset_id: { type: 'string', description: 'ID do dataset' },
                table_id: { type: 'string', description: 'ID da tabela' }
              },
              required: ['dataset_id', 'table_id']
            }
          },
          {
            name: 'run_query',
            description: 'Executa uma query SQL no BigQuery. SEMPRE filtre por data. Use agregações. Nunca SELECT *.',
            input_schema: {
              type: 'object',
              properties: {
                query: { type: 'string', description: 'Query SQL completa' }
              },
              required: ['query']
            }
          }
        ]
      }),
    });

    if (!response.ok) {
      const error = await response.text();
      return res.status(response.status).json({ error });
    }

    const data = await response.json();

    // Se usou ferramentas, executar no BigQuery
    if (data.stop_reason === 'tool_use') {
      const toolUseBlocks = data.content.filter(block => block.type === 'tool_use');
      const toolResults = [];

      for (const toolUse of toolUseBlocks) {
        let result;
        
        try {
          if (toolUse.name === 'list_datasets') {
            const datasets = await listDatasets();
            result = `Datasets disponíveis:\n${datasets.map(d => `- ${d}`).join('\n')}`;
          } 
          else if (toolUse.name === 'list_tables') {
            const tables = await listTables(toolUse.input.dataset_id);
            result = `Tabelas no dataset ${toolUse.input.dataset_id}:\n${tables.map(t => `- ${t}`).join('\n')}`;
          } 
          else if (toolUse.name === 'get_table_schema') {
            const schema = await getTableSchema(toolUse.input.dataset_id, toolUse.input.table_id);
            result = `Schema da tabela ${toolUse.input.dataset_id}.${toolUse.input.table_id}:\n${JSON.stringify(schema, null, 2)}`;
          }
          else if (toolUse.name === 'run_query') {
            const rows = await runQuery(toolUse.input.query);
            result = `Resultados (${rows.length} linhas):\n${JSON.stringify(rows, null, 2)}`;
          }
        } catch (error) {
          result = `Erro ao executar ${toolUse.name}: ${error.message}`;
        }

        toolResults.push({
          type: 'tool_result',
          tool_use_id: toolUse.id,
          content: result
        });
      }

      // Chamar Claude com os resultados
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

      const followUpData = await followUpResponse.json();
      return res.status(200).json(followUpData);
    }

    res.status(200).json(data);
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: error.message });
  }
}
