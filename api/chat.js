
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

const SYSTEM_PROMPT = `Você é o Analista de Dados da Macro, especializado em TIM B2C Ultrafibra.
NUNCA mencione Claude, Anthropic ou IA.

DATASETS E TABELAS (BigQuery cloud-macro):

tim_b2c.ultrafibra_digital_api_blip_funil_analitico
- Funil chatbot Digital. Colunas principais: identity, date, hour, router, origem, etapa_boas_vindas, etapa_cep, etapa_finalizacao, etc.

tim_b2c.ultrafibra_digital_conversas_threads
- Conversas Digital. Colunas: id_conversa, phone, router, data, hora, mensagem_cliente, mensagem_bot

tim_b2c.ultrafibra_digital_conversas_threads_agente_analise
- Análise conversas. Colunas: id_conversa, router, data, sentimento, assunto_agrupado

tim_b2c.ultrafibra_dealer_api_blip_funil_analitico
- Funil chatbot Dealer

tim_b2c.ultrafibra_dealer_ia_voz_funil
- IA Voz (múltiplos registros por lead - usar order_id)

REGRAS:
- Sempre filtrar por date/data (padrão: últimos 7 dias)
- Fuso: CURRENT_DATE('America/Sao_Paulo')
- Nunca SELECT *
- NUNCA exibir phone, cpf, identity individuais
- Use \`cloud-macro.tim_b2c.nome_tabela\` no FROM

FORMATO RESPOSTA:
1. Resumo executivo
2. Dados (tabela)
3. Query SQL
4. Recomendações

Quando perguntar sobre "conversas", use ultrafibra_digital_conversas_threads.
Quando perguntar sobre "vendas/funil", use ultrafibra_digital_api_blip_funil_analitico.`;

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  try {
    const { messages } = req.body;

    console.log('=== Chamada 1: Pergunta do usuário ===');

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
            description: 'Lista datasets',
            input_schema: { type: 'object', properties: {} }
          },
          {
            name: 'list_tables',
            description: 'Lista tabelas',
            input_schema: {
              type: 'object',
              properties: { dataset_id: { type: 'string' } },
              required: ['dataset_id']
            }
          },
          {
            name: 'run_query',
            description: 'Executa SQL',
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
      console.error('Erro chamada 1:', response.status);
      return res.status(response.status).json({ error: 'Erro na API' });
    }

    const data = await response.json();
    console.log('Stop reason chamada 1:', data.stop_reason);

    // Se não usou ferramentas, retorna direto
    if (data.stop_reason !== 'tool_use') {
      console.log('Resposta direta (sem ferramentas)');
      return res.status(200).json(data);
    }

    // Executar ferramentas
    console.log('=== Executando ferramentas ===');
    const toolUseBlocks = data.content.filter(b => b.type === 'tool_use');
    console.log('Ferramentas:', toolUseBlocks.map(t => t.name).join(', '));
    
    const toolResults = [];

    for (const toolUse of toolUseBlocks) {
      let result;
      
      try {
        if (toolUse.name === 'list_datasets') {
          const datasets = await listDatasets();
          result = `Datasets:\n${datasets.join('\n')}`;
          console.log('✓ list_datasets OK');
        } 
        else if (toolUse.name === 'list_tables') {
          const tables = await listTables(toolUse.input.dataset_id);
          result = `Tabelas:\n${tables.join('\n')}`;
          console.log('✓ list_tables OK');
        }
        else if (toolUse.name === 'run_query') {
          console.log('Query:', toolUse.input.query.substring(0, 150) + '...');
          const rows = await runQuery(toolUse.input.query);
          console.log(`✓ run_query OK - ${rows.length} linhas`);
          
          // Limitar tamanho da resposta para não estourar payload
          const limitedRows = rows.slice(0, 100);
          result = `Retornou ${rows.length} linha(s). Primeiras ${limitedRows.length}:\n${JSON.stringify(limitedRows, null, 2)}`;
        }
      } catch (error) {
        console.error(`✗ Erro ${toolUse.name}:`, error.message);
        result = `Erro: ${error.message}`;
      }

      toolResults.push({
        type: 'tool_result',
        tool_use_id: toolUse.id,
        content: result
      });
    }

    // Segunda chamada com resultados
    console.log('=== Chamada 2: Processando resultados ===');

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
      console.error('Erro chamada 2:', followUpResponse.status);
      const errorText = await followUpResponse.text();
      console.error('Detalhes:', errorText);
      
      // Se der erro na segunda chamada, tenta retornar pelo menos os dados brutos
      return res.status(200).json({
        content: [{
          type: 'text',
          text: `Executei a consulta com sucesso, mas tive dificuldade em formatar a resposta.\n\nDados retornados:\n${toolResults[0]?.content?.substring(0, 500) || 'Sem dados'}`
        }]
      });
    }

    const followUpData = await followUpResponse.json();
    console.log('✓ Chamada 2 OK - Stop reason:', followUpData.stop_reason);
    
    // Validar se tem conteúdo de texto
    const hasText = followUpData.content.some(block => block.type === 'text' && block.text.trim());
    
    if (!hasText) {
      console.error('⚠ Resposta sem texto!');
      // Fallback: retornar dados brutos formatados
      return res.status(200).json({
        content: [{
          type: 'text',
          text: `Consulta executada. Resultados:\n\n${toolResults.map(r => r.content).join('\n\n')}`
        }]
      });
    }
    
    return res.status(200).json(followUpData);
    
  } catch (error) {
    console.error('ERRO GERAL:', error);
    res.status(500).json({ error: error.message });
  }
}
