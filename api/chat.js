
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

async function runQuery(query) {
  const [job] = await bigquery.createQueryJob({
    query: query,
    location: process.env.BIGQUERY_LOCATION || 'us-central1',
  });
  const [rows] = await job.getQueryResults();
  return rows;
}

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

Você tem acesso ao BigQuery e pode executar queries SQL quando necessário.
Esta é uma interface white-label da Macro. NUNCA mencione "Claude",
"Anthropic" ou "inteligência artificial". Você é o "Oráculo Insight da Macro".

2. CONTEXTO DE NEGÓCIO
────────────────────────────────────────────────────────────────────────────────

A Macro é parceira de vendas da TIM e opera dois produtos de venda de fibra óptica:

TIM B2C ULTRAFIBRA DIGITAL — o investimento em tráfego pago é da própria TIM.
TIM B2C ULTRAFIBRA DEALER — o investimento em tráfego pago é da Macro.

3. REGRAS DE QUERY
────────────────────────────────────────────────────────────────────────────────

- Sempre filtre por data (padrão: últimos 7 dias)
- Fuso: CURRENT_DATE('America/Sao_Paulo')
- Nunca use SELECT *
- NUNCA exiba phone, cpf, identity individuais
- Use \`cloud-macro.tim_b2c.nome_tabela\`

4. QUANDO PRECISAR DE DADOS DO BIGQUERY
────────────────────────────────────────────────────────────────────────────────

Quando precisar consultar dados, responda no formato:

<SQL>
SELECT ...
FROM \`cloud-macro.tim_b2c.tabela\`
WHERE date >= DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 7 DAY)
</SQL>

O sistema vai executar a query automaticamente e te retornar os dados.

5. TABELAS PRINCIPAIS
────────────────────────────────────────────────────────────────────────────────

cloud-macro.tim_b2c.ultrafibra_digital_api_blip_funil_analitico
- Funil chatbot Digital
- Colunas: identity, date, hour, router, origem, etapa_boas_vindas, etapa_finalizacao

cloud-macro.tim_b2c.ultrafibra_digital_conversas_threads
- Conversas Digital
- Colunas: id_conversa, phone, router, data, mensagem_cliente

cloud-macro.tim_b2c.ultrafibra_digital_conversas_threads_agente_analise
- Análise conversas
- Colunas: id_conversa, router, data, sentimento, assunto_agrupado

cloud-macro.tim_b2c.ultrafibra_dealer_api_blip_funil_analitico
- Funil chatbot Dealer

6. FORMATO DE RESPOSTA
────────────────────────────────────────────────────────────────────────────────

1. Resumo executivo
2. Dados (tabela)
3. Query SQL (bloco código)
4. Recomendações

================================================================================`;

function extractSQL(text) {
  const match = text.match(/<SQL>([\s\S]*?)<\/SQL>/);
  return match ? match[1].trim() : null;
}

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  try {
    const { messages } = req.body;

    // Primeira chamada ao Claude via OpenRouter
    const response = await fetch('https://openrouter.ai/api/v1/chat/completions', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${process.env.OPENROUTER_API_KEY}`,
        'HTTP-Referer': 'https://vercel.app',
        'X-Title': 'Oraculo Insight'
      },
      body: JSON.stringify({
        model: 'anthropic/claude-sonnet-4',
        messages: [
          { role: 'system', content: SYSTEM_PROMPT },
          ...messages.map(m => ({
            role: m.role,
            content: typeof m.content === 'string' ? m.content : JSON.stringify(m.content)
          }))
        ],
        max_tokens: 4000,
        temperature: 0.7
      }),
    });

    if (!response.ok) {
      const errorText = await response.text();
      console.error('Erro OpenRouter:', response.status, errorText);
      return res.status(response.status).json({ 
        error: `Erro ${response.status}: ${errorText.substring(0, 200)}` 
      });
    }

    const data = await response.json();
    const assistantMessage = data.choices?.[0]?.message?.content || '';

    // Verificar se tem SQL para executar
    const sqlQuery = extractSQL(assistantMessage);
    
    if (sqlQuery) {
      console.log('Executando SQL:', sqlQuery.substring(0, 150));
      
      try {
        const rows = await runQuery(sqlQuery);
        console.log(`Retornou ${rows.length} linhas`);
        
        // Segunda chamada com os dados
        const followUpResponse = await fetch('https://openrouter.ai/api/v1/chat/completions', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${process.env.OPENROUTER_API_KEY}`,
            'HTTP-Referer': 'https://vercel.app',
            'X-Title': 'Oraculo Insight'
          },
          body: JSON.stringify({
            model: 'anthropic/claude-sonnet-4',
            messages: [
              { role: 'system', content: SYSTEM_PROMPT },
              ...messages.map(m => ({
                role: m.role,
                content: typeof m.content === 'string' ? m.content : JSON.stringify(m.content)
              })),
              { role: 'assistant', content: assistantMessage },
              { role: 'user', content: `Dados da query:\n${JSON.stringify(rows.slice(0, 100), null, 2)}\n\nAgora analise esses dados e responda de forma executiva.` }
            ],
            max_tokens: 4000,
            temperature: 0.7
          }),
        });

        if (!followUpResponse.ok) {
          return res.status(200).json({
            content: [{
              type: 'text',
              text: `Query executada com sucesso. Retornou ${rows.length} linhas:\n\n${JSON.stringify(rows.slice(0, 10), null, 2)}`
            }]
          });
        }

        const followUpData = await followUpResponse.json();
        const finalMessage = followUpData.choices?.[0]?.message?.content || '';

        return res.status(200).json({
          content: [{
            type: 'text',
            text: finalMessage
          }]
        });

      } catch (error) {
        console.error('Erro SQL:', error);
        return res.status(200).json({
          content: [{
            type: 'text',
            text: `Erro ao executar query: ${error.message}\n\nQuery:\n${sqlQuery}`
          }]
        });
      }
    }

    // Se não tem SQL, retorna direto
    return res.status(200).json({
      content: [{
        type: 'text',
        text: assistantMessage
      }]
    });
    
  } catch (error) {
    console.error('ERRO GERAL:', error.message);
    res.status(500).json({ error: `Erro interno: ${error.message}` });
  }
}
