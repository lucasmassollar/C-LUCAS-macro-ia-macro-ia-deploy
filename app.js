
let messages = [];
let isLoading = false;

function setQuestion(text) {
    document.getElementById('question-input').value = text;
    document.getElementById('question-input').focus();
}

function addMessage(role, content) {
    const welcomeScreen = document.getElementById('welcome-screen');
    if (welcomeScreen) welcomeScreen.remove();

    const chatMessages = document.getElementById('chat-messages');
    const messageDiv = document.createElement('div');
    messageDiv.className = `flex ${role === 'user' ? 'justify-end' : 'justify-start'}`;
    
    // Formatar markdown
    let formattedContent = content;
    if (role === 'assistant') {
        // SQL blocks
        formattedContent = content.replace(/```sql\n([\s\S]*?)```/g, (match, code) => {
            return `<div class="bg-gray-900 rounded-lg p-3 my-3 font-mono text-xs text-cyan-400 overflow-x-auto border border-gray-300">${escapeHtml(code.trim())}</div>`;
        });
        
        // Generic code blocks
        formattedContent = formattedContent.replace(/```([\s\S]*?)```/g, (match, code) => {
            return `<div class="bg-gray-900 rounded-lg p-3 my-3 font-mono text-xs text-gray-300 overflow-x-auto border border-gray-300">${escapeHtml(code.trim())}</div>`;
        });

        // Bold
        formattedContent = formattedContent.replace(/\*\*(.*?)\*\*/g, '<strong class="text-gray-900 font-semibold">$1</strong>');
        
        // Tables (Markdown style: | col1 | col2 |)
        const lines = formattedContent.split('\n');
        let inTable = false;
        let tableHtml = '';
        let newContent = '';
        
        for (let i = 0; i < lines.length; i++) {
            const line = lines[i].trim();
            
            if (line.startsWith('|') && line.endsWith('|')) {
                if (!inTable) {
                    inTable = true;
                    tableHtml = '<table class="w-full my-3 text-sm border-collapse border border-gray-300"><tbody>';
                }
                
                const cells = line.split('|').filter(cell => cell.trim());
                const isHeader = i < lines.length - 1 && lines[i + 1].includes('---');
                
                if (!isHeader && !line.includes('---')) {
                    tableHtml += '<tr>';
                    cells.forEach(cell => {
                        tableHtml += `<td class="border border-gray-300 px-3 py-2 bg-white">${cell.trim()}</td>`;
                    });
                    tableHtml += '</tr>';
                }
            } else {
                if (inTable) {
                    tableHtml += '</tbody></table>';
                    newContent += tableHtml;
                    tableHtml = '';
                    inTable = false;
                }
                newContent += line + '\n';
            }
        }
        
        if (inTable) {
            tableHtml += '</tbody></table>';
            newContent += tableHtml;
        }
        
        formattedContent = newContent || formattedContent;
        
        // Line breaks
        formattedContent = formattedContent.replace(/\n/g, '<br>');
    }
    
    messageDiv.innerHTML = `
        <div class="max-w-3xl ${role === 'user' ? 'ml-12' : 'mr-12'}">
            <div class="flex items-start gap-3 mb-1">
                ${role === 'assistant' ? `
                    <div class="w-8 h-8 bg-gradient-to-br from-blue-600 to-cyan-600 rounded-lg flex items-center justify-center flex-shrink-0">
                        <svg class="w-4 h-4 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z"></path>
                        </svg>
                    </div>
                ` : ''}
                <div class="flex-1 ${role === 'user' ? 'text-right' : ''}">
                    ${role === 'assistant' ? `
                        <div class="text-xs text-gray-500 mb-1">Oráculo Insight</div>
                    ` : ''}
                    <div class="rounded-2xl px-5 py-4 ${
                        role === 'user'
                            ? 'bg-gradient-to-r from-blue-600 to-cyan-600 text-white inline-block'
                            : 'bg-white text-gray-900 border border-gray-200 shadow-sm'
                    }">
                        ${role === 'user' 
                            ? `<div class="text-sm">${escapeHtml(content)}</div>`
                            : `<div class="text-sm leading-relaxed">${formattedContent}</div>`
                        }
                    </div>
                </div>
            </div>
        </div>
    `;
    
    chatMessages.appendChild(messageDiv);
    chatMessages.scrollTop = chatMessages.scrollHeight;
}

function escapeHtml(text) {
    const map = {
        '&': '&amp;',
        '<': '&lt;',
        '>': '&gt;',
        '"': '&quot;',
        "'": '&#039;'
    };
    return text.replace(/[&<>"']/g, m => map[m]);
}

function showLoading() {
    const chatMessages = document.getElementById('chat-messages');
    const loadingDiv = document.createElement('div');
    loadingDiv.id = 'loading-indicator';
    loadingDiv.className = 'flex justify-start';
    loadingDiv.innerHTML = `
        <div class="flex items-start gap-3">
            <div class="w-8 h-8 bg-gradient-to-br from-blue-600 to-cyan-600 rounded-lg flex items-center justify-center">
                <svg class="w-4 h-4 text-white animate-spin" fill="none" viewBox="0 0 24 24">
                    <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
                    <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                </svg>
            </div>
            <div class="bg-white border border-gray-200 rounded-2xl px-5 py-4 shadow-sm">
                <div class="flex items-center gap-2">
                    <span class="text-sm text-gray-600">Analisando dados do BigQuery...</span>
                </div>
            </div>
        </div>
    `;
    chatMessages.appendChild(loadingDiv);
    chatMessages.scrollTop = chatMessages.scrollHeight;
}

function hideLoading() {
    const loadingDiv = document.getElementById('loading-indicator');
    if (loadingDiv) loadingDiv.remove();
}

function showError(message) {
    const chatMessages = document.getElementById('chat-messages');
    const errorDiv = document.createElement('div');
    errorDiv.className = 'p-4 bg-red-50 border border-red-200 rounded-xl';
    errorDiv.innerHTML = `
        <div class="flex items-center gap-2 text-red-700 text-sm">
            <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"></path>
            </svg>
            <span>${escapeHtml(message)}</span>
        </div>
    `;
    chatMessages.appendChild(errorDiv);
    setTimeout(() => errorDiv.remove(), 5000);
}

async function sendMessage() {
    if (isLoading) return;

    const input = document.getElementById('question-input');
    const question = input.value.trim();
    if (!question) return;

    isLoading = true;
    const sendButton = document.getElementById('send-button');
    sendButton.disabled = true;
    input.disabled = true;

    addMessage('user', question);
    messages.push({ role: 'user', content: question });
    input.value = '';

    showLoading();

    try {
        const response = await fetch('/api/chat', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                messages: messages
            })
        });

        hideLoading();

        if (!response.ok) {
            const errorData = await response.json().catch(() => ({}));
            throw new Error(errorData.error || `Erro ${response.status}`);
        }

        const data = await response.json();
        
        // Extrair texto da resposta
        const textBlocks = data.content.filter(block => block.type === 'text');
        const content = textBlocks.map(block => block.text).join('\n\n');
        
        if (content.trim()) {
            addMessage('assistant', content);
            messages.push({ role: 'assistant', content: data.content });
        } else {
            throw new Error('Resposta vazia do assistente');
        }
        
    } catch (error) {
        hideLoading();
        console.error('Error:', error);
        showError(error.message || 'Erro ao processar sua solicitação');
    } finally {
        isLoading = false;
        sendButton.disabled = false;
        input.disabled = false;
        input.focus();
    }
}

// Enter to send
document.getElementById('question-input').addEventListener('keypress', (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
        e.preventDefault();
        sendMessage();
    }
});


