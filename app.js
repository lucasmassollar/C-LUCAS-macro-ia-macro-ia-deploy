// ========================================
// ARQUIVO 3: app.js
// ========================================
// Lógica do frontend
// Salve como: app.js

let messages = [];
let isLoading = false;

function addMessage(role, content) {
    const welcomeScreen = document.getElementById('welcome-screen');
    if (welcomeScreen) welcomeScreen.remove();

    const chatMessages = document.getElementById('chat-messages');
    const messageDiv = document.createElement('div');
    messageDiv.className = `flex ${role === 'user' ? 'justify-end' : 'justify-start'}`;
    
    messageDiv.innerHTML = `
        <div class="max-w-3xl ${role === 'user' ? 'ml-12' : 'mr-12'}">
            <div class="rounded-2xl px-5 py-4 ${
                role === 'user'
                    ? 'bg-gradient-to-br from-blue-600 to-cyan-600 text-white'
                    : 'bg-slate-700/50 text-slate-100 border border-slate-600/50'
            }">
                <pre class="whitespace-pre-wrap font-sans text-sm">${content}</pre>
            </div>
        </div>
    `;
    
    chatMessages.appendChild(messageDiv);
    chatMessages.scrollTop = chatMessages.scrollHeight;
}

async function sendMessage() {
    if (isLoading) return;

    const input = document.getElementById('question-input');
    const question = input.value.trim();
    if (!question) return;

    isLoading = true;
    document.getElementById('send-button').disabled = true;

    addMessage('user', question);
    messages.push({ role: 'user', content: question });
    input.value = '';

    try {
        const response = await fetch('/api/chat', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                messages: messages,
                system: 'Você é o assistente de BI da Macro. Nunca mencione Claude ou Anthropic.'
            })
        });

        const data = await response.json();
        const content = data.content.map(b => b.text || '').join('\n');
        
        addMessage('assistant', content);
        messages.push({ role: 'assistant', content: content });
    } catch (error) {
        console.error(error);
        addMessage('assistant', 'Erro ao processar sua solicitação.');
    } finally {
        isLoading = false;
        document.getElementById('send-button').disabled = false;
    }
}

document.getElementById('question-input').addEventListener('keypress', (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
        e.preventDefault();
        sendMessage();
    }
});
