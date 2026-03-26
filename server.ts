import { WebSocketServer, WebSocket } from 'ws';

const port = process.env.PORT ? parseInt(process.env.PORT) : 5000;
const wss = new WebSocketServer({ port });

console.log(`📡 VoIP Signaling Server running on port ${port}`);

// Pour garder trace de qui est connecté
const clients = new Map<string, WebSocket>();

wss.on('connection', (ws: WebSocket) => {
    let clientId: string | null = null;
    console.log('🔗 New connection attempt');

    ws.on('message', (message: string) => {
        try {
            const data = JSON.parse(message);

            switch (data.type) {
                // Enregistrement d'un terminal POS (par exemple : 'caisse_1', 'cuisine')
                case 'register':
                    if (data.id) {
                        clientId = String(data.id);
                        clients.set(clientId, ws);
                        console.log(`✅ Registered POS terminal: ${clientId}`);
                        ws.send(JSON.stringify({ type: 'registered', id: clientId }));
                    }
                    break;

                // Relais P2P pour SDP Offers, Answers, et ICE Candidates
                case 'offer':
                case 'answer':
                case 'ice-candidate':
                case 'bye':
                    if (data.target && clients.has(data.target)) {
                        console.log(`🔄 Relaying ${data.type} from ${clientId} to ${data.target}`);
                        const targetWs = clients.get(data.target)!;
                        // On attache la source pour que la cible sache de qui ca vient
                        data.source = clientId;
                        targetWs.send(JSON.stringify(data));
                    } else {
                        console.log(`⚠️ Target ${data.target} not found for ${data.type} from ${clientId}`);
                    }
                    break;

                default:
                    console.log(`❓ Unknown message type: ${data.type}`);
                    break;
            }
        } catch (error) {
            console.error('❌ Error parsing message:', error, 'Message was:', message.toString());
        }
    });

    ws.on('close', () => {
        if (clientId) {
            console.log(`🔌 Disconnected: ${clientId}`);
            clients.delete(clientId);
        } else {
            console.log('🔌 Disconnected: unknown client');
        }
    });

    ws.on('error', (err) => {
        console.error('WebSocket Error:', err);
    });
});
