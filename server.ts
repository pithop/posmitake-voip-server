import { WebSocketServer, WebSocket } from 'ws';

const port = process.env.PORT ? parseInt(process.env.PORT) : 5000;
const wss = new WebSocketServer({ port });

console.log(`📡 VoIP Signaling Server running on port ${port}`);

// --- Types ---
interface ClientEntry {
    ws: WebSocket;
    isAlive: boolean;
}

// Pour garder trace de qui est connecté
const clients = new Map<string, ClientEntry>();

// --- Heartbeat : purge les connexions zombie toutes les 30 secondes ---
// Un client qui ne répond pas au ping en 30s est considéré mort et supprimé.
const heartbeatInterval = setInterval(() => {
    clients.forEach((entry, id) => {
        if (!entry.isAlive) {
            console.log(`💀 Connexion zombie purgée : ${id}`);
            clients.delete(id);
            entry.ws.terminate();
            // Notifier tous les autres que ce client n'est plus disponible
            broadcast({ type: 'peer-disconnected', peerId: id });
            return;
        }
        entry.isAlive = false;
        try {
            entry.ws.ping();
        } catch (_) {
            // ignore, la prochaine itération purgera le client
        }
    });
}, 30000);

wss.on('close', () => {
    clearInterval(heartbeatInterval);
});

function broadcast(data: object) {
    const msg = JSON.stringify(data);
    clients.forEach(({ ws }) => {
        if (ws.readyState === WebSocket.OPEN) {
            ws.send(msg);
        }
    });
}

wss.on('connection', (ws: WebSocket) => {
    let clientId: string | null = null;
    console.log('🔗 New connection attempt');

    // Marquer comme vivant dès l'arrivée du pong
    (ws as any).isAlive = true;
    ws.on('pong', () => {
        if (clientId && clients.has(clientId)) {
            clients.get(clientId)!.isAlive = true;
        }
    });

    ws.on('message', (message: Buffer | string) => {
        try {
            const data = JSON.parse(message.toString());

            switch (data.type) {
                // Enregistrement d'un terminal POS
                case 'register':
                    if (data.id) {
                        const newId = String(data.id);

                        // Si un ancien socket avec ce même ID existe, on le remplace silencieusement.
                        // CRITIQUE : on n'appelle PAS old.ws.close() — cela enverrait un close frame
                        // au client, qui déclencherait onclose → scheduleReconnect → nouvelle connexion
                        // → re-registration → close à nouveau → boucle infinie de reconnexion.
                        // Le heartbeat ping/pong s'occupera de purger les zombies naturellement.
                        if (clients.has(newId)) {
                            console.log(`♻️  Re-registration of ${newId}, replacing silently.`);
                            clients.delete(newId);
                        }

                        clientId = newId;
                        clients.set(clientId, { ws, isAlive: true });
                        console.log(`✅ Registered POS terminal: ${clientId}`);
                        ws.send(JSON.stringify({ type: 'registered', id: clientId }));
                    }
                    break;

                // Relais P2P pour SDP Offers, Answers, ICE Candidates, et Raccrochage
                case 'offer':
                case 'answer':
                case 'ice-candidate':
                case 'bye':
                    if (data.target) {
                        const targetEntry = clients.get(data.target);
                        if (targetEntry && targetEntry.ws.readyState === WebSocket.OPEN) {
                            console.log(`🔄 Relaying ${data.type} from ${clientId} to ${data.target}`);
                            data.source = clientId;
                            targetEntry.ws.send(JSON.stringify(data));
                        } else {
                            // CRITIQUE : Prévenir l'appelant que la cible est injoignable
                            console.log(`⚠️ Target ${data.target} not found for ${data.type} from ${clientId}`);
                            if (ws.readyState === WebSocket.OPEN) {
                                ws.send(JSON.stringify({ type: 'peer-unavailable', target: data.target }));
                            }
                        }
                    }
                    break;

                default:
                    console.log(`❓ Unknown message type: ${data.type}`);
                    break;
            }
        } catch (error) {
            console.error('❌ Error parsing message:', error, 'Message was:', message.toString().substring(0, 200));
        }
    });

    ws.on('close', () => {
        if (clientId) {
            console.log(`🔌 Disconnected: ${clientId}`);
            // Ne supprimer que si c'est bien le même socket (pas déjà remplacé par ré-inscription)
            const entry = clients.get(clientId);
            if (entry && entry.ws === ws) {
                clients.delete(clientId);
                // Prévenir les autres que ce peer est parti
                broadcast({ type: 'peer-disconnected', peerId: clientId });
            }
        } else {
            console.log('🔌 Disconnected: unknown client (never registered)');
        }
    });

    ws.on('error', (err) => {
        console.error(`WebSocket Error for ${clientId || 'unknown'}:`, err.message);
    });
});
