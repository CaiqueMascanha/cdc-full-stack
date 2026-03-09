import { useEffect, useRef, useState } from 'react';

export function useKafkaWebSocket<T>(topic: string) {
  const [lastMessage, setLastMessage] = useState<T | null>(null);
  const [connected, setConnected] = useState(false);
  const wsRef = useRef<WebSocket | null>(null);

  useEffect(() => {
    const url = `${process.env.NEXT_PUBLIC_WS_URL}/ws/${topic}`;
    const ws = new WebSocket(url);
    wsRef.current = ws;

    ws.onopen = () => setConnected(true);
    ws.onclose = () => setConnected(false);

    ws.onmessage = (event) => {
      try {
        const data: T = JSON.parse(event.data);
        setLastMessage(data);
      } catch {
        console.error('Erro ao parsear mensagem:', event.data);
      }
    };

    return () => ws.close();
  }, [topic]);

  return { lastMessage, connected };
}
