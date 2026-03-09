'use client';

import { useKafkaWebSocket } from '@/hooks/useKafkaWebSocket';
import { EmprestimoAprovado } from '@/types/emprestimos';

const TOPIC = 'cdc.public.emprestimos_aprovados_agregado_diario';

export function RealtimeFeed() {
  const { lastMessage, connected } =
    useKafkaWebSocket<EmprestimoAprovado>(TOPIC);

  return (
    <div className="p-4 border rounded-lg">
      {/* Status */}
      <div className="flex items-center gap-2 mb-4">
        <span
          className={`w-2 h-2 rounded-full ${connected ? 'bg-green-500' : 'bg-red-500'}`}
        />
        <span className="text-sm">
          {connected ? 'Conectado' : 'Desconectado'}
        </span>
      </div>

      {/* Última mensagem */}
      {lastMessage ? (
        <div className="p-3  rounded text-sm">
          <p>📅 {lastMessage.data_aprovado}</p>
          <p>
            📊 Aprovados:{' '}
            <strong>{lastMessage.total_emprestimos_aprovados}</strong>
          </p>
          <p>
            💰 Valor:{' '}
            <strong>
              R$ {lastMessage.total_valor_aprovado?.toLocaleString('pt-BR')}
            </strong>
          </p>
        </div>
      ) : (
        <p className="text-gray-400 text-sm">Aguardando mensagens...</p>
      )}
    </div>
  );
}
