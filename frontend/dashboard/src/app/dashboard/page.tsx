'use client';

import { RealtimeFeed } from '@/components/dashboard/RealtimeFeed';
import { DatePicker } from '@/components/shared/date-picker';
import { useDateStore } from '@/store/useDateStore';
import { format } from 'date-fns';

export default function DashboardPage() {
  const dateSelected = useDateStore((state) => state.dateSelected);

  return (
    <main className="p-8">
      <div className="flex justify-between items-center rounded-xl shadow-sm border-gray-100 p-4">
        <div className="flex flex-col items-start justify-center">
          <h2 className="tracking-tight text-2xl font-bold text-gray-900 uppercase">
            Painel de Solicitações
          </h2>
          <p className="tracking-widest text-sm font-mono text-gray-500">
            Acompanhamento de emissão de empréstimos
          </p>
        </div>

        <div className="flex items-center gap-2">
          <p className="tracking-widest text-sm font-mono text-gray-500">
            Status:{' '}
          </p>

          <DatePicker />

          <p className="text-sm font-mono">
            Data selecionada:{' '}
            {dateSelected ? format(dateSelected, 'dd/MM/yyyy') : 'Nenhuma'}
          </p>
        </div>
      </div>
      <h1 className="text-2xl font-bold mb-6">Dashboard CDC em Tempo Real</h1>
      <RealtimeFeed />
    </main>
  );
}
