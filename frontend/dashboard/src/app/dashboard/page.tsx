import { RealtimeFeed } from '@/components/dashboard/RealtimeFeed';

export default function DashboardPage() {
  return (
    <main className="p-8">
      <h1 className="text-2xl font-bold mb-6">Dashboard CDC em Tempo Real</h1>
      <RealtimeFeed />
    </main>
  );
}
