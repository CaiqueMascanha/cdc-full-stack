import { create } from 'zustand';

interface DateStore {
  dateSelected: Date | null;
  setDateSelected: (date: Date) => void;
}

export const useDateStore = create<DateStore>((set, get) => ({
  dateSelected: null,

  setDateSelected: (date) => set({ dateSelected: date }),
}));
