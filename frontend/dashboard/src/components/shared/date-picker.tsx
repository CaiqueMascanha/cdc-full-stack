'use client';

import { format } from 'date-fns';
import { Calendar as CalendarIcon } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { Calendar } from '@/components/ui/calendar';
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from '@/components/ui/popover';
import { useDateStore } from '@/store/useDateStore';

export function DatePicker() {
  const date = useDateStore((state) => state.dateSelected);
  const setDateSelected = useDateStore((state) => state.setDateSelected);

  return (
    <Popover>
      <PopoverTrigger asChild>
        <Button
          variant="outline"
          data-empty={!date}
          className="w-[280px] justify-start text-left data-[empty=true]:text-muted-foreground font-mono"
        >
          <CalendarIcon />
          {date ? format(date, 'dd/MM/yyyy') : <span>Selecione uma data</span>}
        </Button>
      </PopoverTrigger>
      <PopoverContent className="w-auto p-0">
        <Calendar
          mode="single"
          selected={date ?? undefined}
          onSelect={(selectedDate) => {
            if (selectedDate) {
              setDateSelected(selectedDate);
            }
          }}
        />
      </PopoverContent>
    </Popover>
  );
}
