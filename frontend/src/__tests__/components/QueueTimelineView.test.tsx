/**
 * The timeline draws pending jobs where the backend's projection puts them.
 *
 * It used to chain each lane's jobs itself, by its own copy of the queue order,
 * and each copy drifted from the scheduler: it ignored Shortest-Job-First
 * (#3043), and it knew nothing of a scheduled job holding its printer. The
 * order is now the projection's, which runs the scheduler's rules and is tested
 * against the scheduler itself; this file checks the drawing.
 */

import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { QueueTimelineView } from '../../components/QueueTimelineView';
import type { PrintQueueItem, Printer, QueueProjectionEntry } from '../../api/client';

const HOUR_MS = 3600 * 1000;

const printers = [
  { id: 1, name: 'Workshop X1C', model: 'X1C' },
  { id: 2, name: 'Garage P1S', model: 'P1S' },
] as unknown as Printer[];

const running = {
  id: 100,
  printer_id: 1,
  status: 'printing',
  position: 0,
  archive_name: 'Running now',
  print_time_seconds: 3600,
  started_at: new Date(Date.now() - 30 * 60 * 1000).toISOString(),
} as unknown as PrintQueueItem;

const pending = (id: number, name: string, over: Partial<PrintQueueItem> = {}) =>
  ({
    id,
    printer_id: null,
    target_model: 'P1S',
    status: 'pending',
    position: id,
    archive_name: name,
    print_time_seconds: 3600,
    scheduled_time: null,
    manual_start: false,
    waiting_reason: null,
    ...over,
  }) as unknown as PrintQueueItem;

const at = (hoursFromNow: number) => new Date(Date.now() + hoursFromNow * HOUR_MS).toISOString();

function renderTimeline(items: PrintQueueItem[], projection: QueueProjectionEntry[]) {
  render(
    <QueueTimelineView
      queueItems={items}
      printers={printers}
      printerStatuses={{ 1: { progress: 50, remaining_time: 30, state: 'RUNNING' } }}
      projection={projection}
      onItemClick={() => {}}
      t={(key: string) => key}
    />,
  );
}

/** Bar names in one lane, left to right. Bars are absolutely positioned, so the
 *  rendered offset is what the user reads, not DOM order. */
function barsLeftToRight(): string[] {
  return screen
    .getAllByRole('button')
    .filter((el) => el.style.left.endsWith('%'))
    .sort((a, b) => parseFloat(a.style.left) - parseFloat(b.style.left))
    .map((el) => (el.getAttribute('title') ?? '').split(' · ')[0]);
}

describe('QueueTimelineView', () => {
  it('draws pending jobs at their projected start, in that order', () => {
    renderTimeline(
      [running, pending(1, 'Later job'), pending(2, 'Sooner job')],
      [
        { item_id: 1, printer_id: 2, start: at(3), end: at(4) },
        { item_id: 2, printer_id: 2, start: at(1), end: at(2) },
      ],
    );
    expect(barsLeftToRight()).toEqual(['Running now', 'Sooner job', 'Later job']);
  });

  it('draws an "Any model" job on the printer it is projected to take', () => {
    renderTimeline([pending(1, 'Any P1S job')], [{ item_id: 1, printer_id: 2, start: at(1), end: at(2) }]);
    const bar = screen.getByTitle(/^Any P1S job/);
    expect(bar.closest('[data-lane]')?.getAttribute('data-lane')).toBe('printer:2');
  });

  it('leaves out a pending job the projection could not place', () => {
    renderTimeline([running, pending(1, 'Staged job', { manual_start: true })], []);
    expect(barsLeftToRight()).toEqual(['Running now']);
  });
});
