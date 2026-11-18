/**
 * The order the scheduler will actually dispatch pending queue items in.
 *
 * The backend decides this in `queue_rules.queue_order_key`: position, or with
 * Shortest-Job-First on, jumped items first (by position), then print time
 * ascending with unknown last, then position; and a scheduled job whose time
 * has come ahead of everything else, earliest first. It is one order
 * across the whole queue, not one per printer (#3200): a job pinned to a
 * printer and an "Any <model>" job both want that printer, and the one higher
 * in this order gets it. Every UI surface that claims to show queue order has
 * to reproduce it, and each one that reproduced it privately drifted: the
 * timeline sorted by `position` alone and so ignored Shortest-Job-First
 * entirely (#3043).
 */

import { parseUTCDate } from './date';

interface OrderableQueueItem {
  printer_id?: number | null;
  target_model?: string | null;
  been_jumped?: boolean;
  print_time_seconds?: number | null;
  position: number;
  id?: number;
  scheduled_time?: string | null;
  manual_start?: boolean;
}

/** The time a scheduled job became due, or null if it has none or is not due yet. */
function dueAt(item: OrderableQueueItem, now: number): number | null {
  if (!item.scheduled_time || item.manual_start) return null;
  const at = parseUTCDate(item.scheduled_time)?.getTime();
  return at !== undefined && at <= now ? at : null;
}

/**
 * Order two pending items the way the scheduler takes them, whichever printer
 * or model each is queued for.
 *
 * @param sjfEnabled the `queue_shortest_first` setting. When off, the
 *                   scheduler orders by position alone and so does this.
 * @param now        the instant "due" is judged at; defaults to now.
 */
export function compareQueueOrder(
  a: OrderableQueueItem,
  b: OrderableQueueItem,
  sjfEnabled: boolean,
  now: number = Date.now(),
): number {
  // A scheduled job whose time has come goes first: it was promised that time.
  const aDue = dueAt(a, now);
  const bDue = dueAt(b, now);
  if (aDue !== null || bDue !== null) {
    if (aDue === null) return 1;
    if (bDue === null) return -1;
    if (aDue !== bDue) return aDue - bDue;
  }

  if (sjfEnabled) {
    // Starvation guard: an item something else was allowed to jump ahead of
    // goes first next time, whatever the print times say.
    const aJumped = a.been_jumped ? 1 : 0;
    const bJumped = b.been_jumped ? 1 : 0;
    if (aJumped !== bJumped) return bJumped - aJumped;

    // Shortest first, and an item whose duration we don't know yet sorts last
    // rather than winning by looking like a zero-second print (NULLS LAST).
    // Jumped items among themselves go by position: by print time again, a
    // long one would be overtaken by every shorter one jumped after it.
    if (!aJumped) {
      const aTime = a.print_time_seconds ?? Infinity;
      const bTime = b.print_time_seconds ?? Infinity;
      if (aTime !== bTime) return aTime - bTime;
    }
  }

  if (a.position !== b.position) return a.position - b.position;
  return (a.id ?? 0) - (b.id ?? 0);
}
