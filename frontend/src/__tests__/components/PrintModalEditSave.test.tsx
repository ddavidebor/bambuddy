/**
 * Saving an edited queue item reports and closes once.
 *
 * The update mutation toasted, called onSuccess and closed the dialog on its
 * own, and the submit handler then did all three again: two "Queue item
 * updated" toasts, two closes, and on a failure two error toasts.
 */

import { screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { http, HttpResponse } from 'msw';
import { describe, it, expect, vi, beforeEach } from 'vitest';

import { render } from '../utils';
import { server } from '../mocks/server';

const mockShowToast = vi.fn();

vi.mock('../../contexts/ToastContext', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../../contexts/ToastContext')>();
  return { ...actual, useToast: () => ({ showToast: mockShowToast }) };
});

import { PrintModal } from '../../components/PrintModal';
import { formatDateInput, getDatePlaceholder } from '../../utils/date';

const queueItem = {
  id: 9, printer_id: 1, archive_id: 1, position: 1, scheduled_time: null, require_previous_success: false,
  auto_off_after: false, gcode_injection: false, manual_start: false, ams_mapping: null, plate_id: null,
  bed_levelling: true, flow_cali: false, vibration_cali: true, layer_inspect: false, timelapse: false,
  use_ams: true, status: 'pending', started_at: null, completed_at: null, error_message: null,
  created_at: '2024-01-01T00:00:00Z', archive_name: 'Edit Print', archive_thumbnail: null,
  printer_name: 'X1 Carbon', print_time_seconds: 3600, batch_id: null, batch_name: null,
};

function renderModal(onClose = vi.fn(), onSuccess = vi.fn(), item: Record<string, unknown> = queueItem) {
  render(
    <PrintModal
      mode="edit-queue-item"
      archiveId={1}
      archiveName="Edit Print"
      queueItem={item as never}
      onClose={onClose}
      onSuccess={onSuccess}
    />,
  );
  return { onClose, onSuccess };
}

describe('PrintModal edit-queue-item save', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    server.use(
      http.get('/api/v1/printers/', () =>
        HttpResponse.json([{ id: 1, name: 'X1 Carbon', model: 'X1C', ip_address: '192.168.1.100', enabled: true, is_active: true }]),
      ),
      http.get('/api/v1/printers/:id/status', () => HttpResponse.json({ connected: true, state: 'IDLE', ams: [], vt_tray: [] })),
      http.get('/api/v1/archives/:id', () => HttpResponse.json({ id: 1, sliced_for_model: null })),
      http.get('/api/v1/archives/:id/plates', () => HttpResponse.json({ is_multi_plate: false, plates: [] })),
      http.get('/api/v1/archives/:id/filament-requirements', () => HttpResponse.json({ filaments: [] })),
    );
  });

  it('toasts, reports success and closes once', async () => {
    server.use(http.patch('/api/v1/queue/:id', () => HttpResponse.json({ id: 9, status: 'pending' })));
    const user = userEvent.setup();
    const { onClose, onSuccess } = renderModal();

    await user.click(await screen.findByRole('button', { name: /save/i }));

    await waitFor(() => expect(onClose).toHaveBeenCalled());
    // Let anything still queued behind the first close run before counting.
    await new Promise((r) => setTimeout(r, 50));
    expect(mockShowToast).toHaveBeenCalledTimes(1);
    expect(onSuccess).toHaveBeenCalledTimes(1);
    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it('shows one error toast on a failure and stays open', async () => {
    server.use(
      http.patch('/api/v1/queue/:id', () => HttpResponse.json({ detail: 'Queue item is printing' }, { status: 409 })),
    );
    const user = userEvent.setup();
    const { onClose } = renderModal();

    await user.click(await screen.findByRole('button', { name: /save/i }));

    await waitFor(() => expect(mockShowToast).toHaveBeenCalled());
    await new Promise((r) => setTimeout(r, 50));
    expect(mockShowToast).toHaveBeenCalledTimes(1);
    expect(mockShowToast.mock.calls[0][1]).toBe('error');
    expect(onClose).not.toHaveBeenCalled();
  });

  it('switching a staged job to Schedule saves it to start on its own', async () => {
    // The manual-start box was hidden under Schedule, but the flag was kept,
    // so the job sat staged past its time with nothing saying why (#3537).
    let body: Record<string, unknown> | null = null;
    server.use(
      http.patch('/api/v1/queue/:id', async ({ request }) => {
        body = (await request.json()) as Record<string, unknown>;
        return HttpResponse.json({ id: 9, status: 'pending' });
      }),
    );
    const user = userEvent.setup();
    renderModal(vi.fn(), vi.fn(), { ...queueItem, manual_start: true });

    expect(await screen.findByLabelText(/require manual start/i)).toBeChecked();
    await user.click(screen.getByRole('button', { name: 'Schedule' }));
    expect(screen.getByLabelText(/require manual start/i)).not.toBeChecked();
    await user.click(screen.getByRole('button', { name: /save/i }));

    await waitFor(() => expect(body).not.toBeNull());
    expect(body!.manual_start).toBe(false);
    expect(typeof body!.scheduled_time).toBe('string');
  });

  it('keeps a scheduled job waiting for Start, and shows that it does', async () => {
    let body: Record<string, unknown> | null = null;
    server.use(
      http.patch('/api/v1/queue/:id', async ({ request }) => {
        body = (await request.json()) as Record<string, unknown>;
        return HttpResponse.json({ id: 9, status: 'pending' });
      }),
    );
    const user = userEvent.setup();
    const later = new Date(Date.now() + 3 * 3600 * 1000).toISOString();
    renderModal(vi.fn(), vi.fn(), { ...queueItem, manual_start: true, scheduled_time: later });

    expect(await screen.findByLabelText(/require manual start/i)).toBeChecked();
    await user.click(screen.getByRole('button', { name: /save/i }));

    await waitFor(() => expect(body).not.toBeNull());
    expect(body!.manual_start).toBe(true);
  });

  it('will not save a schedule whose time has already passed', async () => {
    const user = userEvent.setup();
    const later = new Date(Date.now() + 3 * 3600 * 1000).toISOString();
    renderModal(vi.fn(), vi.fn(), { ...queueItem, scheduled_time: later });

    const save = await screen.findByRole('button', { name: /save/i });
    const dateInput = screen.getByPlaceholderText(getDatePlaceholder('system'));
    // Yesterday: whatever the time field holds, it is in the past.
    await user.clear(dateInput);
    await user.type(dateInput, formatDateInput(new Date(Date.now() - 24 * 3600 * 1000), 'system'));

    expect(await screen.findByText('Please enter a valid date and time')).toBeInTheDocument();
    expect(save).toBeDisabled();
  });
});
