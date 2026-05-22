<?php
declare(strict_types=1);

namespace ContaboPricing;

use WHMCS\Database\Capsule;

/**
 * Notifier — durable, idempotent customer-notice pipeline for the WHMCS
 * Renewal Pricing Policy Engine.
 *
 * Two-stage lifecycle:
 *   1. upsertNotice(...) — INSERT a `pending` row keyed by SHA-1 of
 *      service_id|target_price|effective_at|notice_type. If a row with that
 *      idempotency_key already exists, return the existing row instead of
 *      INSERTing again. Calling this method N times with identical args
 *      creates exactly ONE row.
 *
 *   2. send(noticeId) — pull the row, call WHMCS `SendEmail` LocalAPI with
 *      `messagename` + `customtype='product'` + `id=service_id` + `customvars`
 *      (base64(serialize(array))) per the WHMCS docs, then update
 *      status='sent'+sent_at, OR status='failed'+failure_reason and raise an
 *      admin alert.
 *
 * Phase A semantics (matches ServicePriceWriter):
 *   The full body of `send()` is implemented but the public method short-
 *   circuits when `$enabled === false`. In Phase A the constructor is called
 *   without arguments → `enabled = false` → no SendEmail calls are issued.
 *   The body of `processQueue()` still walks the queue but is also gated
 *   (it only ever calls the gated `send()`).
 *
 * PHP 7.4 + 8.x polyglot.
 */
final class Notifier
{
    /** @var bool */
    private $enabled;

    public function __construct(bool $enabled = false)
    {
        $this->enabled = $enabled;
    }

    /**
     * Compute the canonical idempotency key for a notice.
     *
     * Formula (Deliverable 8, item 5 of the plan):
     *   sha1( serviceId . '|' . targetPrice . '|' . effectiveAt(iso8601) . '|' . noticeType )
     *
     * `targetPrice` is rendered with `number_format($v, 4, '.', '')` so floats
     * with identical economic meaning collapse to the same key.
     */
    public static function idempotencyKey(
        int $serviceId,
        float $targetPrice,
        \DateTimeInterface $effectiveAt,
        string $noticeType
    ): string {
        $parts = [
            (string) $serviceId,
            number_format($targetPrice, 4, '.', ''),
            $effectiveAt->format('c'),
            $noticeType,
        ];
        return sha1(implode('|', $parts));
    }

    /**
     * Insert-or-return a price-change notice row.
     *
     * @param int                $serviceId          tblhosting.id
     * @param float              $targetPrice        the post-change price the
     *                                               client will be notified of
     * @param string             $currency           ISO-4217
     * @param \DateTimeInterface $effectiveAt        when the change will apply
     * @param int                $noticeDaysBefore   send N days before
     *                                               effectiveAt
     * @param string             $noticeType         'pre_change' |
     *                                               'reminder' |
     *                                               'confirmation' |
     *                                               'force_approve_alert'
     * @param string             $emailTemplateName  WHMCS messagename
     * @param array<string,mixed> $customvars         merge variables passed to
     *                                                WHMCS SendEmail
     *
     * @return array<string,mixed> the existing-or-just-inserted notice row.
     *                             Always includes the idempotency_key it was
     *                             stored under.
     */
    public function upsertNotice(
        int $serviceId,
        float $targetPrice,
        string $currency,
        \DateTimeInterface $effectiveAt,
        int $noticeDaysBefore,
        string $noticeType,
        string $emailTemplateName,
        array $customvars = []
    ): array {
        $key = self::idempotencyKey($serviceId, $targetPrice, $effectiveAt, $noticeType);

        $existing = Capsule::table('mod_contabo_price_notice')
            ->where('idempotency_key', $key)
            ->first();

        if ($existing !== null) {
            return $this->toArray($existing);
        }

        $scheduledSendAt = (new \DateTimeImmutable($effectiveAt->format('c')))
            ->sub(new \DateInterval('P' . max(0, $noticeDaysBefore) . 'D'));

        $now = date('Y-m-d H:i:s');
        $row = [
            'idempotency_key'      => $key,
            'service_id'           => $serviceId,
            'notice_type'          => $noticeType,
            'target_price'         => $targetPrice,
            'currency'             => $currency,
            'effective_at'         => $effectiveAt->format('Y-m-d H:i:s'),
            'notice_days_before'   => $noticeDaysBefore,
            'scheduled_send_at'    => $scheduledSendAt->format('Y-m-d H:i:s'),
            'status'               => 'pending',
            'email_template_name'  => $emailTemplateName,
            'email_custom_type'    => 'product',
            'related_id'           => $serviceId, // product emails: related = service_id
            'customvars_json'      => json_encode($customvars),
            'created_at'           => $now,
            'updated_at'           => $now,
        ];

        $id = Capsule::table('mod_contabo_price_notice')->insertGetId($row);
        $row['id'] = $id;
        return $row;
    }

    /**
     * Send a single pending notice through WHMCS SendEmail LocalAPI.
     *
     * In Phase A this is a NO-OP: returns ['skipped'=>true, ...].
     *
     * The full Phase-B body is implemented below the gate so the production
     * code path is fully reviewable today.
     *
     * @return array<string,mixed>
     */
    public function send(int $noticeId): array
    {
        if (!$this->enabled) {
            return [
                'skipped' => true,
                'reason'  => 'phase_a_no_sends',
                'notice_id' => $noticeId,
            ];
        }

        $row = Capsule::table('mod_contabo_price_notice')
            ->where('id', $noticeId)
            ->first();

        if ($row === null) {
            return ['skipped' => true, 'reason' => 'notice_not_found', 'notice_id' => $noticeId];
        }

        $r = $this->toArray($row);

        $customvars = [];
        if (!empty($r['customvars_json'])) {
            $decoded = json_decode((string) $r['customvars_json'], true);
            if (is_array($decoded)) {
                $customvars = $decoded;
            }
        }

        try {
            if (!function_exists('localAPI')) {
                throw new \RuntimeException('localAPI helper unavailable');
            }
            /** @var array<string,mixed> $resp */
            $resp = \localAPI('SendEmail', [
                'messagename' => (string) $r['email_template_name'],
                'id'          => (int) $r['service_id'],
                'customtype'  => 'product',
                'customvars'  => base64_encode(serialize($customvars)),
            ]);
            $status = isset($resp['result']) ? (string) $resp['result'] : '';
            if ($status !== 'success') {
                throw new \RuntimeException(
                    'SendEmail returned ' . $status
                    . ': ' . (isset($resp['message']) ? (string) $resp['message'] : '')
                );
            }
            Capsule::table('mod_contabo_price_notice')
                ->where('id', $noticeId)
                ->update([
                    'status'     => 'sent',
                    'sent_at'    => date('Y-m-d H:i:s'),
                    'updated_at' => date('Y-m-d H:i:s'),
                ]);
            return ['sent' => true, 'notice_id' => $noticeId];
        } catch (\Throwable $e) {
            $reason = $e->getMessage();
            Capsule::table('mod_contabo_price_notice')
                ->where('id', $noticeId)
                ->update([
                    'status'         => 'failed',
                    'failure_reason' => substr($reason, 0, 250),
                    'updated_at'     => date('Y-m-d H:i:s'),
                ]);
            if (function_exists('sendAdminNotification')) {
                \sendAdminNotification(
                    'Account',
                    'Contabo Pricing notice failed',
                    'Service ' . (string) $r['service_id'] . ': ' . $reason
                );
            }
            return ['sent' => false, 'notice_id' => $noticeId, 'failure_reason' => $reason];
        }
    }

    /**
     * Walk all notices where status='pending' AND scheduled_send_at <= now,
     * call send() on each. Phase A: each send() is a no-op so the counts are
     * accurate but no email actually leaves the building.
     *
     * @return array{considered:int, sent:int, failed:int, skipped:int}
     */
    public function processQueue(): array
    {
        $now = date('Y-m-d H:i:s');
        $rows = Capsule::table('mod_contabo_price_notice')
            ->where('status', 'pending')
            ->where('scheduled_send_at', '<=', $now)
            ->get();

        $considered = 0;
        $sent = 0;
        $failed = 0;
        $skipped = 0;

        if (is_iterable($rows)) {
            foreach ($rows as $row) {
                $considered++;
                $r = $this->toArray($row);
                $id = (int) ($r['id'] ?? 0);
                if ($id <= 0) {
                    $skipped++;
                    continue;
                }
                $res = $this->send($id);
                if (!empty($res['skipped'])) {
                    $skipped++;
                } elseif (!empty($res['sent'])) {
                    $sent++;
                } else {
                    $failed++;
                }
            }
        }

        return [
            'considered' => $considered,
            'sent'       => $sent,
            'failed'     => $failed,
            'skipped'    => $skipped,
        ];
    }

    /**
     * Normalise a Capsule row (which may be stdClass or array) to an
     * associative array.
     *
     * @param mixed $row
     * @return array<string,mixed>
     */
    private function toArray($row): array
    {
        if (is_array($row)) {
            return $row;
        }
        if (is_object($row)) {
            return (array) $row;
        }
        return [];
    }
}
