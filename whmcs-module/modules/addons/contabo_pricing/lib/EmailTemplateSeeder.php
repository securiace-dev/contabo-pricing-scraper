<?php
declare(strict_types=1);

namespace ContaboPricing;

use WHMCS\Database\Capsule;

/**
 * Seeds the 4 WHMCS email templates that the Renewal Pricing Policy Engine
 * relies on. Each template is checked by `name` in `tblemailtemplates`; if
 * absent, an INSERT is performed with sensible defaults. Admins can later
 * customise the body/subject via Setup → Email Templates.
 *
 * Idempotent: re-running this seeder never duplicates a template.
 */
class EmailTemplateSeeder
{
    /**
     * Insert any missing templates.
     *
     * @return array{created:int,skipped:int} count of newly-inserted and pre-existing templates
     */
    public function ensure(): array
    {
        $created = 0;
        $skipped = 0;
        $now = date('Y-m-d H:i:s');

        foreach ($this->templates() as $tpl) {
            $exists = Capsule::table('tblemailtemplates')
                ->where('name', $tpl['name'])
                ->exists();

            if ($exists) {
                $skipped++;
                continue;
            }

            Capsule::table('tblemailtemplates')->insert([
                'type'         => $tpl['type'],
                'name'         => $tpl['name'],
                'subject'      => $tpl['subject'],
                'message'      => $tpl['message'],
                'fromname'     => '',
                'fromemail'    => '',
                'disabled'     => 0,
                'custom'       => 1,
                'language'     => '',
                'copyto'       => '',
                'bcc'          => '',
                'plaintext'    => 0,
                'attachments'  => '',
                'updated_at'   => $now,
            ]);
            $created++;
        }

        return ['created' => $created, 'skipped' => $skipped];
    }

    /**
     * Definitions of the templates this engine ships.
     *
     * @return list<array{name:string,type:string,subject:string,message:string}>
     */
    private function templates(): array
    {
        return [
            [
                'name'    => 'Contabo Pricing Change Notice',
                'type'    => 'product',
                'subject' => 'Pricing update for {$service_name}',
                'message' => $this->bodyNotice(),
            ],
            [
                'name'    => 'Contabo Pricing Change Reminder',
                'type'    => 'product',
                'subject' => 'Reminder: price changes for {$service_name} tomorrow',
                'message' => $this->bodyReminder(),
            ],
            [
                'name'    => 'Contabo Pricing Change Confirmation',
                'type'    => 'product',
                'subject' => 'Your {$service_name} pricing has updated',
                'message' => $this->bodyConfirmation(),
            ],
            [
                'name'    => 'Contabo Pricing Force-Approve Alert',
                'type'    => 'general',
                'subject' => 'Force-approval required: {$service_name}',
                'message' => $this->bodyForceApproveAlert(),
            ],
            [
                'name'    => 'Contabo Proposal Delivery',
                'type'    => 'general',
                'subject' => '{$proposal_title}',
                'message' => '{$proposal_body_html}',
            ],
        ];
    }

    private function bodyNotice(): string
    {
        return <<<'HTML'
<p>Hi {$client_name},</p>
<p>We're writing to let you know about an upcoming price change for your service
<strong>{$service_name}</strong>.</p>
<ul>
  <li>Current price: <strong>{$current_price}</strong> per {$cycle}</li>
  <li>New price: <strong>{$new_price}</strong> per {$cycle}</li>
  <li>Change: {$change_pct}</li>
  <li>Effective on: <strong>{$effective_date}</strong></li>
</ul>
<p>Reason: {$reason}</p>
<p>If you have any questions, please <a href="{$contact_url}">contact us</a>.</p>
<p>Thank you,<br/>{$company_name}</p>
HTML;
    }

    private function bodyReminder(): string
    {
        return <<<'HTML'
<p>Hi {$client_name},</p>
<p>This is a quick reminder that the pricing for your service
<strong>{$service_name}</strong> changes tomorrow.</p>
<ul>
  <li>New price: <strong>{$new_price}</strong> per {$cycle}</li>
  <li>Effective on: <strong>{$effective_date}</strong></li>
</ul>
<p>If you have any questions, please <a href="{$contact_url}">contact us</a>.</p>
<p>Thank you,<br/>{$company_name}</p>
HTML;
    }

    private function bodyConfirmation(): string
    {
        return <<<'HTML'
<p>Hi {$client_name},</p>
<p>This is a confirmation that the pricing for your service
<strong>{$service_name}</strong> has been updated.</p>
<ul>
  <li>Previous price: {$current_price} per {$cycle}</li>
  <li>New price: <strong>{$new_price}</strong> per {$cycle}</li>
  <li>Effective from: {$effective_date}</li>
</ul>
<p>If you have any questions, please <a href="{$contact_url}">contact us</a>.</p>
<p>Thank you,<br/>{$company_name}</p>
HTML;
    }

    private function bodyForceApproveAlert(): string
    {
        return <<<'HTML'
<p>An automated repricing decision exceeds the configured ceiling and requires
admin force-approval before it can be applied.</p>
<ul>
  <li>Service: <strong>{$service_name}</strong> (#{$service_id})</li>
  <li>Current price: {$current_price}</li>
  <li>Proposed price: {$new_price}</li>
  <li>Change: {$change_pct} (max allowed: {$max_increase_pct})</li>
  <li>Reason: {$reason}</li>
</ul>
<p>Review and force-approve in WHMCS Admin → Addons → Contabo Pricing →
Approval Queue.</p>
HTML;
    }
}
