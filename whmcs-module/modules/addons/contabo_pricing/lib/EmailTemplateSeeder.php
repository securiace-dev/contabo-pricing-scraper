<?php
declare(strict_types=1);

namespace ContaboPricing;

use WHMCS\Database\Capsule;

/**
 * Seeds the WHMCS email templates used by pricing notices and the native VPS
 * lifecycle. Each template is checked by `name` in `tblemailtemplates`; if
 * absent, an INSERT is performed with conservative, long-lived WHMCS columns.
 * Administrators can later customise the body and subject in WHMCS.
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
            ]);
            $created++;
        }

        return ['created' => $created, 'skipped' => $skipped];
    }

    /**
     * Definitions of the templates this suite ships.
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
                'name'    => 'SecuriAce VPS Ready',
                'type'    => 'product',
                'subject' => 'Your VPS is ready',
                'message' => $this->bodyVpsReady(),
            ],
            [
                'name'    => 'SecuriAce VPS Provisioning Delayed',
                'type'    => 'product',
                'subject' => 'Your VPS setup is still in progress',
                'message' => $this->bodyVpsDelayed(),
            ],
            [
                'name'    => 'SecuriAce VPS Provisioning Review',
                'type'    => 'product',
                'subject' => 'Your VPS setup needs review',
                'message' => $this->bodyVpsReview(),
            ],
            [
                'name'    => 'SecuriAce VPS Password Reset Complete',
                'type'    => 'product',
                'subject' => 'Your VPS password reset is complete',
                'message' => $this->bodyVpsPasswordReset(),
            ],
            [
                'name'    => 'SecuriAce VPS Reinstall Complete',
                'type'    => 'product',
                'subject' => 'Your VPS reinstall is complete',
                'message' => $this->bodyVpsReinstall(),
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

    private function bodyVpsReady(): string
    {
        return <<<'HTML'
<p>Hi {$client_name},</p>
<p>Your VPS is ready. Sign in to the SecuriAce client area and open this service
to review its verified status, network details, and any one-time credential
available for secure reveal.</p>
<p>Operation reference: <strong>{$operation_reference}</strong></p>
<p>For your safety, server credentials are never included in email.</p>
<p>Thank you,<br/>{$company_name}</p>
HTML;
    }

    private function bodyVpsDelayed(): string
    {
        return <<<'HTML'
<p>Hi {$client_name},</p>
<p>Your VPS setup is taking longer than usual. The same provisioning request is
still being reconciled, so no duplicate server will be created.</p>
<p>Operation reference: <strong>{$operation_reference}</strong></p>
<p>You can follow the durable progress state from the service page in your
SecuriAce client area. No action is required unless our team contacts you.</p>
<p>Thank you,<br/>{$company_name}</p>
HTML;
    }

    private function bodyVpsReview(): string
    {
        return <<<'HTML'
<p>Hi {$client_name},</p>
<p>Your VPS setup needs an operator review before it can continue safely. Your
commercial service remains pending while we reconcile the provider result.</p>
<p>Operation reference: <strong>{$operation_reference}</strong></p>
<p>We will update the service timeline when the review is complete.</p>
<p>Thank you,<br/>{$company_name}</p>
HTML;
    }

    private function bodyVpsPasswordReset(): string
    {
        return <<<'HTML'
<p>Hi {$client_name},</p>
<p>Your VPS password reset is complete.</p>
<p>Operation reference: <strong>{$operation_reference}</strong></p>
<p>If a one-time credential is available, retrieve it from the authenticated
service page. It expires and is never sent by email.</p>
<p>Thank you,<br/>{$company_name}</p>
HTML;
    }

    private function bodyVpsReinstall(): string
    {
        return <<<'HTML'
<p>Hi {$client_name},</p>
<p>Your VPS reinstall has completed and the requested image has been verified.</p>
<p>Operation reference: <strong>{$operation_reference}</strong></p>
<p>Review the service page for the current operating system, network details,
and any securely delivered one-time credential.</p>
<p>Thank you,<br/>{$company_name}</p>
HTML;
    }
}
