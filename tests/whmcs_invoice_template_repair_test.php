<?php

declare(strict_types=1);

require_once __DIR__ . '/../scripts/repair-whmcs-invoice-template.php';

$fixture = <<<'PHP'
<?php
$COLOR_DARK_GREY = array(64, 64, 64);
$STATUS_ACCENT_COLOR = array(75, 0, 130);
$pdf->SetTextColor(COLOR_DARK_GREY[0], COLOR_DARK_GREY[1], COLOR_DARK_GREY[2]);
$pdf->SetFillColor(STATUS_ACCENT_COLOR[0], STATUS_ACCENT_COLOR[1], STATUS_ACCENT_COLOR[2]);
if (!function_exists('formatCurrency')) {
    function formatCurrency($amount, $prefix = '', $suffix = '') { return $prefix . $amount . $suffix; }
}
$formatted = formatCurrency(10, 'INR ', '');
$badge = 'DIGITALLY VERIFIED';
$ENH = array('show_verification_badge'    => true);
?>
invoicepdf.tpl
PHP;

$temporaryPath = tempnam(sys_get_temp_dir(), 'invoice-template-repair-');
if ($temporaryPath === false) {
    throw new RuntimeException('Unable to create test fixture.');
}

try {
    if (file_put_contents($temporaryPath, $fixture) === false) {
        throw new RuntimeException('Unable to write test fixture.');
    }

    $result = repairWhmcsInvoiceTemplate($temporaryPath);
    assertWhmcsInvoiceTemplateRepaired($temporaryPath);

    if ($result !== array('color_references' => 6, 'currency_references' => 3, 'trailing_artifact' => 1)) {
        throw new RuntimeException('Unexpected repair counts: ' . json_encode($result));
    }

    $secondPass = repairWhmcsInvoiceTemplate($temporaryPath);
    if ($secondPass !== array('color_references' => 0, 'currency_references' => 0, 'trailing_artifact' => 0)) {
        throw new RuntimeException('Repair is not idempotent: ' . json_encode($secondPass));
    }

    fwrite(STDOUT, "WHMCS invoice template repair tests passed.\n");
} finally {
    @unlink($temporaryPath);
}
