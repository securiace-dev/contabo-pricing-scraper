<?php
/**
 * Global `localAPI()` stub for tests (NO namespace — global, like real WHMCS).
 * Captures calls into $GLOBALS and returns a configurable response. Guarded so it
 * never redefines a real localAPI(). Used by ServicePriceWriterTest to prove the
 * LocalAPI payload uses the `recurringamount` API field while the raw fallback
 * writes the `amount` column.
 */
if (!function_exists('localAPI')) {
    function localAPI($command, $values = [], $adminUser = '')
    {
        $GLOBALS['__cp_localapi_calls'][] = ['command' => $command, 'values' => $values];
        return isset($GLOBALS['__cp_localapi_response'])
            ? $GLOBALS['__cp_localapi_response']
            : ['result' => 'success'];
    }
}
