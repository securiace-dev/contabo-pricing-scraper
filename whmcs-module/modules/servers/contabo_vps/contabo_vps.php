<?php
/**
 * Compatibility entrypoint for services still assigned to `contabo_vps`.
 *
 * WHMCS module assignments are migrated in cohorts.  This file deliberately
 * contains no provider logic: it loads the canonical `securiacevps` module and
 * delegates every supported callback.  Keep it for at least one rollback
 * window after the final service reassignment.
 */

if (!defined('WHMCS')) {
    die('This file cannot be accessed directly');
}

require_once dirname(__DIR__) . '/securiacevps/securiacevps.php';

/** @return array<string,mixed> */
function contabo_vps_MetaData(): array
{
    $metadata = securiacevps_MetaData();
    $metadata['DisplayName'] = 'SecuriAce VPS (legacy compatibility)';
    return $metadata;
}

/** @return array<int,array<string,mixed>> */
function contabo_vps_ConfigOptions(): array
{
    return securiacevps_ConfigOptions();
}

/** @param array<string,mixed> $params @return array<string,mixed> */
function contabo_vps_TestConnection(array $params): array
{
    return securiacevps_TestConnection($params);
}

/** @param array<string,mixed> $params */
function contabo_vps_CreateAccount(array $params): string
{
    return securiacevps_CreateAccount($params);
}

/** @param array<string,mixed> $params */
function contabo_vps_SuspendAccount(array $params): string
{
    return securiacevps_SuspendAccount($params);
}

/** @param array<string,mixed> $params */
function contabo_vps_UnsuspendAccount(array $params): string
{
    return securiacevps_UnsuspendAccount($params);
}

/** @param array<string,mixed> $params */
function contabo_vps_TerminateAccount(array $params): string
{
    return securiacevps_TerminateAccount($params);
}

/** @param array<string,mixed> $params */
function contabo_vps_ChangePackage(array $params): string
{
    return securiacevps_ChangePackage($params);
}

/** @return array<string,string> */
function contabo_vps_AdminCustomButtonArray(): array
{
    return securiacevps_AdminCustomButtonArray();
}

/** @param array<string,mixed> $params */
function contabo_vps_buttonStart(array $params): string
{
    return securiacevps_buttonStart($params);
}

/** @param array<string,mixed> $params */
function contabo_vps_buttonStop(array $params): string
{
    return securiacevps_buttonStop($params);
}

/** @param array<string,mixed> $params */
function contabo_vps_buttonRestart(array $params): string
{
    return securiacevps_buttonRestart($params);
}

/** @param array<string,mixed> $params */
function contabo_vps_buttonResetPassword(array $params): string
{
    return securiacevps_buttonResetPassword($params);
}

/** @param array<string,mixed> $params */
function contabo_vps_buttonReinstall(array $params): string
{
    return securiacevps_buttonReinstall($params);
}

/** @param array<string,mixed> $params */
function contabo_vps_buttonSync(array $params): string
{
    return securiacevps_buttonSync($params);
}

/** @param array<string,mixed> $params @return array<string,string> */
function contabo_vps_AdminServicesTabFields(array $params): array
{
    return securiacevps_AdminServicesTabFields($params);
}

/** @return array<string,string> */
function contabo_vps_ClientAreaCustomButtonArray(): array
{
    return securiacevps_ClientAreaCustomButtonArray();
}

/** @param array<string,mixed> $params */
function contabo_vps_clientStart(array $params): string
{
    return securiacevps_clientStart($params);
}

/** @param array<string,mixed> $params */
function contabo_vps_clientStop(array $params): string
{
    return securiacevps_clientStop($params);
}

/** @param array<string,mixed> $params */
function contabo_vps_clientRestart(array $params): string
{
    return securiacevps_clientRestart($params);
}

/** @param array<string,mixed> $params */
function contabo_vps_clientResetPassword(array $params): string
{
    return securiacevps_clientResetPassword($params);
}

/** @param array<string,mixed> $params @return array<string,mixed> */
function contabo_vps_ClientArea(array $params): array
{
    $result = securiacevps_ClientArea($params);
    // WHMCS resolves template files relative to the selected legacy module.
    $result['templatefile'] = 'clientarea';
    return $result;
}
