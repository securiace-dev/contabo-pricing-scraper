# Re-sealing add-on secrets (provider and Jev API keys)

Provider API keys (`mod_contabo_scrape_sources.api_key_enc`) and the Jev key
(`mod_contabo_settings.scrape.jev_api_key`) are stored as `ENC:` + WHMCS `encrypt()`
output. They are readable only with the WHMCS encryption key (`$cc_encryption_hash` in
`configuration.php`).

## Symptom

If the key changed (restored `configuration.php`, new install pointed at an old database,
key rotation), `SecretStore::open()` can no longer decrypt. Since 1.1.3 this is loud:

- Activity Log: `Contabo Pricing: a sealed secret cannot be decrypted (...)`.
- Admin, Data sources: the key pill reads **unreadable - re-enter key**.
- Source "Test" returns "The stored API key cannot be decrypted".
- Scrape runs skip that source (listed under `unreadable_key_sources` when none is usable).
  An unreadable Jev key sends an otherwise clean run to `needs_review`.

Before 1.1.3 this silently looked like "no key configured".

## Fix

1. Restore the original `$cc_encryption_hash` if you still have it; the values then decrypt
   again with no further action. Otherwise continue.
2. Admin, Addons, Contabo Pricing, Data sources: for each source marked unreadable, paste the
   key into **Replace key** and save. Do the same for the Jev key. Saving seals the key with
   the current encryption key.
3. Run **Test** on each source and confirm the Activity Log has no further decrypt lines.

Never put a plaintext key in the database to "fix" this; a value without the `ENC:` prefix
is treated as legacy plaintext and is only a stop-gap. Keys are never written to logs.
