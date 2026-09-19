-- Reviewed decision-gate register derived from Plan 005 and its cited evidence.
-- The counts represent explicit enabling validations, not decision uncertainty.
SELECT decision, remaining_gates, gate_summary, operating_default
FROM (
    VALUES
        ('Founder time', 1, 'Owner/counsel sign-off on emergency guardrail', 'Monthly expiry; discretionary non-contractual carry-forward'),
        ('Tax and Sum 9', 3, 'Owner confirmation; GST evidence; matching WHMCS tax configuration', 'No separate output GST; Sum 9 internal only'),
        ('Object Storage', 3, 'Authenticated API; cross-tenant isolation; purge verification', 'Pooled regional tenancy; bill committed capacity'),
        ('Cancellation and reassignment', 2, 'Provider reversal capability; sanitization workflow proof', 'Forward-dated cancellation; evidence-gated inventory reuse')
) AS reviewed(decision, remaining_gates, gate_summary, operating_default)
ORDER BY remaining_gates DESC, decision ASC;
