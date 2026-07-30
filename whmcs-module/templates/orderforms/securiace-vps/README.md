# SecuriAce VPS order form

This is a WHMCS child order form. It inherits all unmodified behaviour from
`standard_cart`; only the VPS product discovery and configuration templates are
wrapped with SecuriAce workflow guidance and namespaced styling.

Assign `securiace-vps` to the Self-Managed VPS and Managed VPS product groups.
WHMCS continues to use the installation's default order form for the shared cart,
checkout, coupons, tax, authentication and payment pages.

After installation or upgrade, save the relevant product-group settings once so
WHMCS refreshes its template and module-hook discovery.
