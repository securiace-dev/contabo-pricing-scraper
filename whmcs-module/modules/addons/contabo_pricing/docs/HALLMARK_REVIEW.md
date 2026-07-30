# Hallmark UI review

The VPS suite applies Hallmark as a design-quality and anti-pattern standard,
not as a production runtime dependency. WHMCS continues to own the global
administrator and Twenty-One theme chrome, navigation, authentication, cart,
tax, coupon, invoice, payment, and service routes.

## Reviewed surfaces

| Surface | Macrostructure | Design axis | Preserved WHMCS contract |
| --- | --- | --- | --- |
| Pricing addon | Pricing Operations Ledger | Warm technical editorial, amber anchor | Administrator permissions, routes, forms and CSRF |
| VPS service page | Operational Truth Rail | Neutral operational paper, cobalt anchor | Service ownership, client theme and module actions |
| VPS order form | Sealed Order Journey | Calm infrastructure, evergreen anchor | Standard Cart products, configuration and checkout |

The pre-emit critique for each surface is `P5 H5 E4 S5 R5 V4`: the visual
system is deliberate and coherent, while enrichment is intentionally restrained
because an operational billing interface should not use decorative imagery.

## 58-gate result

All applicable Hallmark gates pass. Navigation and footer gates are
`host-owned`: the child surfaces do not replace or imitate WHMCS chrome.
Illustration-specific gates are `not applicable`: no decorative illustrations
are used. The remaining gates are enforced by the following evidence:

* One clear hierarchy per surface; no title repetition or heading-plus-eyebrow
  stack on the embedded customer panel.
* No inline style blocks, style attributes, JavaScript event attributes, DOM
  style mutation, fake clickable rows, nested cards, thick side stripes, or
  `transition: all`.
* All colour and font declarations are tokens scoped below `.cb-wrap`,
  `.sav-panel`, `.sa-vps-order`, or `#order-standard_cart`.
* Interactive controls expose hover, focus-visible, active and disabled states.
  Controls meet a 44 CSS-pixel minimum target.
* Empty, unavailable, stale, partial, pending, success and failure states remain
  persistent in the workflow instead of relying on success toasts.
* Reduced motion and forced-colour preferences are supported.
* Tables scroll inside their owned region; task summaries collapse for small
  screens; page-owned content produces no horizontal viewport overflow.

`scripts/hallmark-audit.rb` fails the release gate if a machine-verifiable
invariant regresses.

## Contrast evidence

Normal text pairs meet WCAG AA `4.5:1`; focus indicators and large/status text
meet `3:1`. The audit computes the actual sRGB and OKLCH pairs rather than
trusting token names. Representative minimums are:

* Addon muted text on surface: `5.93:1`.
* Addon text on primary action: `4.83:1`.
* Order-form muted text on surface: `5.36:1`.
* Client muted text on surface: `6.12:1`.
* Client warning text on warning surface: `3.79:1`.
* Client danger text on danger surface: `5.10:1`.

## Responsive evidence

Rendered fixtures were inspected at `320`, `375`, `414`, `768`, and `1280`
CSS-pixel viewports for every surface. Each viewport reported document width
equal to viewport width. The small-screen review confirmed readable hierarchy,
unclipped controls, non-overlapping content and accessible task ordering. The
large-screen review confirmed that density and line length remain bounded.

These fixtures are review-only and contain no customer data or credentials.
