"""reCAPTCHA v3 invisible component for forms."""

import reflex as rx

from datanika.config import settings

_RECAPTCHA_JS = """
(function() {
    if (window.__recaptchaInterceptBound) return;
    window.__recaptchaInterceptBound = true;
    document.addEventListener('submit', function(e) {
        var form = e.target;
        if (!form || form.tagName !== 'FORM') return;
        var hidden = form.querySelector('input[name="captcha_token"]');
        if (!hidden || hidden.value) return;
        e.preventDefault();
        e.stopImmediatePropagation();
        grecaptcha.ready(function() {
            grecaptcha.execute('%s', {action: '%s'}).then(function(token) {
                hidden.value = token;
                form.requestSubmit();
            });
        });
    }, true);
})();
"""


def captcha_script(action: str) -> rx.Component:
    """Return reCAPTCHA elements if configured, otherwise an empty fragment."""
    site_key = settings.recaptcha_site_key
    if not site_key:
        return rx.fragment()

    js_code = _RECAPTCHA_JS % (site_key, action)
    return rx.fragment(
        rx.script(src=f"https://www.google.com/recaptcha/api.js?render={site_key}"),
        rx.el.input(type="hidden", name="captcha_token", value=""),
        rx.script(js_code),
    )
