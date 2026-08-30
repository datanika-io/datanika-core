"""The signup legal line is one translated sentence, not fragments plus punctuation.

It used to be `_t["legal.signup_agreement"]` + `" "` + link(`legal.terms`) +
a hardcoded `" · "` + link(`legal.privacy`). Every fragment was translated and
key parity passed, so **no test in the suite could see the defect** — which is
in the *shape*, not the translations:

* `en` used a middot to do the work of "and", which is not a sentence;
* `ru` "соглашаетесь с нашими" governs the instrumental, and the link labels are
  nominative;
* `es` "acepta nuestros" is masculine plural against "Política de privacidad",
  which is feminine singular;
* `el` "αποδέχεστε τους" is masculine accusative plural against a feminine noun;
* `ar` had the separator sitting inside an RTL run with unspecified ordering.

Not fixable by better translations: the determiner, the case, the connective and
the word order all live *between* the fragments, where no locale could reach
them. So the template owns the whole sentence and each locale places the links.

These tests pin the two properties that make that safe — the placeholders exist,
in the order the renderer consumes them — plus the URL *values*, which nothing
asserted (#682 §3).
"""

import ast
import inspect
import json
import pathlib

import pytest

LOCALES = ("en", "ru", "el", "de", "fr", "es", "zh", "ar", "sr")


def _i18n_dir() -> pathlib.Path:
    import datanika.i18n as i18n_pkg

    return pathlib.Path(inspect.getfile(i18n_pkg)).parent


def _value(locale: str) -> str:
    data = json.loads((_i18n_dir() / f"{locale}.json").read_text(encoding="utf-8"))
    return data["legal.signup_agreement"]


class TestEveryLocaleOwnsTheWholeSentence:
    def test_the_probe_reads_real_files(self):
        """Guard the guard — a bad path makes every check below vacuous."""
        found = sorted(p.stem for p in _i18n_dir().glob("*.json"))
        assert found == sorted(LOCALES), f"locale set changed: {found}"

    @pytest.mark.parametrize("locale", LOCALES)
    def test_both_placeholders_are_present_exactly_once(self, locale):
        value = _value(locale)
        assert value.count("{terms}") == 1, f"{locale}: expected exactly one {{terms}}"
        assert value.count("{privacy}") == 1, f"{locale}: expected exactly one {{privacy}}"

    @pytest.mark.parametrize("locale", LOCALES)
    def test_terms_comes_before_privacy(self, locale):
        """``interpolate`` splits sequentially and cannot inspect the string at
        build time — it is a reactive Var resolved in the browser. A locale that
        swapped the order would render ``undefined`` for the tail. This is the
        constraint made mechanical rather than left as a comment.
        """
        value = _value(locale)
        assert value.index("{terms}") < value.index("{privacy}"), (
            f"{locale}: {{terms}} must precede {{privacy}}"
        )

    @pytest.mark.parametrize("locale", LOCALES)
    def test_the_sentence_does_not_end_at_a_placeholder(self, locale):
        """A template ending in ``{privacy}`` with no terminal punctuation is the
        old fragment shape wearing a placeholder — the sentence has to close."""
        value = _value(locale).strip()
        assert not value.endswith("}"), f"{locale}: sentence ends at a placeholder"


class TestTheSubstitutionProducesAWholeSentence:
    """Simulate in Python exactly what ``interpolate`` compiles to in the browser.

    The render test below proves the page *splits*; it cannot show what a reader
    sees. This does: `String.prototype.split` has the same semantics in both
    languages, so reassembling the parts around the real link labels is the
    sentence that ships.
    """

    @staticmethod
    def _rendered(locale: str) -> str:
        data = json.loads((_i18n_dir() / f"{locale}.json").read_text(encoding="utf-8"))
        head, rest = data["legal.signup_agreement"].split("{terms}")
        mid, tail = rest.split("{privacy}")
        return head + data["legal.terms"] + mid + data["legal.privacy"] + tail

    @pytest.mark.parametrize("locale", LOCALES)
    def test_nothing_of_the_template_survives_into_the_copy(self, locale):
        out = self._rendered(locale)
        assert "{" not in out and "}" not in out, f"{locale}: unsubstituted placeholder in {out!r}"

    @pytest.mark.parametrize("locale", LOCALES)
    def test_the_seams_are_clean(self, locale):
        """A stray double space or a space before punctuation is what a
        fragment-joined sentence looks like after someone 'fixed' it."""
        out = self._rendered(locale)
        assert "  " not in out, f"{locale}: double space in {out!r}"
        assert " ." not in out and " ," not in out, f"{locale}: floating punctuation in {out!r}"

    @pytest.mark.parametrize("locale", LOCALES)
    def test_both_labels_are_in_the_copy(self, locale):
        data = json.loads((_i18n_dir() / f"{locale}.json").read_text(encoding="utf-8"))
        out = self._rendered(locale)
        assert data["legal.terms"] in out
        assert data["legal.privacy"] in out

    @pytest.mark.parametrize("locale", LOCALES)
    def test_no_separator_survives_where_a_connective_belongs(self, locale):
        """The `·` is gone from the data too, not just from the Python."""
        assert "·" not in self._rendered(locale)


class TestNoSeparatorIsDoingGrammaticalWork:
    def test_the_signup_page_contains_no_literal_separator(self):
        """AST over non-docstring string literals, so the comment explaining the
        fix does not read as the defect still being present (WORKFLOW_RULES §4:
        count the instruction, not the phrase).
        """
        import datanika.ui.pages.signup as signup_module

        path = pathlib.Path(inspect.getfile(signup_module))
        tree = ast.parse(path.read_text(encoding="utf-8"))
        docstrings = {
            id(node.body[0].value)
            for node in ast.walk(tree)
            if isinstance(node, ast.Module | ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef)
            and node.body
            and isinstance(node.body[0], ast.Expr)
            and isinstance(node.body[0].value, ast.Constant)
            and isinstance(node.body[0].value.value, str)
        }
        offenders = [
            node.value
            for node in ast.walk(tree)
            if isinstance(node, ast.Constant)
            and isinstance(node.value, str)
            and id(node) not in docstrings
            and "·" in node.value
        ]
        assert offenders == [], (
            f"a separator character is still being rendered from Python: {offenders}"
        )


class TestTheLegalUrlsAreAsserted:
    """#682 §3: `test_legal_links.py` compares the render against the same
    constants the code renders, so changing `TERMS_URL` to a wrong host keeps
    all nine green. These assert the values themselves.

    Both apex URLs were verified live to return 200 with no redirect hop; the
    trailing slash is load-bearing for that.
    """

    def test_terms_url(self):
        from datanika.ui.components.layout import TERMS_URL

        assert TERMS_URL == "https://datanika.io/terms/"

    def test_privacy_url(self):
        from datanika.ui.components.layout import PRIVACY_URL

        assert PRIVACY_URL == "https://datanika.io/privacy/"


class TestTheRenderedPageStillCarriesBothLinks:
    def test_both_urls_survive_the_template(self):
        from datanika.ui.components.layout import PRIVACY_URL, TERMS_URL
        from datanika.ui.pages.signup import signup_page

        html = str(signup_page())
        assert TERMS_URL in html
        assert PRIVACY_URL in html

    def test_the_template_is_split_rather_than_printed_whole(self):
        """If the placeholders reached the browser verbatim the user would read
        "you agree to our {terms} and {privacy}" — which renders, and looks like
        working software until somebody reads it."""
        from datanika.ui.pages.signup import signup_page

        html = str(signup_page())
        assert 'split("{terms}")' in html, "the sentence is not being interpolated"
        assert 'split("{privacy}")' in html
