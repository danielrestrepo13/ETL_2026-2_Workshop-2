"""Unit tests of the transformation rules (6.8) on tiny frames.

    python -m tests.test_transform
"""
import pandas as pd

from src import transform as T

SP_KEYS = {T.artist_key(n) for n in ["Earth, Wind & Fire", "Bruno Mars", "Cardi B", "Adele"]}


def test_keys():
    assert T.artist_key("Beyoncé") == T.artist_key("beyonce") == "beyonce"
    assert T.artist_key("(Miles Davis)") == T.artist_key("Miles Davis")
    assert T.artist_key("(Various Artists)") == T.PLACEHOLDER_KEY
    assert T.artist_key("Original Broadway Cast") == T.PLACEHOLDER_KEY
    # regression: pandas NA / NaN / None / empty must be UNKNOWN, never a fake artist called "na"
    for missing in (pd.NA, float("nan"), None, "", "   "):
        assert T.artist_key(missing) == T.UNKNOWN_KEY, repr(missing)


def test_split_rules():
    # whole string is a Spotify artist -> never split
    credits, mode = T.split_grammy_artist("Earth, Wind & Fire", SP_KEYS)
    assert mode == "whole_match" and len(credits) == 1
    # collaboration with evidence in Spotify -> split
    credits, mode = T.split_grammy_artist("Bruno Mars Featuring Cardi B", SP_KEYS)
    assert mode == "split" and [k for _, k in credits] == ["brunomars", "cardib"], credits
    # no evidence -> keep whole (protects band names such as Simon & Garfunkel)
    credits, mode = T.split_grammy_artist("Simon & Garfunkel", SP_KEYS)
    assert mode == "kept_unsplit" and len(credits) == 1
    # parentheses stripped from display name, key unchanged
    credits, _ = T.split_grammy_artist("(Adele)", SP_KEYS)
    assert credits == [("Adele", "adele")]
    # missing and generic credits
    for missing in (pd.NA, None, float("nan")):
        assert T.split_grammy_artist(missing, SP_KEYS) == ([(None, T.UNKNOWN_KEY)], "unknown")
    assert T.split_grammy_artist("(Various Artists)", SP_KEYS)[1] == "generic"


if __name__ == "__main__":
    test_keys(); test_split_rules(); print("OK")