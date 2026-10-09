"""Checks that the prepared-layer suites pass on a valid synthetic batch and fail with the right Rule ID.
The real prepared data exists after transform_and_integrate (6.8); this verifies the gate logic now.

    python -m tests.test_prepared_suites
"""
import numpy as np
import pandas as pd

from src import validation


def good_frames(n_grammy=40):
    keys = [f"artist{i}" for i in range(60)]
    dim = pd.DataFrame({"artist_match_key": keys + ["__unknown__", "__placeholder__"],
                        "artist_type": ["REAL"] * 60 + ["UNKNOWN", "PLACEHOLDER"],
                        "has_grammy_awards": [1] * n_grammy + [0] * (60 - n_grammy) + [0, 0]})
    dim["has_spotify_tracks"] = 1
    dim["grammy_spotify_overlap"] = dim["has_grammy_awards"] * dim["has_spotify_tracks"]
    tc = pd.DataFrame({"track_id": [f"t{i}" for i in range(60)], "artist_match_key": keys,
                       "genre_name": ["pop"] * 60, "popularity": 50, "danceability": 0.5, "energy": 0.5,
                       "valence": 0.5, "acousticness": 0.5})
    ac = pd.DataFrame({"award_id": range(60), "artist_match_key": keys,
                       "matched_in_spotify": [1.0] * 30 + [0.0] * 30})
    return {"prepared_dim_artist": dim, "prepared_track_credit": tc, "prepared_award_credit": ac}


def failed_rules(frames):
    out = {}
    for ds, df in frames.items():
        s = validation.validate_dataset(ds, df, run_label="selftest_prepared", raise_on_critical=False)
        out[ds] = {c["rule_id"] for c in s["checks"] if not c["success"]}, s["pipeline_action"]
    return out


def main():
    ok = failed_rules(good_frames())
    assert all(r == set() and a == "CONTINUE" for r, a in ok.values()), ok
    print("valid batch: all prepared suites pass ->", {k: v[1] for k, v in ok.items()})

    cases = {
        "DQ14 duplicate artist key":   ("prepared_dim_artist",   lambda f: f.assign(artist_match_key=f["artist_match_key"].replace("artist1", "artist0"))),
        "DQ15 duplicate track grain":  ("prepared_track_credit", lambda f: pd.concat([f, f.head(1)])),
        "DQ16 duplicate award grain":  ("prepared_award_credit", lambda f: pd.concat([f, f.head(1)])),
        "DQ17 null artist key":        ("prepared_track_credit", lambda f: f.assign(artist_match_key=[None] + list(f["artist_match_key"][1:]))),
        "DQ17 invalid artist_type":    ("prepared_dim_artist",   lambda f: f.assign(artist_type=["OTHER"] + list(f["artist_type"][1:]))),
        "DQ18 popularity out of range": ("prepared_track_credit", lambda f: f.assign(popularity=[120] + [50] * (len(f) - 1))),
        "DQ19 too few Grammy artists": ("prepared_dim_artist",   None),
        "DQ20 low match rate":         ("prepared_award_credit", lambda f: f.assign(matched_in_spotify=0.0)),
    }
    for name, (ds, mutate) in cases.items():
        frames = good_frames(n_grammy=10) if mutate is None else good_frames()
        if mutate is not None:
            frames[ds] = mutate(frames[ds])
        failed, action = failed_rules(frames)[ds]
        expected = name.split()[0]
        assert expected in failed, (name, failed)
        print(f"{name:32s} -> failed {sorted(failed)} -> {action}")
    print("OK")


if __name__ == "__main__":
    main()