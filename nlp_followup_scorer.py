"""
nlp_followup_scorer.py
──────────────────────────────────────────────────────────────────────────────
NLP pipeline for Perfect Margin IMS follow-up notes.

What it does:
  1. Pulls all follow-up notes from bid___follow_up (PostgreSQL)
  2. Engineers signal features from note text (rule-based + TF-IDF)
  3. Auto-labels notes using keyword rules (no manual labeling needed)
  4. Trains a Logistic Regression classifier
  5. Scores every note with an award probability (0.0 – 1.0)
  6. Writes results to a new table: follow_up_scores (PostgreSQL)
  7. Prints an executive summary

Dependencies:
  pip install sqlalchemy psycopg2-binary pandas scikit-learn

Usage:
  python nlp_followup_scorer.py

Configuration (edit the block below):
  DB_PORT, DB_NAME, DB_USER — match your PostgreSQL setup
──────────────────────────────────────────────────────────────────────────────
"""

import os
import re
import sys
import warnings
warnings.filterwarnings("ignore")

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline, FeatureUnion
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.model_selection import StratifiedKFold, cross_val_score
from sklearn.metrics import classification_report
from sklearn.preprocessing import StandardScaler

# ── CONFIGURATION ─────────────────────────────────────────────────────────────

DB_HOST = "localhost"
DB_PORT = 5433
DB_NAME = "sales_db"
DB_USER = os.popen("whoami").read().strip()   # your Mac username
DB_PASS = ""                                   # blank = peer auth

# ── SIGNAL DICTIONARIES ───────────────────────────────────────────────────────

# Strong positive — high confidence award signals
AWARD_STRONG = [
    "we are doing this project",
    "we will be doing",
    "we have been awarded",
    "we have been re-awarded",
    "re-awarded",
    "notice to proceed",
    "ntp",
    "booked this project",
    "proceed with submittals",
    "proceed with ordering",
    "proceed with order",
    "letter of intent",
    r"\bloi\b",
    "awarded us",
    "awarding us",
    "going with us",
    "selected us",
    "we got it",
    "we won",
    "contract executed",
    "contract signed",
    "co for this project",   # change order = already won
    "co for 2",              # change order reference
]

# Moderate positive — likely award but not confirmed
AWARD_MODERATE = [
    "looks like we have the low number",
    "we have the low",
    "low number",
    "our number",
    "budgeted.*for this",
    "he said to proceed",
    "she said to proceed",
    "wants to proceed",
    "moving forward with us",
    "plans to award",
    "intends to award",
    "hoping to award",
    "award.*next week",
    "per email.*we",
    "per phone.*we",
    "per.*marie",      # referencing an award notification contact
    "confirmed.*award",
]

# Negative signals — lost or stalled
NEGATIVE = [
    "awarded to another",
    "went with another",
    "went with someone else",
    "did not get",
    "we did not win",
    "lost this",
    "lost the bid",
    "no award",
    "project cancelled",
    "project on hold",
    "project is on hold",
    "budget only",
    "over budget",
    "too high",
    "our price was too",
    "could not compete",
    "went to.*competitor",
    "awarded.*instead",
]

# Neutral / checking-in signals
NEUTRAL = [
    "left a message",
    "left a voicemail",
    "called.*no answer",
    "sent an email",
    "following up",
    "checking in",
    "asked for feedback",
    "asking for feedback",
    "still reviewing",
    "hasn't reviewed",
    "has not reviewed",
    "still waiting",
    "waiting on",
    "not sure yet",
    "hoping to get to it",
    "will let us know",
]


def count_matches(text_lower: str, patterns: list) -> int:
    """Count how many patterns match in the text."""
    return sum(1 for p in patterns if re.search(p, text_lower))


def signal_score(text_lower: str) -> float:
    """
    Rule-based signal score combining all dictionaries.
    Returns a float in [-1.0, 1.0]:
      > 0.5  = strong award signal
      0–0.5  = mild positive
      < 0    = negative signal
    """
    strong   = count_matches(text_lower, AWARD_STRONG)
    moderate = count_matches(text_lower, AWARD_MODERATE)
    negative = count_matches(text_lower, NEGATIVE)
    neutral  = count_matches(text_lower, NEUTRAL)

    score = (strong * 1.0) + (moderate * 0.5) - (negative * 1.0) - (neutral * 0.1)
    # Clip to [-3, 3] then normalise to [-1, 1]
    return max(-1.0, min(1.0, score / 3.0))


def auto_label(text_lower: str) -> int:
    """
    Auto-label for training:
      1 = award signal (positive class)
      0 = neutral / checking-in
     -1 = negative (exclude from binary classifier to avoid noise)
    """
    strong   = count_matches(text_lower, AWARD_STRONG)
    moderate = count_matches(text_lower, AWARD_MODERATE)
    negative = count_matches(text_lower, NEGATIVE)

    if strong >= 1 or moderate >= 2:
        return 1
    elif negative >= 1:
        return -1   # will be excluded from training
    else:
        return 0


# ── CUSTOM SKLEARN TRANSFORMER: signal features ───────────────────────────────

class SignalFeatureExtractor(BaseEstimator, TransformerMixin):
    """
    Extracts hand-crafted signal features from raw note text.
    Plugs into an sklearn Pipeline alongside TF-IDF.
    """
    def fit(self, X, y=None):
        return self

    def transform(self, X):
        features = []
        for text in X:
            t = str(text).lower()
            features.append([
                count_matches(t, AWARD_STRONG),
                count_matches(t, AWARD_MODERATE),
                count_matches(t, NEGATIVE),
                count_matches(t, NEUTRAL),
                signal_score(t),
                len(t),                              # note length
                len(t.split()),                      # word count
                1 if "per email" in t else 0,        # email reference
                1 if "per phone" in t else 0,        # phone reference
                1 if re.search(r'\$[\d,]+', t) else 0,  # dollar amount mentioned
                1 if "co " in t or "change order" in t else 0,
            ])
        return np.array(features)


# ── MAIN PIPELINE ─────────────────────────────────────────────────────────────

def main():
    print("\n" + "═" * 60)
    print("  Perfect Margin — Follow-Up NLP Scorer")
    print("═" * 60)

    # ── 1. Connect ──────────────────────────────────────────────────────────
    print("\n[1/6] Connecting to PostgreSQL...")
    engine = create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("      ✓ Connected")
    except Exception as e:
        print(f"      ✗ Connection failed: {e}")
        sys.exit(1)

    # ── 2. Pull data ────────────────────────────────────────────────────────
    print("\n[2/6] Loading follow-up notes...")
    df = pd.read_sql("""
        SELECT
            fu.bidfuid          AS id,
            fu.bidprojectid     AS project_id,
            fu.projectname      AS project_name,
            fu.bidfudate        AS follow_up_date,
            fu.bidfucontractor  AS contractor,
            fu.bidfucreatedby   AS estimator_raw,
            CASE
                WHEN LOWER(TRIM(fu.bidfucreatedby)) = 'scott w. hutchings'
                THEN 'Scott Hutchings'
                ELSE TRIM(fu.bidfucreatedby)
            END                 AS estimator,
            fu.bidfunotes       AS notes
        FROM bid___follow_up fu
        WHERE fu.bidfunotes IS NOT NULL
          AND fu.bidfunotes != ''
          AND LENGTH(TRIM(fu.bidfunotes)) > 10
        ORDER BY fu.bidfudate DESC
    """, engine)

    print(f"      ✓ Loaded {len(df):,} notes")
    print(f"        Date range: {df['follow_up_date'].min().date()} → "
          f"{df['follow_up_date'].max().date()}")
    print(f"        Estimators: {df['estimator'].nunique()}")
    print(f"        Contractors: {df['contractor'].nunique()}")

    # ── 3. Feature engineering ──────────────────────────────────────────────
    print("\n[3/6] Engineering features...")
    df["notes_lower"] = df["notes"].str.lower().fillna("")
    df["rule_score"]  = df["notes_lower"].apply(signal_score)
    df["auto_label"]  = df["notes_lower"].apply(auto_label)

    n_positive = (df["auto_label"] == 1).sum()
    n_negative = (df["auto_label"] == -1).sum()
    n_neutral  = (df["auto_label"] == 0).sum()
    print(f"      Auto-labels: {n_positive} award signals | "
          f"{n_neutral} neutral | {n_negative} negative (excluded from training)")

    # ── 4. Train classifier ─────────────────────────────────────────────────
    print("\n[4/6] Training classifier...")

    # Use only award and neutral for binary classification
    train_df = df[df["auto_label"] != -1].copy()

    if len(train_df) < 20:
        print("      ⚠ Insufficient labeled data. Using rule-based scores only.")
        df["award_probability"] = df["rule_score"].clip(0, 1)
        df["signal_tier"] = df["award_probability"].apply(
            lambda x: "Strong Award Signal" if x >= 0.6
            else "Mild Positive" if x >= 0.2
            else "Neutral / Pending" if x >= -0.1
            else "Negative Signal"
        )
    else:
        X_text   = train_df["notes_lower"].values
        y_labels = train_df["auto_label"].values

        # Combined pipeline: TF-IDF + hand-crafted signal features
        tfidf = TfidfVectorizer(
            max_features  = 300,
            ngram_range   = (1, 3),
            min_df        = 2,
            stop_words    = "english",
            sublinear_tf  = True
        )

        # Build a combined feature set
        from scipy.sparse import hstack, csr_matrix

        tfidf_feats   = tfidf.fit_transform(X_text)
        signal_feats  = csr_matrix(SignalFeatureExtractor().fit_transform(X_text))
        X_combined    = hstack([tfidf_feats, signal_feats])

        clf = LogisticRegression(
            C             = 1.0,
            class_weight  = "balanced",
            max_iter      = 500,
            random_state  = 42
        )
        clf.fit(X_combined, y_labels)

        # Cross-validation score
        cv = StratifiedKFold(n_splits=min(5, n_positive), shuffle=True, random_state=42)
        cv_scores = cross_val_score(clf, X_combined, y_labels, cv=cv, scoring="f1")
        print(f"      ✓ Model trained | CV F1: {cv_scores.mean():.3f} "
              f"(±{cv_scores.std():.3f})")

        # Score ALL notes (including negatives)
        all_tfidf  = tfidf.transform(df["notes_lower"].values)
        all_signal = csr_matrix(SignalFeatureExtractor().transform(df["notes_lower"].values))
        all_feats  = hstack([all_tfidf, all_signal])

        df["award_probability"] = clf.predict_proba(all_feats)[:, 1]

        # Override: force negative-labeled notes down
        df.loc[df["auto_label"] == -1, "award_probability"] = \
            df.loc[df["auto_label"] == -1, "award_probability"].clip(upper=0.15)

        df["signal_tier"] = pd.cut(
            df["award_probability"],
            bins   = [0, 0.25, 0.50, 0.75, 1.01],
            labels = ["Neutral / Pending", "Mild Positive",
                      "Likely Award", "Strong Award Signal"]
        ).astype(str)

        print(f"\n      Signal tier distribution:")
        tier_counts = df["signal_tier"].value_counts()
        for tier, count in tier_counts.items():
            pct = count / len(df) * 100
            print(f"        {tier:<22} {count:>4} notes ({pct:.1f}%)")

    # ── 5. Write scores to PostgreSQL ───────────────────────────────────────
    print("\n[5/6] Writing scores to database...")

    output = df[[
        "id", "project_id", "project_name", "follow_up_date",
        "contractor", "estimator", "notes",
        "rule_score", "award_probability", "signal_tier"
    ]].copy()

    output["scored_at"] = pd.Timestamp.now()

    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS follow_up_scores"))
        conn.execute(text("""
            CREATE TABLE follow_up_scores (
                id                  TEXT,
                project_id          TEXT,
                project_name        TEXT,
                follow_up_date      TIMESTAMP,
                contractor          TEXT,
                estimator           TEXT,
                notes               TEXT,
                rule_score          NUMERIC(6,4),
                award_probability   NUMERIC(6,4),
                signal_tier         TEXT,
                scored_at           TIMESTAMP
            )
        """))

    output.to_sql(
        "follow_up_scores",
        engine,
        if_exists = "append",
        index     = False,
        method    = "multi",
        chunksize = 500
    )
    print(f"      ✓ Written {len(output):,} scored notes → follow_up_scores")

    # ── 6. Executive summary ────────────────────────────────────────────────
    print("\n[6/6] Executive Summary")
    print("─" * 60)

    strong = df[df["signal_tier"] == "Strong Award Signal"]
    likely = df[df["signal_tier"] == "Likely Award"]
    top_notes = pd.concat([strong, likely]).sort_values(
        "award_probability", ascending=False
    )

    print(f"\n  Total notes scored:       {len(df):,}")
    print(f"  Strong award signals:     {len(strong):,}")
    print(f"  Likely award signals:     {len(likely):,}")

    print(f"\n  Top 5 highest-confidence award signals:")
    for _, row in top_notes.head(5).iterrows():
        print(f"\n  [{row['estimator']} | {row['contractor']}]")
        print(f"  Score: {row['award_probability']:.2f} | {row['signal_tier']}")
        note_preview = str(row['notes'])[:200]
        print(f"  Note:  {note_preview}{'...' if len(str(row['notes'])) > 200 else ''}")

    print(f"\n  Award signals by estimator:")
    est_summary = df[df["award_probability"] >= 0.5].groupby("estimator").agg(
        award_signals = ("id", "count"),
        avg_score     = ("award_probability", "mean")
    ).sort_values("award_signals", ascending=False)

    for est, row in est_summary.iterrows():
        print(f"    {est:<22} {int(row['award_signals']):>3} signals | "
              f"avg score: {row['avg_score']:.2f}")

    print("\n" + "═" * 60)
    print("  Run complete. Table 'follow_up_scores' is ready.")
    print("  Next step: connect the Shiny dashboard to this table.")
    print("═" * 60 + "\n")


if __name__ == "__main__":
    main()
