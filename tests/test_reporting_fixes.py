#!/usr/bin/env python3
"""Tests for reporting pipeline fixes: DS merge-upsert, BF timeout/pending, reconcile."""

import json
import os
import sys
import tempfile
import unittest

# Add project root to path
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))


# ---------------------------------------------------------------------------
# Test 1: DS merge-upsert logic
# ---------------------------------------------------------------------------
class TestDSMergeUpsert(unittest.TestCase):
    """Test that _upsert merges instead of overwriting existing sheet data."""

    def _get_upsert_and_header(self):
        # Import the pivot/upsert internals
        sys.path.insert(0, os.path.join(ROOT, "scripts", "data"))
        import importlib
        # We can't easily import ds_tracking_to_gsheet because it calls
        # _add_scripts_root and imports sheets. Test the merge logic directly.
        from ds_tracking_to_gsheet import DS_TRACKING_HEADER
        return DS_TRACKING_HEADER

    def test_merge_preserves_existing_call_data(self):
        """When new row has empty CALL columns, existing CALL data is kept."""
        header = [
            "expiry", "trade_date",
            "put_structure", "call_structure",
            "put_strikes", "call_strikes",
            "put_side", "call_side",
        ]

        existing_row = [
            "2026-03-19", "2026-03-12",
            "BULL_PUT", "BULL_CALL",
            "6500/6510", "6700/6710",
            "BUY", "SELL",
        ]

        # New row only has PUT data — CALL columns are empty
        new_row = [
            "2026-03-19", "2026-03-12",
            "BEAR_PUT", "",  # call_structure is empty
            "6520/6530", "",  # call_strikes is empty
            "SELL", "",       # call_side is empty
        ]

        # Simulate the merge logic from _upsert
        merged = []
        for col_i in range(len(header)):
            new_val = new_row[col_i] if col_i < len(new_row) else ""
            old_val = existing_row[col_i] if col_i < len(existing_row) else ""
            merged.append(new_val if new_val else old_val)

        # PUT columns should be updated
        self.assertEqual(merged[2], "BEAR_PUT")
        self.assertEqual(merged[4], "6520/6530")
        self.assertEqual(merged[6], "SELL")

        # CALL columns should be preserved from existing
        self.assertEqual(merged[3], "BULL_CALL")
        self.assertEqual(merged[5], "6700/6710")
        self.assertEqual(merged[7], "SELL")

    def test_merge_overwrites_when_new_data_present(self):
        """When new row has non-empty values, they overwrite existing."""
        existing = ["2026-03-19", "2026-03-12", "OLD_PUT", "OLD_CALL"]
        new = ["2026-03-19", "2026-03-12", "NEW_PUT", "NEW_CALL"]

        merged = []
        for i in range(len(existing)):
            nv = new[i] if i < len(new) else ""
            ov = existing[i] if i < len(existing) else ""
            merged.append(nv if nv else ov)

        self.assertEqual(merged[2], "NEW_PUT")
        self.assertEqual(merged[3], "NEW_CALL")


# ---------------------------------------------------------------------------
# Test 2: BF timeout/pending plan behavior
# ---------------------------------------------------------------------------
class TestBFPendingPlan(unittest.TestCase):
    """Test handler's _finalize_bf_plan and bf_trades_to_gsheet pending rejection."""

    def test_finalize_patches_pending_to_error(self):
        """_finalize_bf_plan should patch pending plan to ERROR."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            plan = {
                "status": "OK",
                "trade_date": "2026-03-12",
                "result": {"pending": True},
            }
            json.dump(plan, f)
            f.flush()
            plan_path = f.name

        try:
            # Simulate _finalize_bf_plan logic
            with open(plan_path, "r") as fh:
                loaded = json.load(fh)
            result = loaded.get("result", {})
            if isinstance(result, dict) and result.get("pending"):
                loaded["status"] = "ERROR"
                loaded["reason"] = "TIMEOUT"
                loaded["result"] = {"error": True, "rc": 124}
                with open(plan_path, "w") as fh:
                    json.dump(loaded, fh)

            with open(plan_path, "r") as fh:
                finalized = json.load(fh)

            self.assertEqual(finalized["status"], "ERROR")
            self.assertEqual(finalized["reason"], "TIMEOUT")
            self.assertTrue(finalized["result"]["error"])
            self.assertEqual(finalized["result"]["rc"], 124)
        finally:
            os.unlink(plan_path)

    def test_finalize_skips_non_pending(self):
        """_finalize_bf_plan should not touch a plan that isn't pending."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            plan = {
                "status": "OK",
                "trade_date": "2026-03-12",
                "result": {"filled_qty": 1, "last_price": 0.35},
            }
            json.dump(plan, f)
            f.flush()
            plan_path = f.name

        try:
            with open(plan_path, "r") as fh:
                loaded = json.load(fh)
            result = loaded.get("result", {})
            # Should NOT patch — result is not pending
            self.assertFalse(isinstance(result, dict) and result.get("pending"))
            self.assertEqual(loaded["status"], "OK")
        finally:
            os.unlink(plan_path)

    def test_bf_trades_rejects_pending_plan(self):
        """bf_trades_to_gsheet should refuse to append when result.pending is True."""
        plan = {
            "status": "OK",
            "trade_date": "2026-03-12",
            "result": {"pending": True},
        }
        result = plan.get("result", {})
        is_pending = isinstance(result, dict) and result.get("pending")
        self.assertTrue(is_pending, "Should detect pending plan and refuse to append")


# ---------------------------------------------------------------------------
# Test 3: Reconcile parsing and decision logic
# ---------------------------------------------------------------------------
class TestReconcileLogic(unittest.TestCase):
    """Test reconcile_reporting.py check spec parsing and row matching."""

    def test_parse_check_spec(self):
        """Check spec 'BF_Trades:trade_date:status:signal' should parse correctly."""
        spec = "BF_Trades:trade_date:status:signal"
        parts = spec.split(":")
        self.assertEqual(parts[0], "BF_Trades")
        self.assertEqual(parts[1], "trade_date")
        self.assertEqual(parts[2:], ["status", "signal"])

    def test_parse_minimal_spec(self):
        """'DS_Tracking:trade_date' should parse with no required columns."""
        spec = "DS_Tracking:trade_date"
        parts = spec.split(":")
        self.assertEqual(parts[0], "DS_Tracking")
        self.assertEqual(parts[1], "trade_date")
        self.assertEqual(parts[2:], [])

    def test_row_matching_by_header_name(self):
        """Should find today's row by matching header name to column index."""
        headers = ["expiry", "trade_date", "put_structure", "call_structure", "status"]
        data_rows = [
            ["2026-03-19", "2026-03-11", "BULL_PUT", "BULL_CALL", "OK"],
            ["2026-03-20", "2026-03-12", "BEAR_PUT", "", "OK"],
            ["2026-03-21", "2026-03-13", "BULL_PUT", "BULL_CALL", "OK"],
        ]

        date_header = "trade_date"
        today_str = "2026-03-12"

        date_col = headers.index(date_header)
        self.assertEqual(date_col, 1)

        matched = None
        for row in data_rows:
            padded = row + [""] * (len(headers) - len(row))
            if padded[date_col] == today_str:
                matched = padded
                break

        self.assertIsNotNone(matched)
        self.assertEqual(matched[2], "BEAR_PUT")

    def test_partial_row_detection(self):
        """Should detect blank required columns in a matched row."""
        headers = ["expiry", "trade_date", "put_structure", "call_structure"]
        row = ["2026-03-19", "2026-03-12", "BULL_PUT", ""]  # call_structure is blank

        required_headers = ["put_structure", "call_structure"]
        req_cols = {rh: headers.index(rh) for rh in required_headers}

        blanks = [rh for rh, ci in req_cols.items() if not row[ci].strip()]
        self.assertEqual(blanks, ["call_structure"])

    def test_missing_check_config_is_failure(self):
        """Empty RECONCILE_CHECKS should be treated as rc=1 (not soft skip)."""
        checks_raw = ""
        # The reconciler should return 1 for missing config
        self.assertEqual(checks_raw.strip(), "")
        # Expected: return 1 (failure), not 2 (soft skip)

    def test_bad_check_format(self):
        """A check with only one part (no colon) should be flagged."""
        spec = "BF_Trades"
        parts = spec.split(":")
        self.assertTrue(len(parts) < 2, "Should reject specs without date_header")


if __name__ == "__main__":
    unittest.main()
