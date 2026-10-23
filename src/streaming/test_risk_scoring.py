import unittest

try:
    from .risk_scoring import calculate_unified_risk
except ImportError:
    from risk_scoring import calculate_unified_risk


class UnifiedRiskScoringTests(unittest.TestCase):
    def test_low(self):
        self.assertEqual(calculate_unified_risk(0, False), (0, "LOW"))

    def test_medium(self):
        self.assertEqual(calculate_unified_risk(50, False), (50, "MEDIUM"))

    def test_high(self):
        self.assertEqual(calculate_unified_risk(70, False), (70, "HIGH"))

    def test_velocity_only(self):
        self.assertEqual(calculate_unified_risk(0, True), (40, "MEDIUM"))

    def test_velocity_raises_medium_to_high(self):
        self.assertEqual(calculate_unified_risk(50, True), (90, "HIGH"))

    def test_score_is_capped(self):
        self.assertEqual(calculate_unified_risk(70, True), (100, "HIGH"))

    def test_missing_amount_score(self):
        self.assertEqual(calculate_unified_risk(None, True), (None, "NOT_SCORED"))


if __name__ == "__main__":
    unittest.main()
