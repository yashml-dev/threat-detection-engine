"""Rule-based unified risk scoring; scores are not calibrated probabilities."""


def calculate_unified_risk(amount_score, velocity_alert):
    if amount_score is None:
        return None, "NOT_SCORED"

    final_score = min(int(amount_score) + (40 if velocity_alert else 0), 100)
    if final_score >= 70:
        return final_score, "HIGH"
    if final_score >= 40:
        return final_score, "MEDIUM"
    return final_score, "LOW"
