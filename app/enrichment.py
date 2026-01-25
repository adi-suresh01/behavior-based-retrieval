import re
from typing import Dict, List, Tuple

from app.threading import get_thread_text

LABEL_KEYWORDS = {
    "DECISION": ["decision", "approve", "vote", "choose"],
    "RISK": ["risk", "concern", "issue", "safer"],
    "BLOCKER": ["blocker", "blocked", "cannot proceed"],
    "ACTION": ["action", "todo", "follow up", "need to"],
    "FYI": ["fyi", "for your info", "heads up"],
}

MATERIALS = ["carbon fiber", "aluminum", "aluminium"]
PHASE_HINTS = ["evt", "dvt", "pvt"]
VENDORS = ["vendor a", "vendor b"]
DEADLINES = ["by friday", "by eod", "by end of day", "by monday", "by tuesday"]
LEAD_TIME_PATTERN = re.compile(r"\b(\d+)\s+weeks\b", re.IGNORECASE)


def classify_labels(text: str) -> List[str]:
    labels = []
    lowered = text.lower()
    for label, keywords in LABEL_KEYWORDS.items():
        if any(keyword in lowered for keyword in keywords):
            labels.append(label)
    return labels


def extract_entities(text: str) -> Dict[str, List[str]]:
    lowered = text.lower()
    materials = []
    for mat in MATERIALS:
        if mat in lowered:
            materials.append(mat)
    phases = []
    for phase in PHASE_HINTS:
        if re.search(rf"\b{phase}\b", lowered):
            phases.append(phase.upper())
    vendors = []
    for vendor in VENDORS:
        if vendor in lowered:
            vendors.append(vendor.title())
    deadlines = []
    for deadline in DEADLINES:
        if deadline in lowered:
            deadlines.append(deadline)
    lead_times = [match.group(0) for match in LEAD_TIME_PATTERN.finditer(text)]
    return {
        "materials": materials,
        "phases": phases,
        "deadlines": deadlines,
        "vendors": vendors,
        "lead_times": lead_times,
    }


def compute_urgency(text: str, reactions_json_list: List[str]) -> float:
    lowered = text.lower()
    score = 0.0
    if any(deadline in lowered for deadline in DEADLINES):
        score += 0.35
    if "urgent" in lowered or "blocker" in lowered or "blocked" in lowered:
        score += 0.25
    if "decision needed" in lowered or "decision" in lowered:
        score += 0.1
    if any(phase.lower() in lowered for phase in PHASE_HINTS):
        score += 0.15
    if any("rotating_light" in r for r in reactions_json_list if r):
        score += 0.2
    return min(score, 1.0)


def build_title(entities: Dict[str, List[str]]) -> str:
    materials = [m.lower() for m in entities.get("materials", [])]
    if "carbon fiber" in materials and ("aluminum" in materials or "aluminium" in materials):
        return "Material change proposal: aluminum -> carbon fiber"
    if materials:
        return f"Material discussion: {', '.join(sorted(set(materials)))}"
    return "Thread update"


def build_summary(messages: List[Dict]) -> str:
    if not messages:
        return ""
    root = messages[0]
    replies = messages[1:6]
    lines = []
    if root.get("text"):
        lines.append(f"- {root['text']}")
    for reply in replies:
        if reply.get("text"):
            lines.append(f"- {reply['text']}")
    return "\n".join(lines)


def enrich_thread(thread_ts: str) -> Tuple[str, List[str], Dict[str, List[str]], float, str]:
    thread_text, messages = get_thread_text(thread_ts)
    labels = classify_labels(thread_text)
    entities = extract_entities(thread_text)
    reactions_json_list = [msg.get("reactions_json") for msg in messages]
    urgency = compute_urgency(thread_text, reactions_json_list)
    title = build_title(entities)
    summary = build_summary(messages)
    return title, labels, entities, urgency, summary
