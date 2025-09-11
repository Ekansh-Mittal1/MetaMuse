from collections import defaultdict
from typing import Dict, Iterable, List, Tuple

import matplotlib.pyplot as plt

from .curation_models import SampleEvaluation


def compute_accuracy(samples: Iterable[SampleEvaluation]) -> Tuple[Dict[str, float], Dict[str, float]]:
    curated_counts: Dict[str, List[int]] = defaultdict(lambda: [0, 0])
    norm_counts: Dict[str, List[int]] = defaultdict(lambda: [0, 0])

    for sample in samples:
        for field_eval in sample.fields:
            field = field_eval.field_name
            if field_eval.is_curated_correct is not None:
                curated_counts[field][1] += 1
                if field_eval.is_curated_correct:
                    curated_counts[field][0] += 1
            if field_eval.is_normalized_correct is not None:
                norm_counts[field][1] += 1
                if field_eval.is_normalized_correct:
                    norm_counts[field][0] += 1

    curated_acc = {
        f: (num_correct / total if total > 0 else 0.0) for f, (num_correct, total) in curated_counts.items()
    }
    norm_acc = {f: (num_correct / total if total > 0 else 0.0) for f, (num_correct, total) in norm_counts.items()}
    return curated_acc, norm_acc


def render_accuracy_barchart(
    curated_acc: Dict[str, float],
    norm_acc: Dict[str, float],
    output_path: str,
) -> None:
    fields = sorted(set(curated_acc.keys()) | set(norm_acc.keys()))
    curated_vals = [curated_acc.get(f, 0.0) for f in fields]
    norm_vals = [norm_acc.get(f, 0.0) for f in fields]

    x = list(range(len(fields)))
    width = 0.35

    fig, ax = plt.subplots(figsize=(max(8, len(fields) * 0.8), 5))
    ax.bar([i - width / 2 for i in x], curated_vals, width, label="Curation")
    ax.bar([i + width / 2 for i in x], norm_vals, width, label="Normalization")

    ax.set_ylabel("Accuracy")
    ax.set_ylim(0, 1)
    ax.set_title("Per-field Accuracy: Curation vs Normalization")
    ax.set_xticks(x)
    ax.set_xticklabels(fields, rotation=45, ha="right")
    ax.legend()
    fig.tight_layout()
    fig.savefig(output_path)
    plt.close(fig)


