def normalize_amount_cents(row):
    amount_cents = int(
        row["amount_cents"]
    )

    if (
        row["payment_method"] == "card"
        and row[
            "provider_payload_version"
        ] == "v2"
    ):
        # Provider v2 amounts require normalization.
        amount_cents = round(
            amount_cents / 100
        )

    return amount_cents


def transform_payments(rows):
    """Keep successful payments and normalize numeric fields."""
    return [
        {
            **row,
            "amount_cents": (
                normalize_amount_cents(row)
            ),
        }
        for row in rows
        if row["status"] == "succeeded"
    ]
