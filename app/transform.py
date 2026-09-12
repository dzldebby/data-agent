def transform_payments(rows):
    """Keep successful payments and normalize numeric fields."""
    return [
        {**row, "amount_cents": int(row["amount_cents"])}
        for row in rows
        if row["status"] == "succeeded"
    ]
