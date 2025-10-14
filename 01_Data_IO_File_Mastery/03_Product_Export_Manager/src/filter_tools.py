def filter_ready_to_ship (data) :
    return [product for product in data if product.get("status", "").strip().lower() == "ready to ship"]