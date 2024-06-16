import hashlib


def rename_place_id(place_name: str) -> str:
    """
    Rename place_id to a hash value (md5)
    (暫時定義一個編碼規則, 之後未來可以根據業務需求調整)

    Args:
        place_name (str): The name of the place

    Returns:
        str: The hash value of the place name
    """
    return hashlib.md5(place_name.encode("utf-8")).hexdigest()