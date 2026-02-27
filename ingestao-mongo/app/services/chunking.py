def chunked_cursor(cursor, chunk_size: int):
    batch = []
    for doc in cursor:
        batch.append(doc)
        if len(batch) >= chunk_size:
            yield batch
            batch = []
    if batch:
        yield batch