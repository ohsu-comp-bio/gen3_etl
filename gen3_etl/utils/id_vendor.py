import uuid

"""
A singleton to hold the generated uuid for each submitter_id
"""


def get_id(submitter_id):
    """Calculates idempotent uuid based on submitter_id."""
    return uuid.uuid3(uuid.NAMESPACE_DNS, submitter_id)
