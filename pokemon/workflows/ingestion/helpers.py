"""PokeAPI DLT helper.

Creates a reusable REST API source for PokeAPI.
"""

from dlt.sources.rest_api import rest_api_source


def pokeapi(resource: str, limit: int = 100):
    """Create a DLT source for PokeAPI resource.

    Args:
        resource: API resource name (pokemon, type, ability, move, generation)
        limit: Number of items to fetch

    Returns:
        DLT REST API source
    """
    config = {
        "client": {
            "base_url": "https://pokeapi.co/api/v2/",
        },
        "resources": [
            {
                "name": resource,
                "endpoint": {
                    "path": resource,
                    "params": {"limit": limit, "offset": 0},
                    "data_selector": "results",
                },
            },
        ],
    }
    return rest_api_source(config)
