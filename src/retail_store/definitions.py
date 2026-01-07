from pathlib import Path

from dagster import Definitions, definitions, load_from_defs_folder
from retail_store.defs.resources import dbt, retail_store_dbt_models


@definitions
def defs():
    loaded_defs = load_from_defs_folder(path_within_project=Path(__file__).parent)
    
    # Add dbt assets and resources
    return Definitions(
        assets=list(loaded_defs.assets) + [retail_store_dbt_models],
        jobs=list(loaded_defs.jobs),
        schedules=list(loaded_defs.schedules),
        resources={**loaded_defs.resources, "dbt": dbt},
        executor=loaded_defs.executor,
    )
