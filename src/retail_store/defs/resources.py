import dagster as dg
import os
import subprocess
import sys
from pathlib import Path
from dagster_dbt import DbtProject, DbtCliResource, dbt_assets

# Point Dagster to dbt project
dbt_dir = Path(__file__).resolve().parent.parent / "retail_store_dbt"

# Initialize dbt project
project = DbtProject(
    project_dir=dbt_dir,
    profiles_dir=dbt_dir,
)

# Create DbtCliResource
dbt = DbtCliResource(
    project_dir=dbt_dir,
    profiles_dir=dbt_dir,
    profile="retail_store_dbt",
    target="dev",
)

# Ensure manifest.json exists before @dbt_assets decorator is evaluated
try:
    target_dir = dbt_dir / "target"
    target_dir.mkdir(parents=True, exist_ok=True)
    
    expected_manifest_path = Path(str(project.manifest_path))
    manifest_path = None
    
    # Check if manifest already exists
    if expected_manifest_path.exists():
        manifest_path = expected_manifest_path
        print(f"Using existing manifest at: {manifest_path}")
    else:
        # Try to find manifest in target subdirectories
        target_manifests = list(target_dir.glob("**/manifest.json"))
        if target_manifests:
            manifest_path = max(target_manifests, key=lambda p: p.stat().st_mtime)
            print(f"Using existing manifest at: {manifest_path}")
        else:
            # Generate manifest using dbt parse
            print(f"Generating dbt manifest at {dbt_dir}...")
            dbt_cmd = str(Path(sys.executable).parent / "dbt")
            if not Path(dbt_cmd).exists():
                dbt_cmd = "dbt"
            
            result = subprocess.run(
                [
                    dbt_cmd,
                    "parse",
                    "--profiles-dir",
                    str(dbt_dir),
                    "--project-dir",
                    str(dbt_dir),
                    "--no-version-check",
                ],
                cwd=str(dbt_dir),
                capture_output=True,
                text=True,
                timeout=300,
            )
            
            if result.returncode != 0:
                error_msg = result.stderr or result.stdout or "Unknown error"
                raise RuntimeError(
                    f"dbt parse failed with return code {result.returncode}: {error_msg}"
                )
            
            # Check again for manifest after parse
            target_manifests = list(target_dir.glob("**/manifest.json"))
            if not target_manifests:
                raise FileNotFoundError(
                    f"Manifest not created after dbt parse. "
                    f"Expected at: {expected_manifest_path}. "
                    f"Searched in: {target_dir}"
                )
            
            manifest_path = max(target_manifests, key=lambda p: p.stat().st_mtime)
            print(f"Found manifest at: {manifest_path}")
    
    # Verify manifest exists
    if manifest_path is None or not manifest_path.exists():
        raise FileNotFoundError(
            f"Manifest not found. Expected at: {expected_manifest_path}. "
            f"Searched in: {target_dir}"
        )
    
    _resolved_manifest_path = manifest_path

except Exception as e:
    print(f"Error: Failed to create/verify dbt manifest: {e}")
    print(f"Working directory: {dbt_dir}")
    print(f"Expected manifest path: {project.manifest_path}")
    raise

# Define dbt assets
@dbt_assets(manifest=str(_resolved_manifest_path), name="retail_store_dbt")
def retail_store_dbt_models(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    """dbt models for retail store data pipeline"""
    selected_assets = context.selected_asset_keys
    
    if selected_assets:
        # Convert Dagster asset keys to dbt model names
        selected_models = [asset_key.path[-1] for asset_key in selected_assets]
        selected_models = list(dict.fromkeys(selected_models))  # Deduplicate
        
        models_str = " ".join(selected_models)
        context.log.info(f"Running selected dbt models: {models_str}")
        
        try:
            event_iter = dbt.cli(["run", "--select"] + selected_models, context=context).stream()
            for evt in event_iter:
                yield evt
        except Exception as e:
            context.log.error(f"dbt run failed for selected models: {e}")
            raise
    else:
        # No selection - skip running all models for safety
        context.log.info("No asset selection provided; skipping dbt run for safety.")
        return

