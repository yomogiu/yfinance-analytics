import asyncio
import subprocess
import requests
import time
from typing import Optional
from src.pipeline.utils import load_config, ensure_data_dirs, setup_logger
from src.pipeline import DataPipelineScheduler
from src.pipeline.tasks import create_pipeline_tasks

logger = setup_logger(__name__)

async def wait_for_api(url: str, timeout: int = 30, interval: float = 0.5) -> bool:
    """Wait for API to become available."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            response = requests.get(url)
            if response.status_code == 200:
                logger.info("API is ready")
                return True
        except requests.RequestException:
            await asyncio.sleep(interval)
    return False

def start_service(cmd: list[str]) -> Optional[subprocess.Popen]:
    """Start a service and return its process handle."""
    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        return process
    except subprocess.SubprocessError as e:
        logger.error(f"Failed to start service {cmd}: {e}")
        return None

async def run_pipeline():
    """Run the data pipeline."""
    config = load_config()
    ensure_data_dirs(config)
    
    scheduler = DataPipelineScheduler(config)
    tasks = create_pipeline_tasks(config)
    
    for task in tasks:
        scheduler.add_task(task)
    
    return await scheduler.run()

async def main():
    """Run the entire system with proper service orchestration."""
    try:
        # Run the pipeline first
        logger.info("Starting data pipeline...")
        await run_pipeline()
        logger.info("Pipeline completed successfully")
        
        # Start the API server
        logger.info("Starting API server...")
        api_process = start_service(["uvicorn", "src.api.main:app", "--reload", "--port", "8001"])
        if not api_process:
            raise RuntimeError("Failed to start API server")
            
        # Wait for API to become available
        if not await wait_for_api("http://localhost:8001"):
            raise RuntimeError("API server failed to start within timeout")
        
        # Only start dashboard after API is confirmed running
        logger.info("Starting dashboard...")
        dashboard_process = start_service(["streamlit", "run", "src/dashboard/app.py"])
        if not dashboard_process:
            raise RuntimeError("Failed to start dashboard")
        
        # Keep the main process running and handle graceful shutdown
        try:
            while True:
                await asyncio.sleep(1)
                # Check if either process has terminated
                if api_process.poll() is not None or dashboard_process.poll() is not None:
                    raise RuntimeError("One of the services terminated unexpectedly")
        except (KeyboardInterrupt, RuntimeError):
            logger.info("Shutting down services...")
            api_process.terminate()
            dashboard_process.terminate()
            
    except Exception as e:
        logger.error(f"Error running system: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(main())