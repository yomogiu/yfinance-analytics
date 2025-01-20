from dataclasses import dataclass, field
from enum import Enum
import asyncio
import logging
import networkx as nx
from typing import Dict, List, Optional, Callable, Any
from datetime import datetime

class Priority(Enum):
    HIGH = 0
    MEDIUM = 1
    LOW = 2

@dataclass
class PipelineTask:
    name: str
    function: Callable
    priority: Priority
    dependencies: List[str] = field(default_factory=list)
    timeout: int = 60
    retries: int = 3
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __lt__(self, other):
        return self.priority.value < other.priority.value

class DataPipelineScheduler:
    def __init__(self, config: Dict[str, Any]):
        self.tasks: Dict[str, PipelineTask] = {}
        self.graph = nx.DiGraph()
        self.results_cache: Dict[str, Any] = {}
        self.logger = logging.getLogger(__name__)
        self.config = config

    def add_task(self, task: PipelineTask):
        """Add a task with priority to the scheduler."""
        self.tasks[task.name] = task
        self.graph.add_node(task.name, priority=task.priority.value)
        
        for dep in task.dependencies:
            if dep not in self.tasks:
                raise ValueError(f"Dependency {dep} not found")
            self.graph.add_edge(dep, task.name)
        
        if not nx.is_directed_acyclic_graph(self.graph):
            raise ValueError("Cycle detected in task dependencies")

    async def execute_task(self, task_name: str) -> Any:
        """Execute a task and store its result."""
        task = self.tasks[task_name]
        start_time = datetime.now()
        
        for attempt in range(task.retries):
            try:
                self.logger.info(f"Executing {task_name} (Priority: {task.priority.name})")
                async with asyncio.timeout(task.timeout):
                    # Get dependency results if task has dependencies
                    dep_results = {dep: self.results_cache[dep] for dep in task.dependencies}
                    
                    if asyncio.iscoroutinefunction(task.function):
                        # Only pass dep_results if task has dependencies
                        if task.dependencies:
                            result = await task.function(self.config, dep_results)
                        else:
                            result = await task.function(self.config)
                    else:
                        if task.dependencies:
                            result = task.function(self.config, dep_results)
                        else:
                            result = task.function(self.config)
                    
                    execution_time = (datetime.now() - start_time).total_seconds()
                    self.logger.info(f"Task {task_name} completed in {execution_time:.2f}s")
                    self.results_cache[task_name] = result
                    return result
                                
            except asyncio.TimeoutError:
                self.logger.error(f"Task {task_name} timed out")
            except Exception as e:
                self.logger.error(f"Task {task_name} failed: {str(e)}")
                self.logger.exception("Task error details:")
            
            if attempt < task.retries - 1:
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
        
        raise Exception(f"Task {task_name} failed after {task.retries} attempts")

    async def run(self):
        """Execute tasks in priority order within dependency constraints."""
        try:
            # Get tasks in topological order
            task_order = list(nx.topological_sort(self.graph))
            
            # Group tasks by priority and dependencies
            priority_layers = []
            visited = set()
            
            while task_order:
                layer = []
                current_tasks = task_order[:]
                
                # Filter tasks that can be executed (all dependencies met)
                ready_tasks = [
                    task_name for task_name in current_tasks
                    if all(dep in visited for dep in self.tasks[task_name].dependencies)
                ]
                
                # Sort ready tasks by priority
                ready_tasks.sort(key=lambda x: self.tasks[x].priority.value)
                
                for task_name in ready_tasks:
                    layer.append(task_name)
                    visited.add(task_name)
                    task_order.remove(task_name)
                
                if layer:
                    priority_layers.append(layer)
            
            # Execute tasks layer by layer
            for layer in priority_layers:
                tasks = [self.execute_task(task_name) for task_name in layer]
                await asyncio.gather(*tasks)
            
            return self.results_cache
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {str(e)}")
            raise