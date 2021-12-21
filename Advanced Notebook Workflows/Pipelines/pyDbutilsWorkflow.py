# Databricks notebook source
from concurrent.futures import ThreadPoolExecutor, Future, as_completed
from dataclasses import dataclass
from typing import List
import json

# used to carry notebook data
@dataclass
class Notebook:
  path: str
  timeout: int
  parameters: dict = None
  retry: int = 0
  enabled:bool = True
    
  # add the notebook name to parameters using the path and return
  def getParameters(self):
    
    if not self.parameters:
      self.parameters = dict()
      
    params = self.parameters
    params["notebook"] = self.path
    return params

# execute a notebook using databricks workflows
def executeNotebook(notebook:Notebook):
  
  print(f"Executing notebook {notebook.path}")
  
  try:
    
    return dbutils.notebook.run(notebook.path, notebook.timeout, notebook.getParameters())
  
  except Exception as e:
    
    if notebook.retry < 1:
      failed = json.dumps({
          "status" : "failed",
          "error" : str(e),
          "notebook" : notebook.path})
      raise Exception(failed)
    
    print(f"Retrying notebook {notebook.path}")
    notebook.retry -= 1
  
  
def tryFuture(future:Future):
  try:
    return json.loads(future.result())
  except Exception as e:
    return json.loads(str(e))
  
  
# Parallel execute a list of notebooks in parallel
def executeNotebooks(notebooks:List[Notebook], maxParallel:int):
  
  print(f"Executing {len(notebooks)} in with maxParallel of {maxParallel}")
  with ThreadPoolExecutor(max_workers=maxParallel) as executor:

    results = [executor.submit(executeNotebook, notebook)
            for notebook in notebooks 
            if notebook.enabled]
  
    # the individual notebooks handle their errors and pass back a packaged result
    # we will still need to handle the fact that the notebook execution call may fail
    # or a programmer missed the handling of an error in the notebook task
    # that's what tryFuture(future:Future) does    
    return [tryFuture(r) for r in as_completed(results)]

  
# build a list of notebooks to run
notebooks = [
  Notebook("./pyTask1", 3600, {"waittimeout": 15}, 0, True),
  Notebook("./pyTask2", 3600, {"waittimeout": 10}, 0, True),
  Notebook("./pyTask3", 3600, {"waittimeout": 8},  0, True),
  Notebook("./pyTask4", 3600, {"waittimeout": 6},  0, True),
  Notebook("./pyTask5", 3600, {"waittimeout": 4},  0, True),
  Notebook("./pyTask6", 3600, {"waittimeout": 0},  0, True)
]

# execute the notebooks in parallel
results = executeNotebooks(notebooks, 4)

# show the results
print(results)
