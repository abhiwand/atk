from distutils.core import setup
import os

def get_dirs(path):
  dirs = [path]
  for dir in os.listdir(path):
    new_path = os.path.join(path, dir)
    if os.path.isdir(new_path):
      dirs = dirs + get_dirs(new_path)
  return dirs


setup(name='intel_analytics',
      description = 'Intel Analytics Platform',
      version='0.5',
      packages=get_dirs('intel_analytics'),
      platforms = ['any']
      )
