import os


def makedir(dirname):
  dir = os.path.join(os.getcwd(), dirname)
  try:
    os.mkdir(dir)
  except FileExistsError as e:
    pass
  return dir
